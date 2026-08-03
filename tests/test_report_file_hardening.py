# Copyright 2026 the airflow-pytest-operator contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Report-file hardening, for both parsers.

The report is written by the pytest child process, and test code is arbitrary
code: it knows the report path from its own argv. When ``report_dir=`` points
at a shared directory instead of the default per-run temp dir, anything else
with access to that directory can write there too. So the *file at the report
path* is untrusted input, independent of whether its contents are.

Two failures follow from that, and both are worse than a parse error:

* a **named pipe** at the report path makes ``open()`` block until a writer
  appears. There is no timeout on a synchronous ``open()`` and ``on_kill``
  cannot interrupt one, so the Airflow worker slot is pinned forever -- a
  denial of service on the worker, not a failed task;
* a **symlink** at the report path is followed, so the parser reads whatever
  it points at and surfaces that content in the error text and in XCom.

Both parsers must therefore refuse anything that is not a regular file, and
must do it *at the open* rather than via a separate ``os.path.exists`` check
that something can invalidate in the gap.

The hang tests deliberately run ``parse()`` on a daemon thread with a
deadline: a regression here is an infinite block, and asserting on a return
value would simply hang the suite instead of failing it.
"""

from __future__ import annotations

import inspect
import os
import socket
import stat
import tempfile
import threading
from typing import Any

import pytest

from airflow_pytest_operator.exceptions import ReportParseError
from airflow_pytest_operator.reporters import JSONResultParser, JUnitResultParser
from airflow_pytest_operator.reporters._safe_open import _describe_file_type

_JUNIT_OK = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" time="0.1">
<testcase classname="tests.test_x" name="test_a" time="0.1"/>
</testsuite></testsuites>
"""

_JSON_OK = """{
  "duration": 0.1,
  "summary": {"total": 1, "passed": 1},
  "tests": [
    {"nodeid": "tests/test_x.py::test_a", "outcome": "passed",
     "call": {"duration": 0.1}}
  ]
}
"""

# (parser class, the filename it expects, a valid report of that format)
_FORMATS = [
    pytest.param(JUnitResultParser, "junit.xml", _JUNIT_OK, id="junit"),
    pytest.param(JSONResultParser, "report.json", _JSON_OK, id="json"),
]

# Generous on purpose: this is a hang detector, not a performance assertion.
# A correct parse of these tiny reports takes single-digit milliseconds, so a
# 10s budget cannot flake on a loaded CI box, and an actual regression blocks
# forever and blows through any budget at all.
_DEADLINE = 10.0


def _parse_within(parser: Any, path: str, timeout: float = _DEADLINE) -> Any:
    """Call ``parser.parse(path)`` off-thread, failing if it does not return.

    Re-raises whatever ``parse`` raised, so callers can still use
    ``pytest.raises``. The worker is a daemon thread: if the guard regresses
    it stays blocked in ``open()`` forever, and making it a daemon lets the
    suite report the failure and exit rather than hanging alongside it.
    """
    box: dict[str, Any] = {}
    done = threading.Event()

    def run() -> None:
        try:
            box["result"] = parser.parse(path)
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller
            box["exc"] = exc
        finally:
            done.set()

    threading.Thread(target=run, daemon=True).start()
    if not done.wait(timeout):
        pytest.fail(
            f"parse() did not return within {timeout}s: it is blocked on {path!r}. "
            "The report path is not being guarded against non-regular files."
        )
    if "exc" in box:
        raise box["exc"]
    return box["result"]


# ---------------------------------------------------------------------------
# The hang: a FIFO at the report path must not pin the worker.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_fifo_at_report_path_is_refused_instead_of_blocking(
    tmp_path, parser_cls, filename, valid_text
):
    path = tmp_path / filename
    os.mkfifo(path)  # no writer will ever open it

    with pytest.raises(ReportParseError, match="named pipe") as exc_info:
        _parse_within(parser_cls(), str(path))

    print(f"[report-file:fifo] {parser_cls.__name__} -> {exc_info.value}")


# ---------------------------------------------------------------------------
# The disclosure: a symlink at the report path must not be followed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_symlink_at_report_path_is_refused(tmp_path, parser_cls, filename, valid_text):
    secret = tmp_path / "fernet.key"
    secret.write_text("SECRET-KEY-MATERIAL")

    path = tmp_path / filename
    os.symlink(secret, path)

    with pytest.raises(ReportParseError, match="symbolic link") as exc_info:
        _parse_within(parser_cls(), str(path))

    # The point of refusing is that the target is never read -- so its bytes
    # must not reach the error text either, which is logged and surfaced.
    assert "SECRET-KEY-MATERIAL" not in str(exc_info.value)
    print(f"[report-file:symlink] {parser_cls.__name__} -> {exc_info.value}")


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_symlink_is_refused_even_when_target_is_a_valid_report(
    tmp_path, parser_cls, filename, valid_text
):
    # Refusing every link, rather than allowing ones that "point somewhere
    # fine", is what keeps the check atomic: a link that resolves inside the
    # report dir can be re-pointed after any check and before the open.
    real = tmp_path / f"real-{filename}"
    real.write_text(valid_text)

    path = tmp_path / filename
    os.symlink(real, path)

    with pytest.raises(ReportParseError, match="symbolic link"):
        _parse_within(parser_cls(), str(path))
    print(f"[report-file:symlink-valid-target] {parser_cls.__name__} refused")


# ---------------------------------------------------------------------------
# The other non-regular types.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_directory_at_report_path_is_refused(
    tmp_path, parser_cls, filename, valid_text
):
    path = tmp_path / filename
    path.mkdir()

    with pytest.raises(ReportParseError, match="is a directory"):
        _parse_within(parser_cls(), str(path))
    print(f"[report-file:dir] {parser_cls.__name__} refused a directory")


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_character_device_is_refused(parser_cls, filename, valid_text):
    # /dev/zero is the memory-exhaustion twin of the FIFO hang: it is always
    # readable and never ends, so an unguarded read never returns either.
    if not os.path.exists("/dev/zero"):  # pragma: no cover - POSIX only
        pytest.skip("no /dev/zero on this platform")

    with pytest.raises(ReportParseError, match="character device"):
        _parse_within(parser_cls(), "/dev/zero")
    print(f"[report-file:chardev] {parser_cls.__name__} refused /dev/zero")


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_socket_at_report_path_is_refused(parser_cls, filename, valid_text):
    # Bound in a short temp dir on purpose: AF_UNIX paths are capped near 104
    # bytes on macOS, which pytest's own tmp_path can exceed.
    tmpdir = tempfile.mkdtemp(dir="/tmp")
    path = os.path.join(tmpdir, "s")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(path)
        # Unlike the FIFO, this one never reaches our S_ISREG check: POSIX
        # refuses open() on a socket (EOPNOTSUPP/ENXIO), so the guarded open
        # turns the OSError into ReportParseError first. Asserting only
        # "refused as a normal task failure" keeps the test about the
        # behaviour that matters rather than which layer produced it.
        with pytest.raises(ReportParseError) as exc_info:
            _parse_within(parser_cls(), path)
    finally:
        sock.close()
        os.unlink(path)
        os.rmdir(tmpdir)
    assert path in str(exc_info.value)
    print(f"[report-file:socket] {parser_cls.__name__} -> {exc_info.value}")


# ---------------------------------------------------------------------------
# The type-naming itself, unit-tested.
#
# Sockets and block devices cannot be driven end-to-end here (POSIX refuses
# open() on a socket before the type check runs, and a readable block device
# needs privileges and a real disk), but _describe_file_type is a pure
# function of an st_mode int -- so the message mapping is testable directly
# even where the path through parse() is not reachable.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (stat.S_IFIFO | 0o644, "a named pipe (FIFO)"),
        (stat.S_IFDIR | 0o755, "a directory"),
        (stat.S_IFSOCK | 0o644, "a socket"),
        (stat.S_IFCHR | 0o666, "a character device"),
        (stat.S_IFBLK | 0o660, "a block device"),
        (0, "not a regular file"),  # defensive fallback: no type bits at all
    ],
)
def test_file_type_is_named_in_the_error(mode, expected):
    assert _describe_file_type(mode) == expected
    print(f"[report-file:describe] {oct(mode)} -> {expected}")


# ---------------------------------------------------------------------------
# The happy path and the pre-existing contract must both survive.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_regular_file_still_parses(tmp_path, parser_cls, filename, valid_text):
    path = tmp_path / filename
    path.write_text(valid_text)

    result = _parse_within(parser_cls(), str(path))
    print(f"[report-file:regular] {parser_cls.__name__} total={result.total}")
    assert result.total == 1
    assert result.passed == 1


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_report_in_symlinked_directory_still_parses(
    tmp_path, parser_cls, filename, valid_text
):
    # O_NOFOLLOW constrains only the final component. Pointing report_dir at a
    # symlinked artifacts directory is a normal deployment and must keep
    # working -- this is the compatibility half of refusing symlinks.
    real_dir = tmp_path / "artifacts-v2"
    real_dir.mkdir()
    (real_dir / filename).write_text(valid_text)

    link_dir = tmp_path / "artifacts"
    os.symlink(real_dir, link_dir)

    result = _parse_within(parser_cls(), str(link_dir / filename))
    print(f"[report-file:symlinked-dir] {parser_cls.__name__} total={result.total}")
    assert result.total == 1


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_missing_report_still_reports_not_found(
    tmp_path, parser_cls, filename, valid_text
):
    # The guarded open replaced an explicit exists() check; the operator-facing
    # message for the ordinary "pytest produced nothing" case must not change.
    with pytest.raises(ReportParseError, match="not found"):
        _parse_within(parser_cls(), str(tmp_path / filename))
    print(f"[report-file:missing] {parser_cls.__name__} still says 'not found'")


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_empty_path_still_reports_not_found(parser_cls, filename, valid_text):
    with pytest.raises(ReportParseError, match="not found"):
        _parse_within(parser_cls(), "")
    print(f"[report-file:empty-path] {parser_cls.__name__} still says 'not found'")


# ---------------------------------------------------------------------------
# The check must not drift back into a separate exists() call.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("parser_cls", "filename", "valid_text"), _FORMATS)
def test_parse_does_not_check_existence_separately_from_opening(
    parser_cls, filename, valid_text
):
    """Guard the *shape* of the fix, which no output value can reveal.

    A reintroduced ``os.path.exists(path)`` before the open would pass every
    test above -- the file type is still checked afterwards -- while
    reopening the window in which the checked file is swapped for another.
    The atomicity lives in there being no second lookup, so that is what this
    asserts. Scoped to ``parse``: ``report_request`` uses os.path freely.
    """
    parse_src = inspect.getsource(parser_cls.parse)
    print(f"[report-file:no-toctou] {parser_cls.__name__}.parse checked")
    assert "os.path.exists" not in parse_src
    assert "open_report_file" in parse_src
