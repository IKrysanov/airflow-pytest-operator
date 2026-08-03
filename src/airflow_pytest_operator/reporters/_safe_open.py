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

"""Guarded opening of report files the parsers did not write themselves.

A report file is written by the pytest child process, and test code is
arbitrary code: it knows the report path from its own argv and can replace
the file with something that is not a regular file. When ``report_dir=``
points at a shared or mounted directory rather than the default per-run temp
dir, anything else with access to that directory can do the same.

The naive shape -- ``os.path.exists(path)`` then ``open(path)`` -- fails
three ways against that:

* a **named pipe** passes ``exists()``, and ``open()`` on it blocks until a
  writer appears, which may be never. There is no timeout on a synchronous
  ``open()`` and ``on_kill`` cannot interrupt it, so the Airflow worker slot
  is held forever;
* a **symlink** is followed silently, so a link planted at the report path
  makes the parser read (and surface, via the error text and XCom) whatever
  it points at;
* the gap between the ``exists()`` check and the ``open()`` is a window in
  which the thing that was checked can be swapped for the thing that is
  opened.

:func:`open_report_file` closes all three by validating *at the moment of
use*: a single ``os.open`` carries the guarantees as flags, and the file type
is checked on the already-open descriptor, so there is no separate check left
to race.

What this does **not** cover -- both verified, and both accepted rather than
overlooked:

* ``O_NOFOLLOW`` constrains only the **final** path component, so a symlinked
  *parent directory* is still traversed. Anyone able to repoint a directory
  component of ``report_dir`` can therefore still redirect the read. That is a
  strictly stronger position than "can write a file in the report dir" -- it
  means already controlling where reports land -- and refusing symlinked
  parents would break pointing ``report_dir`` at a symlinked artifacts
  directory, which is a legitimate and common deployment.
* A **hard link** is not distinguishable from the file it links to: same
  inode, ``S_ISREG`` true, no symlink involved. Creating one requires the
  attacker to already have read access to the target (and be on the same
  filesystem; Linux additionally enforces this via
  ``fs.protected_hardlinks``), so it grants no read the attacker did not
  already have -- but it does mean this guard stops symlink redirection, not
  every form of redirection.

Neither weakens the denial-of-service half: a FIFO cannot be reached through
either route, because the type check runs on the opened inode.
"""

from __future__ import annotations

import errno
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from typing import BinaryIO

from ..exceptions import ReportParseError

# O_NOFOLLOW -- a symlink at the report path fails the open outright instead of
#   being followed, and, because the check *is* the open, there is no
#   check-then-open window to swap into.
# O_NONBLOCK -- a FIFO opens immediately (with no writer) instead of blocking
#   forever, so the type check below can reject it. It is a no-op for reads on
#   the regular files we keep, so it costs nothing on the happy path.
# O_BINARY -- Windows-only, and required there: os.open defaults to text mode,
#   which would translate CRLF in the byte stream we hand to the XML/JSON
#   parsers. A no-op flag (0) everywhere else.
# Each is absent on some platform, so every one goes through getattr and simply
# degrades to a plain O_RDONLY where it does not exist.
_OPEN_FLAGS: int = (
    os.O_RDONLY
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_BINARY", 0)
)

# How a kernel reports "the final component is a symlink and O_NOFOLLOW
# forbade it": Linux and macOS raise ELOOP, some BSDs raise EMLINK.
_SYMLINK_ERRNOS: frozenset[int] = frozenset(
    e
    for e in (getattr(errno, "ELOOP", None), getattr(errno, "EMLINK", None))
    if e is not None
)


def _describe_file_type(mode: int) -> str:
    """Name the file type behind ``mode`` for an operator-facing message."""
    if stat.S_ISFIFO(mode):
        return "a named pipe (FIFO)"
    if stat.S_ISDIR(mode):
        return "a directory"
    # Kept for completeness, though POSIX refuses open() on a socket outright
    # (EOPNOTSUPP/ENXIO), so a socket is rejected by the OSError handler in
    # open_report_file before it can ever be fstat'd here.
    if stat.S_ISSOCK(mode):
        return "a socket"
    if stat.S_ISCHR(mode):
        return "a character device"
    if stat.S_ISBLK(mode):
        return "a block device"
    return "not a regular file"


@contextmanager
def open_report_file(report_path: str, *, kind: str) -> Iterator[BinaryIO]:
    """Open ``report_path`` for reading, or raise :class:`ReportParseError`.

    Yields a binary file object for a **regular file only**. A missing path, a
    symlink, a FIFO, a directory, a device or a socket each raise
    ``ReportParseError`` naming what was found, so the operator surfaces it as
    an ordinary task failure rather than hanging or reading the wrong file.

    ``kind`` is the human-readable report format ("JUnit", "JSON") used to
    build those messages.

    Binary, not text: the caller decides the encoding. XML carries its own
    declaration and must be handed to the XML parser undecoded.
    """
    if not report_path:
        raise ReportParseError(f"{kind} report not found: {report_path!r}")

    try:
        fd = os.open(report_path, _OPEN_FLAGS)
    except FileNotFoundError as exc:
        raise ReportParseError(f"{kind} report not found: {report_path!r}") from exc
    except OSError as exc:
        if exc.errno in _SYMLINK_ERRNOS:
            raise ReportParseError(
                f"{kind} report {report_path!r} is a symbolic link; refusing to "
                "follow it (the report must be a regular file)"
            ) from exc
        raise ReportParseError(
            f"Failed to open {kind} report {report_path!r}: {exc}"
        ) from exc

    try:
        mode = os.fstat(fd).st_mode
        if not stat.S_ISREG(mode):
            raise ReportParseError(
                f"{kind} report {report_path!r} is {_describe_file_type(mode)}; "
                "the report must be a regular file"
            )
    except BaseException:
        # Nothing owns the descriptor yet, so it is ours to close -- including
        # on a KeyboardInterrupt between the open and the handover below.
        os.close(fd)
        raise

    # fdopen takes ownership: closing the wrapper closes the descriptor, and it
    # closes the descriptor itself if the wrapper cannot be constructed.
    with os.fdopen(fd, "rb") as fh:
        yield fh
