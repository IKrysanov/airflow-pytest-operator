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


"""cwd resolution, target-path anchoring, and report-dir cleanup policy. Shared
fakes in _run_helpers."""

from __future__ import annotations

import os
from pathlib import Path

from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_auto_cwd_for_directory_target(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open('cwd_marker.txt', 'w').write(os.getcwd())\n"
    )
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, str(tests_dir))

    assert artifacts.exit_code == 0
    marker = tests_dir / "cwd_marker.txt"
    assert marker.exists(), "pytest did not run from the tests directory"
    print(f"cwd_marker: {marker.read_text()!r}")
    assert marker.read_text() == str(tests_dir.resolve())


def test_auto_cwd_for_file_target_uses_parent(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    test_file = tests_dir / "test_y.py"
    test_file.write_text("def test_a(): assert True\n")
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open('cwd_marker.txt', 'w').write(os.getcwd())\n"
    )
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, str(test_file))

    assert artifacts.exit_code == 0
    print(f"cwd_marker: {(tests_dir / 'cwd_marker.txt').read_text()!r}")
    assert (tests_dir / "cwd_marker.txt").read_text() == str(tests_dir.resolve())


def test_explicit_cwd_overrides_auto(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open(os.path.join(os.environ['MARK_DIR'], 'm.txt'), 'w')"
        ".write(os.getcwd())\n"
    )
    explicit = tmp_path / "elsewhere"
    explicit.mkdir()
    runner = SubprocessPytestRunner(cwd=str(explicit))
    artifacts = _run(runner, str(tests_dir), env={"MARK_DIR": str(explicit)})

    assert artifacts.exit_code == 0
    print(f"m.txt content: {(explicit / 'm.txt').read_text()!r}")
    assert (explicit / "m.txt").read_text() == str(explicit.resolve())


def test_report_path_unaffected_by_auto_cwd(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    rep = tmp_path / "rep"
    report_request = JUnitResultParser(report_dir=str(rep)).report_request
    runner = SubprocessPytestRunner()
    artifacts = runner.run(str(tests_dir), report_request=report_request)

    expected = str(rep / "junit.xml")
    assert artifacts.report_path == expected
    assert Path(expected).exists()


def test_relative_report_dir_resolves_against_worker_cwd(tmp_path, monkeypatch):
    # Regression: the runner derives pytest's cwd from the test target, but a
    # relative parser report_dir must resolve against the worker cwd (where the
    # runner looks for the file), not pytest's derived cwd. Otherwise pytest
    # writes the report somewhere the runner never checks, report_path comes
    # back None, and the operator raises an execution error on an otherwise
    # successful run.
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    monkeypatch.chdir(tmp_path)  # worker cwd; report_dir is relative to it
    report_request = JUnitResultParser(report_dir="reports").report_request

    runner = SubprocessPytestRunner()
    artifacts = runner.run("suite", report_request=report_request)

    assert artifacts.report_path == str(tmp_path / "reports" / "junit.xml")
    assert os.path.exists(artifacts.report_path)


def test_relative_dir_target_does_not_double_join(tmp_path, monkeypatch):
    # Regression: a relative target plus a derived cwd used to double-join
    # ("tests" -> chdir tests/ + arg "tests" -> tests/tests), failing with
    # "file or directory not found". The runner must absolutise the target
    # when it derives the cwd itself.
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    monkeypatch.chdir(tmp_path)  # worker cwd; target is relative to it

    runner = SubprocessPytestRunner()
    artifacts = _run(runner, "tests")

    print(f"exit_code={artifacts.exit_code}, stderr={artifacts.stderr[-200:]!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None


def test_relative_multiple_targets_do_not_double_join(tmp_path, monkeypatch):
    root = tmp_path / "tests"
    a_dir = root / "a"
    b_dir = root / "b"
    a_dir.mkdir(parents=True)
    b_dir.mkdir()
    (a_dir / "test_a.py").write_text("def test_a(): assert True\n")
    (b_dir / "test_b.py").write_text("def test_b(): assert True\n")
    monkeypatch.chdir(tmp_path)

    runner = SubprocessPytestRunner()
    artifacts = _run(runner, ["tests/a/test_a.py", "tests/b/test_b.py"])

    print(f"exit_code={artifacts.exit_code}, stderr={artifacts.stderr[-200:]!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None


def test_resolve_target_paths_absolutises_only_for_derived_cwd(tmp_path):
    suite = tmp_path / "test_x.py"
    suite.write_text("def test_a(): pass\n")
    rel = "tests/test_x.py"

    # Derived cwd -> targets absolutised so pytest won't double-join them.
    derived = SubprocessPytestRunner()
    out = derived._resolve_target_paths([rel], str(tmp_path))
    assert out == [os.path.abspath(rel)]

    # Explicit cwd -> targets passed verbatim (user owns cwd + targets).
    explicit = SubprocessPytestRunner(cwd=str(tmp_path))
    assert explicit._resolve_target_paths([rel], str(tmp_path)) == [rel]

    # No cwd (node-id/glob/missing) -> verbatim, resolved by inherited cwd.
    assert derived._resolve_target_paths([rel], None) == [rel]


def test_cleanup_removes_auto_dir_by_default(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path)
    auto_dir = artifacts.working_dir
    assert auto_dir is not None and os.path.isdir(auto_dir)

    print(f"auto_dir={auto_dir!r}")
    runner.cleanup(success=True)
    assert not os.path.exists(auto_dir)


def test_cleanup_never_keeps_auto_dir(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="never")
    artifacts = _run(runner, path)
    runner.cleanup(success=True)
    assert os.path.isdir(artifacts.working_dir)


def test_cleanup_is_idempotent_for_keep_policy(tmp_path, caplog):
    # On a kill the operator calls cleanup() twice (execute() finally + on_kill).
    # The "keep" branches must not re-log -- the first call claims the dir, the
    # second is a silent no-op. Regression for duplicate "Keeping..." logs.
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="never")
    _run(runner, path)
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner.cleanup(success=False)
        runner.cleanup(success=False)

    keeps = [
        m for m in (r.getMessage() for r in caplog.records) if "Keeping report" in m
    ]
    print(f"keep logs: {keeps}")
    assert len(keeps) == 1, keeps


def test_temp_dir_is_owned_and_cleaned_when_parser_uses_fallback(tmp_path):
    # No parser report_dir -> parser uses the runner's temp fallback, which the
    # runner owns and removes per policy (cleanup="always" by default).
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path)
    temp_dir = artifacts.working_dir
    assert temp_dir is not None and os.path.isdir(temp_dir)
    assert artifacts.report_path.startswith(temp_dir)
    runner.cleanup(success=True)
    assert not os.path.exists(temp_dir)


def test_parser_supplied_dir_is_user_owned_and_not_cleaned(tmp_path):
    # A parser-supplied report dir is user-owned: kept even with cleanup="always",
    # and no temp dir is left behind.
    user_dir = tmp_path / "artifacts"
    user_dir.mkdir()
    path = _suite(tmp_path, "def test_a(): assert True")
    report_request = JUnitResultParser(report_dir=str(user_dir)).report_request

    runner = SubprocessPytestRunner(cleanup="always")
    artifacts = runner.run(path, report_request=report_request)
    runner.cleanup(success=True)

    assert artifacts.working_dir == str(user_dir)
    assert user_dir.is_dir()  # not removed
    assert artifacts.report_path == str(user_dir / "junit.xml")
    assert os.path.exists(artifacts.report_path)
    assert runner._created_report_dir is None  # runner never claimed it


def test_cleanup_logs_parser_supplied_dir_location(tmp_path, caplog):
    # A parser-supplied dir produces no temp to clean, but cleanup() still logs
    # where the report was left (parity with the owned-temp "Keeping" log), and
    # is idempotent across the double-call the operator makes on a kill.
    user_dir = tmp_path / "artifacts"
    user_dir.mkdir()
    path = _suite(tmp_path, "def test_a(): assert True")
    report_request = JUnitResultParser(report_dir=str(user_dir)).report_request
    runner = SubprocessPytestRunner(cleanup="never")
    runner.run(path, report_request=report_request)
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner.cleanup(success=False)
        runner.cleanup(success=False)

    left = [
        m for m in (r.getMessage() for r in caplog.records) if "Report left at" in m
    ]
    print(f"left logs: {left}")
    assert len(left) == 1, left
    assert str(user_dir) in left[0]


def test_is_within_distinguishes_sibling_prefix_dirs():
    # _is_within decides cleanup ownership (report inside the runner's temp ->
    # owned; outside -> user-owned). A naive startswith() would wrongly treat a
    # sibling whose path shares a prefix ("/tmp/foobar" vs "/tmp/foo") as inside.
    from airflow_pytest_operator.runners.subprocess_runner import _is_within

    assert _is_within("/tmp/foo/report.json", "/tmp/foo") is True
    assert _is_within("/tmp/foo", "/tmp/foo") is True  # the dir itself
    assert _is_within("/tmp/foobar/report.json", "/tmp/foo") is False  # sibling
    assert _is_within("/tmp/other/report.json", "/tmp/foo") is False


def test_is_within_resolves_symlinks(tmp_path):
    # _is_within must compare *real* paths: a report path reached through a
    # symlinked directory points at the same physical location as the temp
    # dir, so it must count as "inside". A naive abspath() compare would say
    # False and could lead the runner to delete data through the link.
    from airflow_pytest_operator.runners.subprocess_runner import _is_within

    real_dir = tmp_path / "real"
    real_dir.mkdir()
    link_dir = tmp_path / "link"
    link_dir.symlink_to(real_dir, target_is_directory=True)

    # Report declared via the symlink, temp dir given as the real path:
    # different textual paths, same physical directory -> inside.
    assert _is_within(str(link_dir / "report.json"), str(real_dir)) is True
    # And the mirror image (real path vs symlinked dir).
    assert _is_within(str(real_dir / "report.json"), str(link_dir)) is True
    # A genuinely separate dir reached via a sibling symlink stays outside.
    other = tmp_path / "other"
    other.mkdir()
    assert _is_within(str(other / "report.json"), str(real_dir)) is False
