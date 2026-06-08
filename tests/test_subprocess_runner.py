"""Tests for SubprocessPytestRunner using real child processes."""

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

from __future__ import annotations

import logging
import os
import sys
import textwrap
from pathlib import Path

import pytest

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner

_JUNIT_REPORT_REQUEST = JUnitResultParser().report_request


def _run(runner, *args, **kwargs):
    """Thin wrapper around ``runner.run`` that supplies the required
    ``report_request`` kwarg, so the body of each test stays focused on
    what it is actually testing (timeout, env, cwd, ...) rather than on
    the runner/parser plumbing.
    """
    kwargs.setdefault("report_request", _JUNIT_REPORT_REQUEST)
    return runner.run(*args, **kwargs)


def _suite(tmp_path: Path, src: str) -> str:
    f = tmp_path / "test_x.py"
    f.write_text(textwrap.dedent(src))
    return str(f)


def test_runner_produces_report_and_zero_exit_on_pass(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    artifacts = _run(SubprocessPytestRunner(), path)
    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert Path(artifacts.report_path).exists()


def test_runner_nonzero_exit_on_failure_but_does_not_raise(tmp_path):
    path = _suite(tmp_path, "def test_bad(): assert False")
    artifacts = _run(SubprocessPytestRunner(), path)
    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code != 0
    assert artifacts.report_path is not None


def test_runner_passes_extra_args(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_one(): assert True
        def test_two(): assert True
    """,
    )
    artifacts = _run(
        SubprocessPytestRunner(),
        path,
        pytest_args=["-k", "test_one"],
    )
    print(
        f"exit_code={artifacts.exit_code}, stdout snippet: {artifacts.stdout[:120]!r}"
    )
    assert artifacts.exit_code == 0
    assert "test_one" in artifacts.stdout or artifacts.exit_code == 0


def test_runner_forwards_env(tmp_path):
    path = _suite(
        tmp_path,
        """
        import os
        def test_env(): assert os.environ.get("MY_FLAG") == "42"
    """,
    )
    artifacts = _run(
        SubprocessPytestRunner(),
        path,
        env={"MY_FLAG": "42"},
    )
    print(f"exit_code={artifacts.exit_code}")
    assert artifacts.exit_code == 0


def test_runner_bad_interpreter_raises_execution_error(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(python_executable="/no/such/python")
    with pytest.raises(TestExecutionError):
        _run(runner, path)


def test_cancel_kills_running_tree(tmp_path, caplog):
    import threading
    import time

    path = _suite(
        tmp_path,
        """
        import time
        def test_slow():
            time.sleep(60)
    """,
    )
    runner = SubprocessPytestRunner(grace_period=2.0)

    result_box = {}

    def _do_run():
        result_box["artifacts"] = _run(runner, path)

    t = threading.Thread(target=_do_run)
    started = time.monotonic()
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        t.start()

        time.sleep(2.0)
        runner.cancel()
        t.join(timeout=15)

    elapsed = time.monotonic() - started
    print(f"cancel elapsed: {elapsed:.2f}s")
    assert not t.is_alive(), "run() did not return after cancel"
    assert elapsed < 20, f"cancel was too slow: {elapsed:.1f}s"

    msgs = [r.getMessage() for r in caplog.records]
    assert any("Cancellation requested" in m for m in msgs), msgs
    assert any("Sent SIGTERM" in m for m in msgs), msgs


def test_cancel_without_live_process_is_quiet(tmp_path, caplog):
    runner = SubprocessPytestRunner()
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner.cancel()
    # No live child -> no warning-level noise; the no-op is debug only.
    msgs = [r.getMessage() for r in caplog.records]
    assert not any("Cancellation requested" in m for m in msgs), msgs


def test_cancel_is_idempotent_and_safe_without_run(tmp_path):
    runner = SubprocessPytestRunner()
    runner.cancel()
    runner.cancel()


def test_cancel_before_completion_then_run_normally(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path)
    assert artifacts.exit_code == 0


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


def test_run_logs_absolute_report_location(tmp_path, caplog):
    # The report path is otherwise invisible: an auto-created dir lands in the
    # system temp, so users keeping artifacts (cleanup="never") need the log
    # line to find report.json. Assert it carries the absolute dir, the
    # cleanup policy, and the absolute report file.
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="never")
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, path)

    msgs = [r.getMessage() for r in caplog.records]
    line = next((m for m in msgs if "pytest report directory:" in m), None)
    assert line is not None, msgs
    assert os.path.abspath(artifacts.working_dir) in line
    assert os.path.abspath(artifacts.report_path) in line
    assert "auto-created" in line
    assert "cleanup='never'" in line


def test_run_logs_completion_with_report(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, path)

    msgs = [r.getMessage() for r in caplog.records]
    line = next((m for m in msgs if "pytest run finished:" in m), None)
    assert line is not None, msgs
    assert "exit_code=0" in line
    assert "report written" in line
    # The absolute path is announced once (startup line), not repeated here.
    assert os.path.abspath(artifacts.report_path) not in line
    assert artifacts.report_path is not None  # still returned in artifacts


def test_missing_report_completion_logged_at_debug_not_warning(tmp_path, caplog):
    # When no report file is produced, the runner must NOT warn -- the
    # operator owns the user-facing warning + error (with the parser name and
    # stderr), so a runner-level WARNING would just double-log. The runner
    # records the fact at DEBUG only.
    from airflow_pytest_operator.models import ReportRequest

    declared = str(tmp_path / "rep" / "never.xml")

    def _no_write_report_request(report_dir):
        # Declare a path but ask for no plugin args -> pytest passes but
        # never writes the file, so produced ends up None.
        return ReportRequest(pytest_args=(), report_path=declared)

    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    logger = "airflow_pytest_operator.runners.subprocess_runner"
    with caplog.at_level("DEBUG", logger=logger):
        artifacts = runner.run(path, report_request=_no_write_report_request)

    assert artifacts.report_path is None  # no file at the declared path

    finished = [r for r in caplog.records if "pytest run finished:" in r.getMessage()]
    assert finished, [r.getMessage() for r in caplog.records]
    # The completion line for the no-report case is DEBUG, never WARNING.
    assert all(r.levelno == logging.DEBUG for r in finished), [
        (r.levelname, r.getMessage()) for r in finished
    ]
    assert not any(
        r.levelno >= logging.WARNING and "no report file" in r.getMessage()
        for r in caplog.records
    )


def test_resolve_cwd_falls_back_to_none_on_commonpath_value_error(
    tmp_path, monkeypatch
):
    # commonpath raises ValueError for targets with no common anchor
    # (e.g. different Windows drives). The runner must swallow it and fall
    # back to None rather than letting it escape _run_locked.
    import os.path as _osp

    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    (a / "test_a.py").write_text("def test_a(): pass\n")
    (b / "test_b.py").write_text("def test_b(): pass\n")

    def _boom(_dirs):
        raise ValueError("paths don't have the same drive")

    monkeypatch.setattr(_osp, "commonpath", _boom)
    runner = SubprocessPytestRunner()
    assert runner._resolve_cwd([str(a / "test_a.py"), str(b / "test_b.py")]) is None


def test_cleanup_logs_decision_to_keep(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="never")
    artifacts = _run(runner, path)
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner.cleanup(success=True)

    msgs = [r.getMessage() for r in caplog.records]
    line = next((m for m in msgs if "Keeping report directory" in m), None)
    assert line is not None, msgs
    assert os.path.abspath(artifacts.working_dir) in line
    assert "cleanup='never'" in line


def test_cleanup_logs_decision_to_remove(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()  # cleanup="always"
    artifacts = _run(runner, path)
    with caplog.at_level(
        "INFO", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner.cleanup(success=True)

    msgs = [r.getMessage() for r in caplog.records]
    line = next((m for m in msgs if "Removing report directory" in m), None)
    assert line is not None, msgs
    assert artifacts.working_dir in line


def test_cleanup_on_success_keeps_dir_on_failure(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="on_success")
    artifacts = _run(runner, path)
    runner.cleanup(success=False)

    assert os.path.isdir(artifacts.working_dir)


def test_cleanup_on_success_removes_dir_on_success(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="on_success")
    artifacts = _run(runner, path)
    runner.cleanup(success=True)
    assert not os.path.exists(artifacts.working_dir)


def test_cleanup_never_touches_parser_supplied_dir(tmp_path):
    user_dir = tmp_path / "my_reports"
    user_dir.mkdir()
    path = _suite(tmp_path, "def test_a(): assert True")
    report_request = JUnitResultParser(report_dir=str(user_dir)).report_request
    runner = SubprocessPytestRunner()
    runner.run(path, report_request=report_request)
    runner.cleanup(success=True)
    assert user_dir.is_dir()


def test_cleanup_is_safe_without_run(tmp_path):
    runner = SubprocessPytestRunner()
    runner.cleanup(success=True)


def test_invalid_cleanup_policy_rejected():
    with pytest.raises(ValueError):
        SubprocessPytestRunner(cleanup="sometimes")


def test_report_dir_pointing_at_file_raises_execution_error(tmp_path):
    # Parser declares a report dir that is actually a file -> makedirs fails.
    not_a_dir = tmp_path / "iam_a_file"
    not_a_dir.write_text("x")
    report_request = JUnitResultParser(report_dir=str(not_a_dir)).report_request
    runner = SubprocessPytestRunner()
    with pytest.raises(TestExecutionError, match="report directory"):
        runner.run(str(tmp_path), report_request=report_request)


def test_cancel_does_not_block_cleanup_during_grace(tmp_path):
    import threading
    import time

    path = _suite(
        tmp_path,
        """
        import signal, time
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        def test_stubborn(): time.sleep(30)
        """,
    )
    runner = SubprocessPytestRunner(grace_period=5.0)

    def _do_run():
        try:
            _run(runner, path)
        except Exception:  # noqa: BLE001
            pass

    t = threading.Thread(target=_do_run)
    t.start()
    time.sleep(1.5)

    canceller = threading.Thread(target=runner.cancel)
    canceller.start()
    time.sleep(0.5)

    t0 = time.monotonic()
    runner.cleanup(success=False)
    elapsed = time.monotonic() - t0
    assert elapsed < 2.0, f"cleanup blocked by cancel's grace wait: {elapsed:.1f}s"

    canceller.join(timeout=15)
    t.join(timeout=15)


def test_concurrent_run_on_same_instance_is_rejected(tmp_path):
    import threading
    import time

    slow = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(5)\n")
    runner = SubprocessPytestRunner()

    errors = {}

    def _slow():
        try:
            _run(runner, slow)
        except Exception as e:  # noqa: BLE001
            errors["slow"] = e

    t = threading.Thread(target=_slow)
    t.start()
    time.sleep(1.0)

    with pytest.raises(TestExecutionError, match="already executing"):
        _run(runner, slow)

    runner.cancel()
    t.join(timeout=15)


def test_sequential_reuse_of_same_instance_works(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    a = _run(runner, path)
    assert a.exit_code == 0
    b = _run(runner, path)
    print(f"first run exit_code={a.exit_code}, second run exit_code={b.exit_code}")
    assert b.exit_code == 0


def test_run_times_out_raises_execution_error(tmp_path):
    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(timeout=1, grace_period=2.0)
    with pytest.raises(TestExecutionError, match="timed out"):
        _run(runner, path)


def test_stdout_and_stderr_are_captured(tmp_path):
    path = _suite(
        tmp_path,
        """
        import sys
        def test_streams():
            print("hello-stdout")
            print("hello-stderr", file=sys.stderr)
            assert True
        """,
    )
    artifacts = _run(
        SubprocessPytestRunner(),
        path,
        pytest_args=["-s"],
    )
    print(f"stdout: {artifacts.stdout!r}")
    print(f"stderr: {artifacts.stderr!r}")
    assert artifacts.exit_code == 0
    assert "hello-stdout" in artifacts.stdout
    assert "hello-stderr" in artifacts.stderr


def test_usage_error_yields_none_report_path_without_raising(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    artifacts = _run(
        SubprocessPytestRunner(),
        path,
        pytest_args=["--definitely-not-a-real-option"],
    )
    print(artifacts.exit_code)
    print(artifacts.report_path)

    assert artifacts.exit_code != 0
    assert artifacts.report_path is None


def test_working_dir_is_the_report_dir(tmp_path):
    rep = tmp_path / "rep"
    path = _suite(tmp_path, "def test_a(): assert True")
    report_request = JUnitResultParser(report_dir=str(rep)).report_request
    artifacts = SubprocessPytestRunner().run(path, report_request=report_request)

    print(artifacts.working_dir)

    assert artifacts.working_dir == str(rep)


def test_resolve_cwd_returns_none_for_node_id_selectors(tmp_path):
    runner = SubprocessPytestRunner()
    suite_file = tmp_path / "x.py"
    suite_file.write_text("def test_a(): pass\n")

    # Any ``::`` selector -> None, regardless of whether the file part
    # actually exists. We don't try to chdir under a selector because
    # the selector's path portion is resolved by pytest verbatim.
    assert runner._resolve_cwd([str(suite_file) + "::test_a"]) is None
    assert runner._resolve_cwd([str(tmp_path / "missing.py") + "::test_a"]) is None
    assert runner._resolve_cwd(["::orphan_name"]) is None

    # A single ``::`` anywhere in the list poisons the whole list -- we
    # can't commonpath if one entry is left to the inherited cwd.
    assert runner._resolve_cwd([str(suite_file), str(suite_file) + "::test_a"]) is None

    # Non-selector paths that don't exist on disk: None as well.
    assert runner._resolve_cwd([str(tmp_path / "tests" / "*.py")]) is None
    assert runner._resolve_cwd([]) is None

    # Sanity: plain paths still get the "deduce from file/dir" treatment.
    assert runner._resolve_cwd([str(suite_file)]) == str(tmp_path)
    assert runner._resolve_cwd([str(tmp_path)]) == str(tmp_path)


def test_resolve_cwd_uses_commonpath_for_multiple_paths(tmp_path):
    """Multiple targets -> cwd is the closest shared parent.

    The whole point: ``addopts = --alluredir=allure-results`` should
    drop artefacts at the common root of the chosen suites (typically
    ``tests/``), not inside the first suite's subfolder.
    """
    runner = SubprocessPytestRunner()
    tests_root = tmp_path / "tests"
    a_dir = tests_root / "a"
    b_dir = tests_root / "b"
    a_dir.mkdir(parents=True)
    b_dir.mkdir()
    file_a = a_dir / "test_a.py"
    file_b = b_dir / "test_b.py"
    file_a.write_text("def test_one(): pass\n")
    file_b.write_text("def test_two(): pass\n")

    # Two files under tests/{a,b}/ -> tests/ is the common parent.
    cwd = runner._resolve_cwd([str(file_a), str(file_b)])
    assert cwd == str(tests_root)

    # File + sibling directory: still tests/.
    cwd = runner._resolve_cwd([str(file_a), str(b_dir)])
    assert cwd == str(tests_root)

    # Both pointing at the same dir collapses to that dir.
    cwd = runner._resolve_cwd([str(a_dir), str(a_dir)])
    assert cwd == str(a_dir)

    # If a non-selector entry doesn't exist, we bail to None for the
    # whole list -- can't safely chdir for an entry we can't resolve.
    cwd = runner._resolve_cwd([str(file_a), str(tmp_path / "ghost.py")])
    assert cwd is None


def test_stale_cancel_does_not_abort_next_run(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    runner.cancel()
    artifacts = _run(runner, path)

    print(artifacts.exit_code)
    print(artifacts.report_path)

    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None


def test_separate_instances_run_in_parallel_safely(tmp_path):
    import threading

    results = {}

    def _go(key):
        d = tmp_path / f"suite_{key}"
        d.mkdir()
        (d / "test_x.py").write_text("def test_a(): assert True\n")
        r = SubprocessPytestRunner()
        art = _run(r, str(d))
        results[key] = art.working_dir
        r.cleanup(success=True)

    threads = [threading.Thread(target=_go, args=(k,)) for k in ("a", "b", "c")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    dirs = list(results.values())
    assert len(set(dirs)) == 3, "temp dirs collided across instances"
    for d in dirs:
        assert not os.path.exists(d), "each instance must clean its own dir"


def test_terminate_returns_early_when_process_already_dead(tmp_path):
    import subprocess as _sp

    runner = SubprocessPytestRunner()
    dead = _sp.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    assert dead.poll() is not None
    runner._terminate(dead)


def test_terminate_handles_process_lookup_on_sigterm(tmp_path, monkeypatch):
    runner = SubprocessPytestRunner()

    class _FakeProc:
        returncode = None
        pid = 4321

        def poll(self):
            return None

    def _raise_lookup(proc, sig):
        raise ProcessLookupError

    monkeypatch.setattr(runner, "_signal_group", _raise_lookup)
    runner._terminate(_FakeProc())  # type: ignore[arg-type]


def test_terminate_handles_process_lookup_on_sigkill(tmp_path, monkeypatch):
    import signal as _signal_module
    import subprocess as _sp

    runner = SubprocessPytestRunner(grace_period=0.1)

    class _FakeProc:
        returncode = None
        pid = 4322

        def poll(self):
            return None

        def wait(self, timeout=None):
            raise _sp.TimeoutExpired(cmd="pytest", timeout=timeout)

    calls = {"n": 0}

    def _signal(proc, sig):
        calls["n"] += 1
        if sig == _signal_module.SIGKILL:
            raise ProcessLookupError

    monkeypatch.setattr(runner, "_signal_group", _signal)
    runner._terminate(_FakeProc())  # type: ignore[arg-type]
    print(f"signal calls: {calls['n']}")
    assert calls["n"] == 2


def test_concurrent_cleanup_only_one_rmtree(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path)
    auto_dir = artifacts.working_dir
    assert auto_dir is not None and os.path.isdir(auto_dir)

    real_lock = runner._lock

    class _ClaimingLock:
        def __enter__(self):
            runner._created_report_dir = None
            return real_lock.__enter__()

        def __exit__(self, *exc):
            return real_lock.__exit__(*exc)

    runner._lock = _ClaimingLock()  # type: ignore[assignment]
    runner.cleanup(success=True)
    runner._lock = real_lock  # type: ignore[assignment]

    assert os.path.isdir(auto_dir)
    import shutil as _shutil

    _shutil.rmtree(auto_dir, ignore_errors=True)


def test_timeout_logs_drained_stdout_and_stderr(tmp_path, caplog):
    import logging as _logging
    import sys as _sys
    import textwrap

    suite = tmp_path / "test_hang.py"
    suite.write_text(
        textwrap.dedent(
            """
            import sys, time

            def test_hang():
                # pytest disables stdout capture for output to actually
                # leave the child by default; -s on the runner side is
                # not how we ship, so write straight to fd 1/2 instead.
                # That bypasses pytest's capture and goes to the pipe
                # the runner is draining.
                import os
                os.write(1, b"drained-stdout-line\\n")
                os.write(2, b"drained-stderr-line\\n")
                time.sleep(30)  # hang until SIGKILL
            """
        ).strip()
    )

    runner = SubprocessPytestRunner(
        python_executable=_sys.executable,
        timeout=1.5,
        grace_period=0.5,
    )

    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        with pytest.raises(TestExecutionError, match="timed out"):
            _run(runner, str(suite), pytest_args=["-s"])

    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "drained-stdout-line" in joined
    assert "drained-stderr-line" in joined


def test_runner_splices_arbitrary_parser_args(tmp_path):
    from airflow_pytest_operator.models import ReportRequest

    captured = {}

    def no_report(report_dir):
        captured["dir"] = report_dir
        return ReportRequest(pytest_args=(), report_path=None)

    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path, report_request=no_report)

    print(
        f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}, captured dir={captured['dir']!r}"
    )
    assert artifacts.exit_code == 0
    assert artifacts.report_path is None
    # The runner offered the parser a temp fallback directory.
    assert os.path.basename(captured["dir"]).startswith("pytest_report_")


def test_runner_reports_none_when_parser_path_missing(tmp_path):
    from airflow_pytest_operator.models import ReportRequest

    def wrong_path(report_dir):
        return ReportRequest(
            pytest_args=(),
            report_path=str(tmp_path / "rep" / "wishful.report"),
        )

    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path, report_request=wrong_path)

    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is None


def test_runner_handles_drained_stream_closed_mid_read(tmp_path, monkeypatch, caplog):
    import subprocess as _sp

    class _BadStream:
        def __init__(self):
            self._calls = 0

        def readline(self):
            self._calls += 1
            if self._calls == 1:
                return "first-line\n"
            raise ValueError("I/O operation on closed file")

        def close(self):
            pass

    class _OKStream:
        def readline(self):
            return ""

        def close(self):
            pass

    class _FakeProc:
        returncode = 0

        def __init__(self):
            self.stdout = _BadStream()
            self.stderr = _OKStream()

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(_sp, "Popen", lambda *_a, **_k: _FakeProc())
    runner = SubprocessPytestRunner()
    monkeypatch.setattr(runner, "_terminate", lambda proc: None)

    with caplog.at_level(
        "WARNING", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, str(tmp_path))
    assert artifacts.exit_code == 0
    assert "first-line" in artifacts.stdout
    # The failure happened on the READ side -> the log must say "draining",
    # not "closing" (which would be the close() path in finally). Regression
    # for the copy-pasted "close stream after drain" message that fired for
    # both cases and made the two indistinguishable.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("error draining pytest output stream" in m for m in msgs), msgs
    assert not any("error closing" in m for m in msgs), msgs


def test_runner_tolerates_close_failure_on_drained_stream(
    tmp_path, monkeypatch, caplog
):
    import subprocess as _sp

    class _CloseRaiser:
        def __init__(self):
            self._done = False

        def readline(self):
            if not self._done:
                self._done = True
                return "one-line\n"
            return ""

        def close(self):
            raise OSError("close failed for reasons")

    class _OKStream:
        def readline(self):
            return ""

        def close(self):
            pass

    class _FakeProc:
        returncode = 0

        def __init__(self):
            self.stdout = _CloseRaiser()
            self.stderr = _OKStream()

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(_sp, "Popen", lambda *_a, **_k: _FakeProc())
    runner = SubprocessPytestRunner()
    monkeypatch.setattr(runner, "_terminate", lambda proc: None)

    with caplog.at_level(
        "WARNING", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, str(tmp_path))
    assert artifacts.exit_code == 0
    assert "one-line" in artifacts.stdout
    # The read loop succeeded; only close() failed -> the log must point at
    # the close path ("closing ... after drain"), not the read path.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("error closing pytest output stream after drain" in m for m in msgs), (
        msgs
    )
    assert not any("error draining" in m for m in msgs), msgs


def _process_alive(pid: int) -> bool:
    """Return True if the OS still has a live process with this PID.

    `os.kill(pid, 0)` is the POSIX idiom -- signal 0 does nothing, but
    the call still fails with OSError/ProcessLookupError if the process
    is gone (or with PermissionError if we lack rights, which is fine
    for our purposes: still alive).
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover -- not expected under test
        return True
    return True


def test_unexpected_exception_during_wait_kills_subprocess(tmp_path, monkeypatch):
    import subprocess as _sp
    import time

    class FakeTaskTimeout(Exception):  # noqa: N818
        pass

    suite = tmp_path / "test_long.py"
    suite.write_text(
        textwrap.dedent(
            """
            import time
            def test_long_sleeper(): time.sleep(30)
            """
        ).strip()
    )

    original_popen = _sp.Popen
    captured_proc: dict = {}

    def patched_popen(*args, **kwargs):
        proc = original_popen(*args, **kwargs)
        captured_proc["proc"] = proc
        real_wait = proc.wait
        wait_call_count = {"n": 0}

        def evil_wait(timeout=None):
            wait_call_count["n"] += 1
            if wait_call_count["n"] == 1:
                time.sleep(0.6)
                raise FakeTaskTimeout("simulated Airflow execution_timeout")
            return real_wait(timeout=timeout)

        proc.wait = evil_wait  # type: ignore[method-assign]
        return proc

    monkeypatch.setattr(_sp, "Popen", patched_popen)

    runner = SubprocessPytestRunner()

    with pytest.raises(FakeTaskTimeout, match="simulated"):
        _run(runner, str(suite))

    proc = captured_proc["proc"]
    pid = proc.pid

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and _process_alive(pid):
        time.sleep(0.05)
    alive_after = _process_alive(pid)
    print(
        f"[unexpected_exc_kills_proc] pid={pid} alive_after_propagation={alive_after} "
        f"returncode={proc.returncode!r} "
        f"(alive_after must be False, returncode should be -SIGTERM=-15 or -SIGKILL=-9)"
    )

    assert not alive_after, (
        f"pytest subprocess (pid={pid}) survived the FakeTaskTimeout: this "
        "means _run_locked's `except BaseException -> _terminate` clause "
        "regressed, and Airflow's execution_timeout would now leak orphans."
    )
    assert proc.returncode in (-15, -9), (
        f"unexpected exit cause: returncode={proc.returncode}"
    )


def test_cancel_after_unexpected_exception_is_safe_noop(tmp_path, monkeypatch):
    import subprocess as _sp
    import time

    class FakeTaskTimeout(Exception):  # noqa: N818
        pass

    suite = tmp_path / "test_long.py"
    suite.write_text(
        textwrap.dedent(
            """
            import time
            def test_long_sleeper(): time.sleep(30)
            """
        ).strip()
    )

    original_popen = _sp.Popen

    def patched_popen(*args, **kwargs):
        proc = original_popen(*args, **kwargs)
        real_wait = proc.wait
        wait_calls = {"n": 0}

        def evil_wait(timeout=None):
            wait_calls["n"] += 1
            if wait_calls["n"] == 1:
                time.sleep(0.4)
                raise FakeTaskTimeout("airflow-style timeout")
            return real_wait(timeout=timeout)

        proc.wait = evil_wait  # type: ignore[method-assign]
        return proc

    monkeypatch.setattr(_sp, "Popen", patched_popen)

    runner = SubprocessPytestRunner()
    with pytest.raises(FakeTaskTimeout):
        _run(runner, str(suite))

    print(
        "[cancel_after_unexpected_exc] calling cancel() post-exception, "
        "expecting silent no-op"
    )
    runner.cancel()
    runner.cancel()
    print("[cancel_after_unexpected_exc] cancel() completed without error")


def test_on_kill_during_active_run_kills_subprocess(tmp_path):
    import threading
    import time

    from airflow_pytest_operator.operators import PytestOperator

    suite = tmp_path / "test_long.py"
    suite.write_text(
        textwrap.dedent(
            """
            import time
            def test_long(): time.sleep(30)
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    op = PytestOperator(task_id="t", test_path=str(suite), runner=runner)

    captured_pid: dict = {}

    def watcher():
        for _ in range(50):
            time.sleep(0.1)
            proc = runner._proc  # noqa: SLF001 — test introspection
            if proc is not None:
                captured_pid["pid"] = proc.pid
                return

    def killer():
        time.sleep(1.0)
        op.on_kill()

    threading.Thread(target=watcher, daemon=True).start()
    kill_thread = threading.Thread(target=killer, daemon=True)
    kill_thread.start()

    from airflow_pytest_operator.exceptions import TestExecutionError

    started = time.monotonic()
    with pytest.raises(TestExecutionError):
        op.execute({})
    elapsed = time.monotonic() - started

    kill_thread.join(timeout=3.0)

    pid = captured_pid.get("pid")
    assert pid is not None, "watcher never saw a live child PID"

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and _process_alive(pid):
        time.sleep(0.05)
    alive_after = _process_alive(pid)
    print(
        f"[on_kill_during_run] pid={pid} elapsed={elapsed:.2f}s "
        f"alive_after_on_kill={alive_after} (must be False)"
    )

    # Cleanup also ran: PytestOperator.on_kill() calls
    # self._runner.cleanup(success=False) defensively. We don't assert
    # the directory state here (the operator's own finally also runs and
    # cleanup is idempotent under lock), only that no exception leaked.
    assert not alive_after, (
        f"on_kill failed to terminate pytest subprocess (pid={pid}); "
        "Airflow's SIGTERM path would leave an orphan."
    )
    # Sanity: the run did NOT linger past the kill -- elapsed should be
    # well under the test_long's 30s, demonstrating the kill actually
    # short-circuited the wait.
    assert elapsed < 10.0, (
        f"execute() took {elapsed:.2f}s -- on_kill should short-circuit "
        "the wait, but the test ran far too long."
    )


@pytest.mark.parametrize("bad", [0, -1, -1024])
def test_runner_rejects_non_positive_max_output_bytes(bad):
    with pytest.raises(ValueError, match="max_output_bytes"):
        SubprocessPytestRunner(max_output_bytes=bad)


def test_runner_truncates_stdout_when_cap_exceeded(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        def test_noisy():
            # ~200 KiB of stdout, an order of magnitude above the cap.
            for i in range(2000):
                print('x' * 100)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(
        f"[truncate] exit={artifacts.exit_code} "
        f"stdout_len={len(artifacts.stdout)} cap={cap}"
    )
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert "stdout truncated at" in artifacts.stdout
    assert len(artifacts.stdout.encode("utf-8")) <= cap + 1024


def test_runner_truncates_stderr_when_cap_exceeded(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        import sys
        def test_noisy_stderr():
            for i in range(2000):
                print('e' * 100, file=sys.stderr)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert "stderr truncated at" in artifacts.stderr
    assert len(artifacts.stderr.encode("utf-8")) <= cap + 1024


def test_runner_caps_single_oversized_chunk_at_exact_limit(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        def test_one_huge_line():
            # A single ~1 MiB line -> one readline() chunk, ~250x the cap. With
            # the old pre-append check this whole chunk would be captured.
            print('y' * 1000000)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    assert artifacts.exit_code == 0
    assert "stdout truncated at" in artifacts.stdout

    marker = "\n...(stdout truncated"
    body = artifacts.stdout[: artifacts.stdout.index(marker)]
    print(f"[overshoot] body_len={len(body)} cap={cap}")
    # The captured body (everything before the marker) is clamped to exactly
    # the cap -- not cap + one giant chunk.
    assert len(body) == cap


def test_truncation_marker_reports_characters_unit(tmp_path):
    cap = 2048
    path = _suite(
        tmp_path,
        """
        def test_noisy():
            for _ in range(1000):
                print('z' * 100)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(f"[marker] tail={artifacts.stdout[-120:]!r}")
    assert f"stdout truncated at {cap} characters" in artifacts.stdout
    # The old "~N chars" phrasing is gone.
    assert "chars;" not in artifacts.stdout


def test_runner_does_not_truncate_when_cap_disabled(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_quiet():
            print('hello-from-child')
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=None)
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(f"[no-cap] stdout_len={len(artifacts.stdout)}")
    assert artifacts.exit_code == 0
    assert "hello-from-child" in artifacts.stdout
    assert "truncated" not in artifacts.stdout
    assert "truncated" not in artifacts.stderr


def test_drainer_size_counting_is_fast_on_long_suite_output():
    import time

    # Realistic chunk: ~80 chars of pytest output, plain ASCII. We use a
    # mix of plain ASCII (the common pytest case) and a small percentage
    # of non-ASCII (test names occasionally contain Cyrillic / emoji) so
    # the benchmark reflects a realistic mix, not a strawman.
    ascii_line = "tests/test_module_007.py::TestClass::test_method[param=42] PASSED\n"
    unicode_line = "tests/test_кириллица.py::test_тест_💥 PASSED\n"
    chunks = ([ascii_line] * 100 + [unicode_line]) * 100  # ~10_100 lines

    # Old path (what we replaced): allocate a bytes object per line and
    # take its length. We inline-reproduce it so the comparison is
    # against the actual previous implementation, not a memory of it.
    def old_count(chunks):
        total = 0
        for c in chunks:
            total += len(c.encode("utf-8", errors="replace"))
        return total

    def new_count(chunks):
        total = 0
        for c in chunks:
            total += len(c)
        return total

    # Warm-up
    old_count(chunks)
    new_count(chunks)

    iters = 50

    t0 = time.perf_counter()
    for _ in range(iters):
        old_total_bytes = old_count(chunks)
    old_elapsed = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(iters):
        new_total_chars = new_count(chunks)
    new_elapsed = time.perf_counter() - t0

    speedup = old_elapsed / max(new_elapsed, 1e-9)

    under_ratio = old_total_bytes / max(new_total_chars, 1)

    print(
        f"[drainer_perf] chunks={len(chunks)} iters={iters} "
        f"old_encode={old_elapsed * 1000:.1f}ms "
        f"new_len={new_elapsed * 1000:.1f}ms "
        f"speedup={speedup:.1f}x "
        f"under_count_ratio={under_ratio:.3f}x"
    )

    assert new_elapsed < old_elapsed, (
        f"len(chunk) ({new_elapsed * 1000:.1f}ms) is not faster than "
        f"chunk.encode().len() ({old_elapsed * 1000:.1f}ms) -- something "
        "regressed the optimisation."
    )

    assert 1.0 <= under_ratio < 1.10, (
        f"under_count_ratio={under_ratio:.3f} -- the realistic mix should "
        "stay well under 1.10 for ASCII-dominant pytest output."
    )


def test_dry_run_only_collects_does_not_execute_test_bodies(tmp_path):
    from airflow_pytest_operator.operators import PytestOperator

    marker = tmp_path / "test_body_executed"
    suite = tmp_path / "test_x.py"
    suite.write_text(
        textwrap.dedent(
            f"""
            import pathlib

            def test_would_fail():
                # Sentinel: prove whether the body ran.
                pathlib.Path({str(marker)!r}).touch()
                assert False, "this would fail if executed"

            def test_would_pass():
                pathlib.Path({str(marker)!r}).touch()
                assert True
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    op = PytestOperator(
        task_id="t",
        test_path=str(suite),
        dry_run=True,
        runner=runner,
    )

    summary = op.execute({})

    print(
        f"[dry_run:e2e] exit_code={summary['exit_code']} "
        f"failed={summary['failed']} "
        f"marker_exists={marker.exists()}"
    )

    # THE essential property: no test body ran.
    assert not marker.exists(), (
        "test body executed despite dry_run=True -- the operator's "
        "--collect-only flag was not honoured"
    )
    assert summary["exit_code"] == 0
    assert summary["failed"] == 0
    assert summary["errors"] == 0
    assert summary["total"] == 0


def test_dry_run_collection_error_surfaces_as_task_failure(tmp_path):
    from airflow_pytest_operator.exceptions import TestsFailedError
    from airflow_pytest_operator.operators import PytestOperator

    suite = tmp_path / "test_broken.py"
    suite.write_text("def test_x(:  # invalid syntax\n    pass\n")

    runner = SubprocessPytestRunner()
    op = PytestOperator(
        task_id="t",
        test_path=str(suite),
        dry_run=True,
        runner=runner,
    )

    with pytest.raises(TestsFailedError):
        op.execute({})
    print(
        "[dry_run:collection_error] dry_run with SyntaxError raised "
        "TestsFailedError as expected -- collection errors are NOT "
        "silenced by --collect-only"
    )


def test_dry_run_with_junit_parser_collects_but_lacks_count(tmp_path):
    from airflow_pytest_operator import JUnitResultParser
    from airflow_pytest_operator.operators import PytestOperator

    suite = tmp_path / "test_x.py"
    suite.write_text(
        textwrap.dedent(
            """
            def test_a(): assert True
            def test_b(): assert True
            def test_c(): assert True
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    op = PytestOperator(
        task_id="t",
        test_path=str(suite),
        dry_run=True,
        runner=runner,
        parser=JUnitResultParser(),
    )
    summary = op.execute({})

    print(f"[dry_run:junit_limitation] total={summary['total']}")
    # Collection succeeded (exit code 0, no test failed) ...
    assert summary["exit_code"] == 0
    assert summary["failed"] == 0
    # ... but JUnit can't tell us how many tests were collected.
    assert summary["total"] == 0


# ---------------------------------------------------------------------------
# Multi-positional ``test_path``.
# ---------------------------------------------------------------------------


def test_run_with_list_of_paths_runs_them_all_as_positionals(tmp_path):
    file_a = tmp_path / "a" / "test_a.py"
    file_b = tmp_path / "b" / "test_b.py"
    file_a.parent.mkdir()
    file_b.parent.mkdir()
    file_a.write_text("def test_one(): assert True\n")
    file_b.write_text("def test_two(): assert True\n")

    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = runner.run(
        [str(file_a), str(file_b)],
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(
        f"[multi_paths] exit={artifacts.exit_code} "
        f"total={result.total} cases={[c.node_id for c in result.cases]}"
    )
    assert artifacts.exit_code == 0
    assert result.total == 2
    # Both files contributed exactly one test each.
    node_ids = sorted(c.name for c in result.cases)
    assert node_ids == ["test_one", "test_two"]


def test_run_with_list_of_node_id_selectors_filters_to_specific_tests(tmp_path):
    suite = tmp_path / "test_x.py"
    suite.write_text(
        textwrap.dedent(
            """
            def test_a(): assert True
            def test_b(): assert True
            def test_c(): assert True
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    # Re-run only test_a and test_c, skip test_b.
    artifacts = runner.run(
        [f"{suite}::test_a", f"{suite}::test_c"],
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    selected = sorted(c.name for c in result.cases)
    print(f"[multi_selectors] selected={selected}")
    assert selected == ["test_a", "test_c"]
    # test_b must NOT have been collected -- it's not in the selector list.
    assert all("test_b" not in name for name in selected)


def test_run_with_string_test_path_unchanged_behaviour(tmp_path):
    suite = tmp_path / "test_x.py"
    suite.write_text("def test_a(): assert True\n")

    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = runner.run(
        str(suite),  # str, not list -- exercises the normalisation path
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(f"[string_compat] total={result.total}")
    assert result.total == 1


def test_run_with_empty_list_raises_test_execution_error(tmp_path):
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    with pytest.raises(TestExecutionError, match="test_path must be a non-empty"):
        runner.run(
            [],
            pytest_args=[],
            report_request=parser.report_request,
        )
    print("[empty_list] raised TestExecutionError as expected")


def test_run_with_blank_only_targets_raises(tmp_path):
    # All targets blank (e.g. a Jinja expression that rendered to "") -> after
    # filtering nothing remains, so we fail like the empty-sequence case.
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    for bad in ("", "   ", ["", "  "]):
        with pytest.raises(TestExecutionError, match="non-blank"):
            runner.run(bad, pytest_args=[], report_request=parser.report_request)
    print("[blank_targets] raised TestExecutionError as expected")


def test_run_filters_blank_targets_but_keeps_valid_ones(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    with caplog.at_level(
        "WARNING", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, [path, "", "   "])

    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    msgs = [r.getMessage() for r in caplog.records]
    assert any("Ignoring 2 empty/blank test target" in m for m in msgs), msgs


def test_run_with_relative_node_id_selector_as_test_path_works(tmp_path):
    suite_dir = tmp_path / "tests"
    suite_dir.mkdir()
    suite_file = suite_dir / "test_x.py"
    suite_file.write_text("def test_y(): pass\ndef test_z(): pass\n")

    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        runner = SubprocessPytestRunner()
        parser = JUnitResultParser()
        artifacts = runner.run(
            "tests/test_x.py::test_y",  # relative selector
            pytest_args=[],
            report_request=parser.report_request,
        )
        result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    finally:
        os.chdir(orig_cwd)

    print(
        f"[relative_selector] exit={artifacts.exit_code} "
        f"total={result.total} cases={[c.name for c in result.cases]}"
    )
    # Selector matched -> 1 test ran, exit 0, no "file not found".
    assert artifacts.exit_code == 0
    assert result.total == 1
    assert "file or directory not found" not in (artifacts.stderr or "")
    # And pytest selected ONLY test_y, not test_z -- the whole point
    # of passing a specific selector.
    assert [c.name for c in result.cases] == ["test_y"]


def test_early_cancel_returns_artifacts_without_raising(tmp_path, monkeypatch):
    import subprocess as _subprocess

    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(grace_period=2.0)

    real_popen = _subprocess.Popen

    def _popen_then_cancel(*args, **kwargs):
        proc = real_popen(*args, **kwargs)
        # _proc not registered yet -> cancel() only flips the flag, and the
        # post-Popen check in _run_locked terminates the just-launched tree.
        runner.cancel()
        return proc

    monkeypatch.setattr(
        "airflow_pytest_operator.runners.subprocess_runner.subprocess.Popen",
        _popen_then_cancel,
    )

    # Does NOT raise -- a normal RunArtifacts comes back.
    artifacts = _run(runner, path)

    print(
        f"[early_cancel] exit_code={artifacts.exit_code} "
        f"report_path={artifacts.report_path!r}"
    )
    # No report was written (killed before pytest could produce one).
    assert artifacts.report_path is None
    # The exit code reflects termination by signal: on POSIX a process killed
    # by SIGTERM/SIGKILL surfaces as a negative returncode. The 60s sleep never
    # completed normally, so the code is non-zero either way.
    assert artifacts.exit_code != 0
    if os.name != "nt":
        assert artifacts.exit_code < 0, (
            "expected a signal-derived (negative) exit code after termination, "
            f"got {artifacts.exit_code}"
        )
    # Streams are ordinary strings, never None, even on the cancel path.
    assert isinstance(artifacts.stdout, str)
    assert isinstance(artifacts.stderr, str)


def test_report_request_exception_cleans_up_fallback_dir(tmp_path):
    # If the report_request callback raises (a buggy/strict custom parser),
    # the runner has created its fallback temp dir but not yet recorded
    # ownership on self, so a later cleanup() would never reach it. The runner
    # must remove the temp dir before propagating, otherwise every failed run
    # leaks an empty pytest_report_* dir under the system temp.
    path = _suite(tmp_path, "def test_a(): assert True")
    captured = {}

    def boom(report_dir):
        # The fallback dir exists at this point -- record it so we can assert
        # it was cleaned up after the exception unwinds.
        captured["dir"] = report_dir
        assert os.path.isdir(report_dir)
        raise RuntimeError("parser blew up")

    runner = SubprocessPytestRunner()
    with pytest.raises(RuntimeError, match="parser blew up"):
        runner.run(path, report_request=boom)

    print(f"fallback dir was: {captured.get('dir')!r}")
    assert "dir" in captured
    # The temp dir the runner offered must not survive the callback's failure.
    assert not os.path.exists(captured["dir"])
    # The runner claimed no ownership, so a follow-up cleanup() is a safe no-op.
    assert runner._created_report_dir is None
    runner.cleanup(success=False)  # must not raise


def test_timeout_error_carries_captured_streams(tmp_path):
    # On timeout the captured stdout/stderr must be reachable programmatically
    # (via the exception), not only via the worker log -- so an operator/UI can
    # show "why did it hang" without scraping logs.
    import sys as _sys
    import textwrap

    suite = tmp_path / "test_hang.py"
    suite.write_text(
        textwrap.dedent(
            """
            import os, time

            def test_hang():
                # Write straight to fd 1/2 so the bytes bypass pytest capture
                # and reach the pipe the runner drains, then hang until SIGKILL.
                os.write(1, b"hang-stdout-line\\n")
                os.write(2, b"hang-stderr-line\\n")
                time.sleep(30)
            """
        ).strip()
    )
    runner = SubprocessPytestRunner(
        python_executable=_sys.executable, timeout=1.5, grace_period=0.5
    )
    with pytest.raises(TestExecutionError, match="timed out") as excinfo:
        _run(runner, str(suite), pytest_args=["-s"])

    err = excinfo.value
    print(f"stdout attr: {err.stdout!r}\nstderr attr: {err.stderr!r}")
    assert err.stdout is not None and err.stderr is not None
    assert "hang-stdout-line" in err.stdout
    assert "hang-stderr-line" in err.stderr


def test_execution_error_without_output_has_none_streams(tmp_path):
    # A launch failure (missing interpreter) has no associated child output:
    # the stream attributes default to None, and the plain single-arg
    # construction keeps working.
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(python_executable="/no/such/python")
    with pytest.raises(TestExecutionError) as excinfo:
        _run(runner, path)
    assert excinfo.value.stdout is None
    assert excinfo.value.stderr is None


def test_invalid_timeout_rejected():
    # Non-positive timeout would make proc.wait() raise immediately, turning
    # every run into an instant timeout -- reject it at construction.
    with pytest.raises(ValueError, match="timeout"):
        SubprocessPytestRunner(timeout=0)
    with pytest.raises(ValueError, match="timeout"):
        SubprocessPytestRunner(timeout=-5)
    # None (no limit) and a positive value are both fine.
    SubprocessPytestRunner(timeout=None)
    SubprocessPytestRunner(timeout=30)


def test_invalid_grace_period_rejected():
    with pytest.raises(ValueError, match="grace_period"):
        SubprocessPytestRunner(grace_period=-1.0)
    # Zero is valid: SIGTERM then escalate to SIGKILL immediately.
    SubprocessPytestRunner(grace_period=0)


def test_run_without_env_overrides_inherits_worker_environment(tmp_path, monkeypatch):
    # With no env overrides the runner passes env=None to Popen so the child
    # inherits the worker's environment directly (no os.environ.copy needed).
    # Prove inheritance works: a var set on the worker is visible to pytest.
    monkeypatch.setenv("INHERITED_FLAG", "from-worker")
    path = _suite(
        tmp_path,
        """
        import os
        def test_inherits():
            assert os.environ.get("INHERITED_FLAG") == "from-worker"
        """,
    )
    artifacts = _run(SubprocessPytestRunner(), path)
    print(f"exit_code={artifacts.exit_code}")
    assert artifacts.exit_code == 0


def test_terminate_falls_back_to_direct_kill_on_killpg_oserror(monkeypatch):
    # _terminate must never let an OSError escape (it runs on the on_kill /
    # timeout paths). If killpg raises something other than ProcessLookupError
    # -- e.g. PermissionError when the child changed gid, or a racing ESRCH --
    # the runner falls back to killing the direct child and returns quietly.
    runner = SubprocessPytestRunner(grace_period=0.1)

    class _FakeProc:
        pid = 4242
        returncode = -9

        def __init__(self):
            self._kill_called = False

        def poll(self):
            return None  # appears alive so _terminate proceeds

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            self._kill_called = True

    proc = _FakeProc()

    def _boom_killpg(*_args, **_kwargs):
        raise PermissionError("operation not permitted")

    # Patch both the group lookup/signal path. getpgid returns a pgid; killpg
    # raises PermissionError -> fallback to proc.kill().
    monkeypatch.setattr(os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(os, "killpg", _boom_killpg)

    # Must not raise, and must have attempted the direct-child kill.
    runner._terminate(proc)  # type: ignore[arg-type]
    print(f"direct kill called: {proc._kill_called}")
    assert proc._kill_called is True


def test_terminate_returns_quietly_when_group_already_gone(monkeypatch):
    # ProcessLookupError from killpg means the whole group already exited:
    # _terminate returns without falling back to a direct kill.
    runner = SubprocessPytestRunner(grace_period=0.1)

    class _FakeProc:
        pid = 5252
        returncode = 0

        def __init__(self):
            self._kill_called = False

        def poll(self):
            return None

        def wait(self, timeout=None):
            return 0

        def kill(self):
            self._kill_called = True

    proc = _FakeProc()

    def _gone_killpg(*_args, **_kwargs):
        raise ProcessLookupError("no such process")

    monkeypatch.setattr(os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(os, "killpg", _gone_killpg)

    runner._terminate(proc)  # type: ignore[arg-type]
    assert proc._kill_called is False


def test_cancel_landing_before_proc_registration_terminates_early(
    tmp_path, monkeypatch
):
    # The race-guard branch in run(): cancel() can set _cancelled in the tiny
    # window after Popen returns but before run() stores the handle in
    # self._proc. At that instant cancel() sees _proc is still None and only
    # flips the flag; the post-Popen check must honour it and terminate the
    # tree, so a just-launched run does not sail past an already-issued cancel.
    # We force the interleaving deterministically (no sleeps/threads) by making
    # Popen call cancel() itself right before handing back the process.
    import subprocess as _subprocess
    import time

    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(grace_period=2.0)

    real_popen = _subprocess.Popen

    def _popen_then_cancel(*args, **kwargs):
        proc = real_popen(*args, **kwargs)
        # _proc is not registered yet -> cancel() just sets the flag.
        runner.cancel()
        return proc

    monkeypatch.setattr(
        "airflow_pytest_operator.runners.subprocess_runner.subprocess.Popen",
        _popen_then_cancel,
    )

    start = time.monotonic()
    artifacts = _run(runner, path)
    elapsed = time.monotonic() - start
    print(f"elapsed={elapsed:.2f}s exit_code={artifacts.exit_code}")
    # The 60s sleep never completes: the early-cancel branch killed the tree.
    assert elapsed < 30, f"early cancel did not terminate the run: {elapsed:.1f}s"
    assert artifacts.report_path is None  # killed before any report was written
