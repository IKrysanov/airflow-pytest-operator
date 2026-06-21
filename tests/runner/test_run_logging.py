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


"""Logging and cleanup-decision diagnostics; _is_within path checks. Shared fakes
in _run_helpers."""

from __future__ import annotations

import logging
import os

import pytest
from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner


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
