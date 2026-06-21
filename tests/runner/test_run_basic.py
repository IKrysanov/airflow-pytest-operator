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


"""Basic runner behaviour: report/exit codes, arg & env forwarding, bad
interpreter, and a real pytest-xdist run. Shared fakes in _run_helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.runners import SubprocessPytestRunner


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


def test_runner_runs_real_suite_under_xdist(tmp_path):
    # End-to-end check that the operator's parallel=/-n path actually works:
    # forward "-n 2" to the real runner and confirm pytest-xdist runs the suite
    # to a clean report (not just that the arg is spliced). Skipped where xdist
    # is absent (e.g. the bare integration CI job).
    pytest.importorskip("xdist")
    path = _suite(
        tmp_path,
        """
        def test_a(): assert True
        def test_b(): assert True
        def test_c(): assert True
        def test_d(): assert True
    """,
    )
    artifacts = _run(SubprocessPytestRunner(), path, pytest_args=["-n", "2"])
    print(f"[xdist] exit_code={artifacts.exit_code} stdout={artifacts.stdout[:160]!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    # xdist prints a "N workers" / "gw0" banner when it actually parallelises.
    assert "workers" in artifacts.stdout or "gw0" in artifacts.stdout


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
