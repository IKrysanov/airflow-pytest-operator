"""Operator tests with fully injected collaborators.

These tests prove the Dependency Inversion design pays off: we test the
operator's *orchestration logic* (fail policy, XCom push, logging order)
without a real subprocess, without parsing XML, and — via a stubbed
BaseOperator in conftest — without Airflow installed.
"""

# Copyright 2026 Ilya Krysanov
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

import pytest

from airflow_pytest_operator.exceptions import TestsFailedError
from airflow_pytest_operator.models import RunArtifacts, TestRunResult
from airflow_pytest_operator.operators import PytestOperator


class FakeRunner:
    """Records how it was called and returns canned artifacts."""

    def __init__(self, artifacts: RunArtifacts):
        self._artifacts = artifacts
        self.calls = []
        self.cancelled = 0
        self.cleanup_calls = []

    def run(self, test_path, *, pytest_args=None, env=None):
        self.calls.append(
            {"test_path": test_path, "pytest_args": pytest_args, "env": env}
        )
        return self._artifacts

    def cancel(self):
        self.cancelled += 1

    def cleanup(self, *, success=True):
        self.cleanup_calls.append(success)


class FakeParser:
    """Returns a canned result regardless of input."""

    def __init__(self, result: TestRunResult):
        self._result = result
        self.parsed_paths = []

    def parse(self, report_path, *, exit_code=0):
        self.parsed_paths.append((report_path, exit_code))
        return self._result


class FakeTI:
    def __init__(self):
        self.pushed = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _result(*, failed=0, errors=0, passed=1):
    total = passed + failed + errors
    return TestRunResult(
        total=total,
        passed=passed,
        failed=failed,
        skipped=0,
        errors=errors,
        duration=0.1,
        exit_code=0 if not (failed or errors) else 1,
    )


def _ctx():
    return {"ti": FakeTI()}


def test_passing_run_returns_summary_for_xcom():
    runner = FakeRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(passed=3))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)

    ctx = _ctx()
    out = op.execute(ctx)

    # The summary is the return value; Airflow pushes it under return_value.
    assert out["success"] is True
    assert out["passed"] == 3
    # No custom key is pushed -- we rely solely on return_value now.
    assert ctx["ti"].pushed == {}
    assert op.do_xcom_push is True  # default: Airflow will push return_value
    # orchestration order: runner called, then parser fed its xml path
    assert runner.calls[0]["test_path"] == "tests/"
    assert parser.parsed_paths[0] == ("/x.xml", 0)


def test_failing_run_raises_by_default():
    runner = FakeRunner(RunArtifacts(exit_code=1, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(failed=2))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)

    with pytest.raises(TestsFailedError) as exc:
        op.execute(_ctx())
    assert exc.value.result.failed == 2


def test_fail_on_test_failure_false_swallows_failure():
    runner = FakeRunner(RunArtifacts(exit_code=1, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(failed=2))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )

    out = op.execute(_ctx())  # must NOT raise
    assert out["success"] is False
    assert out["failed"] == 2


def test_do_xcom_push_defaults_true_and_returns_summary():
    runner = FakeRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)
    out = op.execute(_ctx())
    # Default: Airflow will push the returned summary under return_value.
    assert op.do_xcom_push is True
    assert out["success"] is True


def test_do_xcom_push_false_is_respected():
    # No custom flag: users disable XCom with Airflow's standard parameter.
    runner = FakeRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=parser,
        do_xcom_push=False,
    )
    out = op.execute(_ctx())
    assert op.do_xcom_push is False
    # execute still returns the summary for in-process callers/tests.
    assert out["success"] is True


def test_args_and_env_forwarded_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/smoke",
        pytest_args=["-k", "smoke"],
        env={"E": "1"},
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    assert runner.calls[0]["pytest_args"] == ["-k", "smoke"]
    assert runner.calls[0]["env"] == {"E": "1"}


def test_missing_report_raises_execution_error():
    # Runner ran but produced no JUnit XML (crash/collection error).
    runner = FakeRunner(
        RunArtifacts(
            exit_code=2,
            junit_xml_path=None,
            stderr="ERROR: file or directory not found",
        )
    )
    parser = FakeParser(_result(passed=1))  # should never be called
    op = PytestOperator(task_id="t", test_path="bad/path", runner=runner, parser=parser)

    from airflow_pytest_operator.exceptions import TestExecutionError

    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    assert "no JUnit report" in str(exc.value)
    assert "not found" in str(exc.value)  # stderr is surfaced
    assert parser.parsed_paths == []  # parser never reached


def test_cleanup_called_on_success():
    runner = FakeRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=3)),
    )
    op.execute(_ctx())
    assert runner.cleanup_calls == [True]


def test_cleanup_called_false_on_test_failure():
    runner = FakeRunner(RunArtifacts(exit_code=1, junit_xml_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(failed=2)),
    )
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())
    assert runner.cleanup_calls == [False]  # ran via finally, reported failure


def test_cleanup_called_false_on_missing_report():
    runner = FakeRunner(RunArtifacts(exit_code=2, junit_xml_path=None))
    op = PytestOperator(
        task_id="t",
        test_path="bad/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    from airflow_pytest_operator.exceptions import TestExecutionError

    with pytest.raises(TestExecutionError):
        op.execute(_ctx())
    assert runner.cleanup_calls == [False]


def test_on_kill_delegates_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)

    op.on_kill()
    assert runner.cancelled == 1
    # Killed run must also clean up the temp dir (default "always" policy).
    assert runner.cleanup_calls == [False]


def test_on_kill_never_raises_even_if_cancel_fails():
    class ExplodingRunner(FakeRunner):
        def cancel(self):
            raise RuntimeError("cannot cancel")

    runner = ExplodingRunner(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )

    # Must swallow the error -- teardown must not raise. cleanup still runs.
    op.on_kill()
    assert runner.cleanup_calls == [False]


def test_on_kill_never_raises_even_if_cleanup_fails():
    class ExplodingCleanup(FakeRunner):
        def cleanup(self, *, success=True):
            raise RuntimeError("cannot clean")

    runner = ExplodingCleanup(RunArtifacts(exit_code=0, junit_xml_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.on_kill()  # must not raise despite cleanup blowing up


def test_default_collaborators_are_wired():
    # No injection -> real defaults, proving the DI defaults exist.
    from airflow_pytest_operator.reporters import JUnitResultParser
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    op = PytestOperator(task_id="t", test_path="tests/")
    assert isinstance(op._runner, SubprocessPytestRunner)
    assert isinstance(op._parser, JUnitResultParser)


def test_stdout_and_stderr_are_logged():
    # The operator must surface child stdout/stderr in the task log. We assert
    # on the operator's own logging contract by spying on the logger's methods
    # rather than using `caplog`: on Airflow 3 the operator logger is
    # `airflow.task...`, whose records Airflow routes through its own
    # task-logging, so a root-handler `caplog` wouldn't see them. Patching the
    # logger methods proves the contract ("we called self.log.info/warning
    # with this content") independent of how any Airflow version wires
    # handlers. `BaseOperator.log` is a read-only property on real Airflow, so
    # we patch its methods in place rather than reassigning `op.log`.
    from unittest import mock

    runner = FakeRunner(
        RunArtifacts(
            exit_code=0,
            junit_xml_path="/x.xml",
            stdout="some-stdout-line",
            stderr="some-stderr-line",
        )
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with (
        mock.patch.object(op.log, "info") as info,
        mock.patch.object(op.log, "warning") as warning,
    ):
        op.execute(_ctx())

    logged = " ".join(str(c) for c in info.call_args_list + warning.call_args_list)
    assert "some-stdout-line" in logged
    assert "some-stderr-line" in logged


def test_failed_node_ids_are_logged():
    # When the parsed result has failed cases, the operator logs their node
    # ids at error level. Same logger-spy rationale as above.
    from unittest import mock

    def _result_with_failed_cases():
        # A result whose cases yield non-empty failed_node_ids, exercising the
        # operator's "Failed tests:" logging branch (pytest_operator.py:127).
        from airflow_pytest_operator.models import CaseResult

        cases = [
            CaseResult(
                name="test_bad",
                classname="tests.test_api",
                time=0.1,
                outcome="failed",
            ),
        ]
        return TestRunResult(
            total=1,
            passed=0,
            failed=1,
            skipped=0,
            errors=0,
            duration=0.1,
            exit_code=1,
            cases=cases,
        )

    runner = FakeRunner(RunArtifacts(exit_code=1, junit_xml_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result_with_failed_cases()),
        fail_on_test_failure=False,  # don't raise; we only check logging
    )
    with mock.patch.object(op.log, "error") as error:
        op.execute(_ctx())

    logged = " ".join(str(c) for c in error.call_args_list)
    assert "tests.test_api::test_bad" in logged
