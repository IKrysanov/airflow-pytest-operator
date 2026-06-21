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


"""Core orchestration: fail policy, XCom push, cleanup, on_kill, logging, and
missing/parse-error report handling. Shared fakes in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    _ctx,
    _result,
)

from airflow_pytest_operator.exceptions import TestsFailedError
from airflow_pytest_operator.models import RunArtifacts, TestRunResult
from airflow_pytest_operator.operators import PytestOperator


def test_passing_run_returns_summary_for_xcom():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=3))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)

    ctx = _ctx()
    out = op.execute(ctx)
    print(f"xcom summary: {out}")

    assert out["success"] is True
    assert out["passed"] == 3
    assert ctx["ti"].pushed == {}
    assert op.do_xcom_push is True
    assert runner.calls[0]["test_path"] == "tests/"
    assert parser.parsed_paths[0] == ("/x.xml", 0)


def test_sequence_test_path_forwarded_to_runner():
    # The operator accepts str | Sequence[str] and forwards a list of
    # targets verbatim; the runner splices them as positional pytest args.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=2))
    targets = ["tests/a/test_a.py", "tests/b/test_b.py"]
    op = PytestOperator(task_id="t", test_path=targets, runner=runner, parser=parser)

    out = op.execute(_ctx())
    print(f"forwarded test_path: {runner.calls[0]['test_path']!r}")

    assert out["success"] is True
    assert runner.calls[0]["test_path"] == targets


def test_failing_run_raises_by_default():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_result(failed=2))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)

    with pytest.raises(TestsFailedError) as exc:
        op.execute(_ctx())
    print(f"TestsFailedError: result.failed={exc.value.result.failed}")
    assert exc.value.result.failed == 2


def test_fail_on_test_failure_false_swallows_failure():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_result(failed=2))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )

    out = op.execute(_ctx())
    print(f"out: success={out['success']}, failed={out['failed']}")
    assert out["success"] is False
    assert out["failed"] == 2


def test_do_xcom_push_defaults_true_and_returns_summary():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)
    out = op.execute(_ctx())
    assert op.do_xcom_push is True
    assert out["success"] is True


def test_do_xcom_push_false_is_respected():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
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
    assert out["success"] is True


def test_args_and_env_forwarded_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
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
    print(f"pytest_args forwarded: {runner.calls[0]['pytest_args']}")
    print(f"env forwarded: {runner.calls[0]['env']}")
    assert runner.calls[0]["pytest_args"] == ["-k", "smoke"]
    assert runner.calls[0]["env"] == {"E": "1"}


def test_missing_report_raises_execution_error():
    runner = FakeRunner(
        RunArtifacts(
            exit_code=2,
            report_path=None,
            stderr="ERROR: file or directory not found",
        )
    )
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="bad/path", runner=runner, parser=parser)

    from airflow_pytest_operator.exceptions import TestExecutionError

    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    msg = str(exc.value)
    print(f"TestExecutionError: {msg!r}")
    # The error names the parser class so users can tell which format was
    # expected (FakeParser here; "JUnitResultParser" / "JSONResultParser"
    # in real installs).
    assert "produced no" in msg
    assert "FakeParser" in msg
    assert "report" in msg
    assert "not found" in msg
    assert parser.parsed_paths == []


def test_cleanup_called_on_success():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=3)),
    )
    op.execute(_ctx())
    assert runner.cleanup_calls == [True]


def test_cleanup_called_false_on_test_failure():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(failed=2)),
    )
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())
    assert runner.cleanup_calls == [False]


def test_cleanup_called_false_on_missing_report():
    runner = FakeRunner(RunArtifacts(exit_code=2, report_path=None))
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
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)

    op.on_kill()
    assert runner.cancelled == 1
    assert runner.cleanup_calls == [False]


def test_on_kill_never_raises_even_if_cancel_fails():
    class ExplodingRunner(FakeRunner):
        def cancel(self):
            raise RuntimeError("cannot cancel")

    runner = ExplodingRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )

    op.on_kill()
    assert runner.cleanup_calls == [False]


def test_on_kill_never_raises_even_if_cleanup_fails():
    class ExplodingCleanup(FakeRunner):
        def cleanup(self, *, success=True):
            raise RuntimeError("cannot clean")

    runner = ExplodingCleanup(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.on_kill()


def test_default_collaborators_are_wired():
    from airflow_pytest_operator.reporters import JUnitResultParser
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    op = PytestOperator(task_id="t", test_path="tests/")
    assert isinstance(op._runner, SubprocessPytestRunner)
    assert isinstance(op._parser, JUnitResultParser)


def test_operator_does_not_mutate_injected_runner():
    # The operator wires collaborators without mutating the injected runner;
    # the report location flows through report_request, not runner config.
    from airflow_pytest_operator.reporters import JUnitResultParser
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    injected = SubprocessPytestRunner(cleanup="never")
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=injected,
        parser=JUnitResultParser(report_dir="/parser/dir"),
    )
    assert op._runner is injected
    assert injected._cleanup == "never"  # untouched
    assert not hasattr(injected, "_report_dir")  # runner no longer owns it


def test_stdout_and_stderr_are_logged():
    from unittest import mock

    runner = FakeRunner(
        RunArtifacts(
            exit_code=0,
            report_path="/x.xml",
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
    print(f"logged (stdout+stderr calls): {logged[:300]!r}")
    assert "some-stdout-line" in logged
    assert "some-stderr-line" in logged


def test_failed_node_ids_are_logged():
    from unittest import mock

    def _result_with_failed_cases():
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

    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result_with_failed_cases()),
        fail_on_test_failure=False,
    )
    with mock.patch.object(op.log, "error") as error:
        op.execute(_ctx())

    logged = " ".join(str(c) for c in error.call_args_list)
    print(f"error logged: {logged!r}")
    assert "tests.test_api::test_bad" in logged


def test_operator_forwards_parser_report_request_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="tests/", runner=runner, parser=parser)
    op.execute(_ctx())

    forwarded = runner.calls[0]["report_request"]
    assert forwarded.__self__ is parser
    assert forwarded.__func__ is type(parser).report_request
    assert parser.report_request_calls == ["/fake/report/dir"]
    assert runner.calls[0]["spec"].pytest_args == ("--fake-report",)


def test_missing_report_error_names_the_parser_class():
    runner = FakeRunner(RunArtifacts(exit_code=2, report_path=None, stderr="boom"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="x/", runner=runner, parser=parser)

    from airflow_pytest_operator.exceptions import TestExecutionError

    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    assert "FakeParser" in str(exc.value)


def test_missing_report_error_truncates_huge_stderr():
    huge = "x" * 4097
    runner = FakeRunner(RunArtifacts(exit_code=2, report_path=None, stderr=huge))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="x/", runner=runner, parser=parser)

    from airflow_pytest_operator.exceptions import TestExecutionError

    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    msg = str(exc.value)
    print(f"truncated msg (len={len(msg)}): {msg[:200]!r}")
    assert "...(truncated)" in msg
    assert huge not in msg


def test_missing_report_error_handles_empty_stderr():
    runner = FakeRunner(RunArtifacts(exit_code=137, report_path=None, stderr=""))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(task_id="t", test_path="x/", runner=runner, parser=parser)

    from airflow_pytest_operator.exceptions import TestExecutionError

    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    print(f"empty stderr msg: {str(exc.value)!r}")
    assert "<empty>" in str(exc.value)


def test_report_parse_error_propagates_and_cleanup_called():
    """If the parser raises ReportParseError, it propagates out of execute()
    and cleanup() is still called with success=False."""
    from airflow_pytest_operator.exceptions import ReportParseError
    from airflow_pytest_operator.models import ReportRequest

    class ErrorParser:
        def report_request(self, report_dir):
            return ReportRequest(pytest_args=(), report_path=f"{report_dir}/x.json")

        def parse(self, report_path, *, exit_code=0):
            raise ReportParseError("malformed report")

    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.json"))
    op = PytestOperator(
        task_id="t", test_path="tests/", runner=runner, parser=ErrorParser()
    )

    with pytest.raises(ReportParseError, match="malformed"):
        op.execute(_ctx())

    assert runner.cleanup_calls == [False]


def test_report_parse_error_is_not_swallowed_by_fail_on_test_failure_false():
    """fail_on_test_failure=False must NOT suppress ReportParseError -- that is
    an infrastructure error, not a test-result decision."""
    from airflow_pytest_operator.exceptions import ReportParseError
    from airflow_pytest_operator.models import ReportRequest

    class ErrorParser:
        def report_request(self, report_dir):
            return ReportRequest(pytest_args=(), report_path=f"{report_dir}/x.json")

        def parse(self, report_path, *, exit_code=0):
            raise ReportParseError("corrupt")

    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.json"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=ErrorParser(),
        fail_on_test_failure=False,
    )

    with pytest.raises(ReportParseError):
        op.execute(_ctx())
