"""Operator tests with fully injected collaborators.

These tests prove the Dependency Inversion design pays off: we test the
operator's *orchestration logic* (fail policy, XCom push, logging order)
without a real subprocess, without parsing XML, and — via a stubbed
BaseOperator in conftest — without Airflow installed.
"""

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

    def run(self, test_path, *, pytest_args=None, env=None, report_request):
        spec = report_request("/fake/report/dir")
        self.calls.append(
            {
                "test_path": test_path,
                "pytest_args": pytest_args,
                "env": env,
                "report_request": report_request,
                "spec": spec,
            }
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
        self.report_request_calls = []

    def report_request(self, report_dir):
        from airflow_pytest_operator.models import ReportRequest

        self.report_request_calls.append(report_dir)
        return ReportRequest(
            pytest_args=("--fake-report",),
            report_path=f"{report_dir}/fake.report",
        )

    def parse(self, report_path, *, exit_code=0):
        self.parsed_paths.append((report_path, exit_code))
        return self._result


class FakeTI:
    def __init__(
        self, try_number=1, dag_id=None, task_id=None, run_id=None, max_tries=None
    ):
        self.pushed = {}
        # Airflow exposes the attempt number here: 1 on the first run, 2+
        # on retries. The operator reads it to decide whether to narrow a
        # 'failed_only' retry to --lf.
        self.try_number = try_number
        # max_tries: the operator compares it with try_number to detect the
        # final attempt (try_number > max_tries) for cache cleanup.
        self.max_tries = max_tries
        # Ids the operator uses to derive a per-task-instance pytest cache dir
        # for failed_only. Default None -> operator falls back to the default
        # cache location (so tests that don't care are unaffected).
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id

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


def _ctx(try_number=1, *, dag_id=None, task_id=None, run_id=None, max_tries=None):
    return {
        "ti": FakeTI(
            try_number=try_number,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            max_tries=max_tries,
        )
    }


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


def test_dry_run_appends_collect_only_to_runner_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dry_run:args] forwarded pytest_args = {forwarded_args!r}")

    assert forwarded_args[:2] == ["-k", "smoke"]
    assert forwarded_args.count("--collect-only") == 1
    assert forwarded_args[-1] == "--collect-only"


def test_dry_run_false_does_not_append_collect_only():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        # dry_run=False is the default; explicit to make intent clear
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dry_run:default_off] forwarded pytest_args = {forwarded_args!r}")
    assert "--collect-only" not in forwarded_args
    assert forwarded_args == ["-k", "smoke"]


def test_dry_run_does_not_mutate_user_pytest_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    user_args = ["-k", "smoke", "-v"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())
    op.execute(_ctx())  # second execute (retry simulation)

    print(f"[dry_run:no_mutation] op.pytest_args after two runs = {op.pytest_args!r}")
    assert op.pytest_args == ["-k", "smoke", "-v"]
    for call in runner.calls:
        assert call["pytest_args"].count("--collect-only") == 1


def test_dry_run_default_is_false():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[dry_run:default_pin] op.dry_run = {op.dry_run}")
    assert op.dry_run is False


def test_dry_run_logs_indicate_mode():
    # Airflow operator loggers do NOT always propagate to root, so pytest's
    # ``caplog`` fixture misses them on some Airflow versions. We capture
    # at the source: mock op.log.info and inspect what was called. This
    # mirrors the pattern used by test_stdout_and_stderr_are_logged.
    from unittest import mock

    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    with mock.patch.object(op.log, "info") as info:
        op.execute(_ctx())

    # Flatten all info() calls into one string so users searching for
    # either "dry-run" or "--collect-only" find the matching line.
    logged = " ".join(str(c) for c in info.call_args_list)
    print(f"[dry_run:log] info() calls: {logged!r}")
    assert "dry-run" in logged
    assert "--collect-only" in logged


def test_dry_run_does_not_double_add_when_user_passed_collect_only():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke", "--collect-only"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:explicit] forwarded = {forwarded_args!r}")

    assert forwarded_args.count("--collect-only") == 1
    assert forwarded_args == ["-k", "smoke", "--collect-only"]


def test_dry_run_recognises_collectonly_alias():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--collectonly"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:legacy_alias] forwarded = {forwarded_args!r}")
    # Operator left the user's alias in place AND did not append its own
    # --collect-only on top.
    assert "--collect-only" not in forwarded_args
    assert forwarded_args == ["--collectonly"]


def test_dry_run_recognises_co_short_alias():
    # ``--co`` is the short alias. Same dedup principle.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--co"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:short_alias] forwarded = {forwarded_args!r}")
    assert "--collect-only" not in forwarded_args
    assert forwarded_args == ["--co"]


def test_dry_run_dedup_does_not_touch_other_repeated_flags():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=[
            "-v",
            "-v",  # extra-verbose
            "-o",
            "console_output_style=count",  # paired -o #1
            "-o",
            "junit_family=xunit2",  # paired -o #2
            "--ignore=tests/slow",  # ignore #1
            "--ignore=tests/flaky",  # ignore #2
        ],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:narrow] forwarded = {forwarded_args!r}")
    expected_user_args = [
        "-v",
        "-v",
        "-o",
        "console_output_style=count",
        "-o",
        "junit_family=xunit2",
        "--ignore=tests/slow",
        "--ignore=tests/flaky",
    ]
    # User's args, in order, followed by our appended --collect-only.
    assert forwarded_args[:-1] == expected_user_args
    assert forwarded_args[-1] == "--collect-only"
    # Total count: 8 user args + 1 appended --collect-only = 9.
    assert len(forwarded_args) == 9


def test_dry_run_false_with_collect_only_in_args_still_runs_collection():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--collect-only", "-k", "smoke"],
        dry_run=False,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:user_explicit_dry_run_off] forwarded = {forwarded_args!r}")
    assert forwarded_args == ["--collect-only", "-k", "smoke"]


# ---------------------------------------------------------------------------
# test_retry_strategy="failed_only" -- re-run only the failed tests on the next
# Airflow retry, carrying the failed node-ids through an Airflow Variable.
# ---------------------------------------------------------------------------


class FakeStore:
    """In-memory stand-in for VariableLastFailedStore.

    Records every read/write/delete so tests can assert the cross-retry
    bookkeeping without touching a real Airflow Variable.
    """

    def __init__(self, initial=None):
        self.data = dict(initial or {})
        self.reads = []
        self.writes = []
        self.deletes = []

    def read(self, key):
        self.reads.append(key)
        return list(self.data.get(key, []))

    def write(self, key, node_ids):
        self.writes.append((key, list(node_ids)))
        self.data[key] = list(node_ids)

    def delete(self, key):
        self.deletes.append(key)
        self.data.pop(key, None)


def _key(dag_id="d", task_id="t", run_id="r"):
    """The Variable key the operator derives for these ids (real derivation)."""
    from airflow_pytest_operator.stores import last_failed_var_key

    return last_failed_var_key(_ctx(dag_id=dag_id, task_id=task_id, run_id=run_id))


def test_test_retry_strategy_default_is_all():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[retry:default_pin] op.test_retry_strategy = {op.test_retry_strategy!r}")
    assert op.test_retry_strategy == "all"


def test_invalid_test_retry_strategy_raises_value_error():
    with pytest.raises(ValueError, match="test_retry_strategy"):
        PytestOperator(task_id="t", test_path="tests/", test_retry_strategy="bogus")


def test_failed_only_first_attempt_runs_full_suite_and_records_failures():
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=2))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )

    op.execute(_ctx(try_number=1, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:first] test_path={runner.calls[0]['test_path']!r} "
          f"reads={store.reads} writes={store.writes}")
    # First attempt: the store (keyed by this run_id) is empty, so the read
    # finds nothing and the full suite runs -- no explicit is-retry check needed.
    assert store.reads == [_key()]
    assert runner.calls[0]["test_path"] == "tests/"
    # The failing node-id is recorded for the next retry to narrow to.
    assert store.writes == [(_key(), ["tests.test_x::test_a"])]
    # No pytest --lf flag is involved anymore.
    assert "--lf" not in runner.calls[0]["pytest_args"]


def test_failed_only_retry_narrows_to_stored_failures():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a", "tests.test_y::test_b"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=2))     # the narrowed run passes
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:retry] test_path={runner.calls[0]['test_path']!r}")
    # The retry runs ONLY the previously-failed tests, converted to selectors.
    assert runner.calls[0]["test_path"] == [
        "tests/test_x.py::test_a",
        "tests/test_y.py::test_b",
    ]
    assert store.reads == [key]
    # User pytest_args are forwarded untouched -- no --lf appended.
    assert runner.calls[0]["pytest_args"] == ["-k", "smoke"]


def test_failed_only_retry_with_empty_store_runs_full_suite():
    store = FakeStore()                          # nothing stored
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:retry_empty] test_path={runner.calls[0]['test_path']!r}")
    # No stored failures -> safe fallback to the full suite.
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == [_key()]


def test_strategy_all_ignores_store_even_on_retry():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="all",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=3, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:all] test_path={runner.calls[0]['test_path']!r}")
    assert runner.calls[0]["test_path"] == "tests/"
    # Strategy "all" never touches the store.
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_never_appends_lf():
    store = FakeStore({_key(): ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[failed_only:no_lf] forwarded = {forwarded_args!r}")
    assert forwarded_args == ["-k", "smoke"]     # no --lf, no -o cache_dir=...


def test_failed_only_does_not_mutate_user_config_across_retries():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    user_args = ["-k", "smoke"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=1, dag_id="d", task_id="t", run_id="r"))
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:no_mutation] pytest_args={op.pytest_args!r} "
          f"test_path={op.test_path!r}")
    # Neither the stored pytest_args nor test_path are mutated by narrowing.
    assert op.pytest_args == ["-k", "smoke"]
    assert op.test_path == "tests/"


def test_failed_only_logs_on_retry():
    from unittest import mock

    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    with mock.patch.object(op.log, "info") as info:
        op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    logged = " ".join(str(c) for c in info.call_args_list)
    print(f"[failed_only:log] info() calls: {logged!r}")
    assert "failed_only" in logged
    assert "narrowing" in logged


def test_failed_only_missing_ti_in_context_degrades_to_full_suite():
    """A context without a usable 'ti' must not crash execute(); it should
    behave as the first attempt (full suite, store untouched)."""
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute({})  # no "ti" key at all -> no derivable key

    print(f"[failed_only:no_ti] test_path={runner.calls[0]['test_path']!r}")
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


# ---------------------------------------------------------------------------
# rerun_failed: in-process re-run of ONLY the failed tests (no cache, no XCom)
# ---------------------------------------------------------------------------


class SequenceParser:
    """Returns canned results in sequence -- one per parse() call.

    Lets a test script several pytest rounds (first full run, then reruns):
    parse() returns results[0], results[1], ... and clamps to the last one.
    """

    def __init__(self, results):
        self._results = list(results)
        self._i = 0
        self.parsed_paths = []
        self.report_request_calls = []

    def report_request(self, report_dir):
        from airflow_pytest_operator.models import ReportRequest

        self.report_request_calls.append(report_dir)
        return ReportRequest(
            pytest_args=("--fake-report",),
            report_path=f"{report_dir}/fake.report",
        )

    def parse(self, report_path, *, exit_code=0):
        self.parsed_paths.append((report_path, exit_code))
        result = self._results[min(self._i, len(self._results) - 1)]
        self._i += 1
        return result


def _res(failed_ids=(), *, passed=0):
    """Build a TestRunResult whose failed_node_ids == list(failed_ids)."""
    from airflow_pytest_operator.models import CaseResult

    cases = [
        CaseResult(
            name=fid.partition("::")[2],
            classname=fid.partition("::")[0],
            time=0.0,
            outcome="failed",
        )
        for fid in failed_ids
    ]
    failed = len(cases)
    return TestRunResult(
        total=passed + failed,
        passed=passed,
        failed=failed,
        skipped=0,
        errors=0,
        duration=0.1,
        exit_code=0 if failed == 0 else 1,
        cases=tuple(cases),
    )


def test_rerun_failed_default_is_zero():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[rerun:default] rerun_failed={op.rerun_failed}")
    assert op.rerun_failed == 0


def test_rerun_failed_negative_raises_value_error():
    with pytest.raises(ValueError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=-1)


def test_rerun_failed_bool_raises_value_error():
    # bool is an int subclass; True must not slip through as a count.
    with pytest.raises(ValueError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=True)


def test_rerun_failed_non_int_raises_value_error():
    # 2.5 would otherwise blow up later at range(self.rerun_failed).
    with pytest.raises(ValueError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=2.5)


def test_rerun_failed_zero_does_not_rerun_even_with_failures():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser([_res(["tests.test_x::test_a"], passed=2)])
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    out = op.execute(_ctx())
    print(f"[rerun:zero] calls={len(runner.calls)} out={out}")
    assert len(runner.calls) == 1            # one pytest run, no reruns
    assert "rerun_rounds" not in out         # summary unchanged for rerun_failed=0
    assert out["failed"] == 1


def test_rerun_failed_recovers_all_makes_task_succeed():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=3),
            _res([], passed=2),  # rerun: both recovered
        ]
    )
    op = PytestOperator(
        task_id="t", test_path="tests/", rerun_failed=2, runner=runner, parser=parser
    )

    out = op.execute(_ctx())  # must NOT raise -- reruns recovered everything
    print(f"[rerun:recovered] out={out}")

    assert out["success"] is True
    assert out["rerun_rounds"] == 1          # stopped early once all passed
    assert sorted(out["recovered_node_ids"]) == [
        "tests.test_x::test_a",
        "tests.test_x::test_b",
    ]
    assert out["still_failing_node_ids"] == []
    # First run on the full path; second run on the converted failed selectors.
    assert runner.calls[0]["test_path"] == "tests/"
    assert runner.calls[1]["test_path"] == [
        "tests/test_x.py::test_a",
        "tests/test_x.py::test_b",
    ]


def test_rerun_failed_partial_recovery_fails_task():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=3),
            _res(["tests.test_x::test_b"], passed=1),
            _res(["tests.test_x::test_b"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t", test_path="tests/", rerun_failed=2, runner=runner, parser=parser
    )

    with pytest.raises(TestsFailedError):
        op.execute(_ctx())
    assert len(runner.calls) == 3            # full run + 2 reruns


def test_rerun_failed_partial_recovery_summary_when_not_failing_task():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=3),
            _res(["tests.test_x::test_b"], passed=1),
            _res(["tests.test_x::test_b"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=2,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )

    out = op.execute(_ctx())
    print(f"[rerun:partial] out={out}")
    assert out["success"] is False
    assert out["rerun_rounds"] == 2
    assert out["recovered_node_ids"] == ["tests.test_x::test_a"]
    assert out["still_failing_node_ids"] == ["tests.test_x::test_b"]
    # XCom keeps the first full run's counts (honest picture of the suite).
    assert out["total"] == 5
    assert out["failed"] == 2


def test_rerun_failed_no_failures_no_reruns():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser([_res([], passed=5)])
    op = PytestOperator(
        task_id="t", test_path="tests/", rerun_failed=3, runner=runner, parser=parser
    )
    out = op.execute(_ctx())
    assert len(runner.calls) == 1
    assert "rerun_rounds" not in out
    assert out["success"] is True


def test_rerun_failed_ignored_in_dry_run():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser([_res(["tests.test_x::test_a"], passed=0)])
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=2,
        dry_run=True,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())
    print(f"[rerun:dry_run] calls={len(runner.calls)} args={runner.calls[0]['pytest_args']}")
    assert len(runner.calls) == 1            # no reruns in dry-run
    assert runner.calls[0]["pytest_args"][-1] == "--collect-only"


def test_rerun_failed_cleans_report_dir_between_rounds():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=1),
            _res(["tests.test_x::test_a"], passed=0),
            _res(["tests.test_x::test_a"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=2,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())
    print(f"[rerun:cleanup] cleanup_calls={runner.cleanup_calls}")
    # cleanup(False) before each of the 2 reruns + final cleanup(False).
    assert runner.cleanup_calls == [False, False, False]


# ---------------------------------------------------------------------------
# failed_only: crash-safe Variable lifecycle
#   - consume-on-read: delete the Variable the moment it is read (before the run)
#   - write only when a further retry will read it (failed AND not final)
#   - the terminal/success attempt writes nothing -> can never orphan a Variable
# ---------------------------------------------------------------------------


def test_failed_only_consumes_variable_on_read():
    # A retry reads the stored failures and deletes the Variable immediately --
    # before running a single test -- so a mid-run crash cannot orphan it.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=2))     # narrowed run passes
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:consume] reads={store.reads} deletes={store.deletes}")
    # Deleted on read (consumed); passing run writes nothing back.
    assert store.deletes == [key]
    assert store.writes == []
    assert key not in store.data


def test_failed_only_consume_then_rewrite_when_still_failing_non_final():
    # Non-final retry: consume the old set on read, then write the (narrowed)
    # still-failing set for the NEXT retry.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a", "tests.test_y::test_b"]})
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_y::test_b"], passed=1))  # one still fails
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    # try_number (2) <= max_tries (3) -> not final, so a rewrite is expected.
    op.execute(_ctx(try_number=2, max_tries=3, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:consume_rewrite] deletes={store.deletes} writes={store.writes}")
    assert store.deletes == [key]                     # old set consumed on read
    assert store.writes == [(key, ["tests.test_y::test_b"])]  # narrowed set saved
    assert store.data[key] == ["tests.test_y::test_b"]


def test_failed_only_writes_for_next_retry_on_failing_mid_cycle_first_attempt():
    # First attempt (empty store) fails and is not final -> write the failures
    # forward; nothing was read so nothing is consumed.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:mid_cycle] writes={store.writes} deletes={store.deletes}")
    assert store.writes == [(key, ["tests.test_x::test_a"])]
    assert store.deletes == []
    assert store.data[key] == ["tests.test_x::test_a"]


def test_failed_only_terminal_attempt_consumes_and_writes_nothing():
    # The final attempt reads+consumes whatever a prior attempt left, runs, and
    # -- crucially -- writes nothing back, so it cannot leave an orphan even if
    # it fails. (Here it fails on its narrowed targets.)
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    # Final attempt: try_number (3) > max_tries (2).
    op.execute(_ctx(try_number=3, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:terminal] writes={store.writes} deletes={store.deletes}")
    assert store.deletes == [key]      # consumed on read
    assert store.writes == []          # final attempt never writes -> no orphan
    assert key not in store.data


def test_failed_only_final_attempt_with_empty_store_does_nothing():
    # The durability property in its purest form: a final attempt that fails
    # with no prior store neither reads anything to delete nor writes anything,
    # so a crash right after it can leave nothing behind.
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    op.execute(_ctx(try_number=3, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:terminal_empty] writes={store.writes} deletes={store.deletes}")
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_writes_forward_when_no_max_tries():
    # max_tries missing -> treat as "more retries may come" -> write forward.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    op.execute(_ctx(try_number=9, dag_id="d", task_id="t", run_id="r"))  # no max_tries
    assert store.deletes == []
    assert store.data[key] == ["tests.test_x::test_a"]


def test_failed_only_no_store_when_ids_missing():
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())  # ti has no dag_id/task_id/run_id -> no derivable key
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_strategy_all_never_touches_store():
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t", test_path="tests/", runner=runner, parser=parser, store=store
    )
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_skipped_in_dry_run():
    # dry-run + failed_only is meaningless: --collect-only never runs test
    # bodies, so there is no "last failed" to narrow to. The operator touches
    # no Variable and adds no --lf; only --collect-only applies.
    store = FakeStore({_key(): ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        dry_run=True,
        runner=runner,
        parser=parser,
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    args = runner.calls[0]["pytest_args"]
    print(f"[dry_run+failed_only] args={args}")
    assert "--lf" not in args
    assert "--collect-only" in args        # dry-run itself still applies
    # Narrowing is skipped: the full suite is collected, store is untouched.
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []
