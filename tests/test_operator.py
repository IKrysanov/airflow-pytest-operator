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

    def run(
        self,
        test_path,
        *,
        pytest_args=None,
        env=None,
        env_file=None,
        env_file_overrides=False,
        report_request,
    ):
        spec = report_request("/fake/report/dir")
        self.calls.append(
            {
                "test_path": test_path,
                "pytest_args": pytest_args,
                "env": env,
                "env_file": env_file,
                "env_file_overrides": env_file_overrides,
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
        # Airflow exposes the attempt number here: 1 on the first run, 2+ on
        # retries. With max_tries it tells the operator whether this is the
        # final attempt (try_number > max_tries) -- which gates whether the
        # failed_only Variable is written forward for a next retry.
        self.try_number = try_number
        self.max_tries = max_tries
        # (dag_id, task_id, run_id) derive the failed_only Variable key. Default
        # None -> no derivable key, so tests that don't care are unaffected.
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


def test_invalid_store_raises_type_error_at_init():
    # A store missing read/write/delete must fail fast at init, not at execute().
    with pytest.raises(TypeError, match="LastFailedStore"):
        PytestOperator(task_id="t", test_path="tests/", store=42)


def test_duck_typed_store_is_accepted():
    # Structural typing: any object with the three methods satisfies the
    # protocol -- no subclassing required. Pin that the injected instance is
    # used as-is (not silently replaced by the default store).
    store = FakeStore()
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        store=store,
        runner=FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml")),
    )
    assert op._store is store


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
    )

    # Default fail_on_test_failure=True: the failing run raises (so Airflow
    # retries) AND records its failures forward for that retry to narrow to.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, dag_id="d", task_id="t", run_id="r"))

    print(
        f"[failed_only:first] test_path={runner.calls[0]['test_path']!r} "
        f"reads={store.reads} writes={store.writes}"
    )
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
    parser = FakeParser(_res([], passed=2))  # the narrowed run passes
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
    store = FakeStore()  # nothing stored
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
    assert forwarded_args == ["-k", "smoke"]  # no --lf, no -o cache_dir=...


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

    print(
        f"[failed_only:no_mutation] pytest_args={op.pytest_args!r} "
        f"test_path={op.test_path!r}"
    )
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
    # Right type, wrong value -> ValueError (Python convention).
    with pytest.raises(ValueError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=-1)


def test_rerun_failed_bool_raises_type_error():
    # bool is an int subclass; True must not slip through as a count. A wrong
    # *type* is a TypeError, not a ValueError.
    with pytest.raises(TypeError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=True)


def test_rerun_failed_non_int_raises_type_error():
    # 2.5 would otherwise blow up later at range(self.rerun_failed); reject the
    # wrong type up front with a TypeError.
    with pytest.raises(TypeError, match="rerun_failed"):
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
    assert len(runner.calls) == 1  # one pytest run, no reruns
    assert "rerun_rounds" not in out  # summary unchanged for rerun_failed=0
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
    assert out["rerun_rounds"] == 1  # stopped early once all passed
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
    assert len(runner.calls) == 3  # full run + 2 reruns


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
    print(
        f"[rerun:dry_run] calls={len(runner.calls)} args={runner.calls[0]['pytest_args']}"
    )
    assert len(runner.calls) == 1  # no reruns in dry-run
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
    parser = FakeParser(_res([], passed=2))  # narrowed run passes
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
    )
    # try_number (2) <= max_tries (3) -> not final, so a rewrite is expected.
    # Default fail_on_test_failure=True -> the failing attempt raises (Airflow
    # will retry) and hands the narrowed set forward for that retry.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=2, max_tries=3, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:consume_rewrite] deletes={store.deletes} writes={store.writes}")
    assert store.deletes == [key]  # old set consumed on read
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
    )
    # Default fail_on_test_failure=True -> the attempt raises and writes forward.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:mid_cycle] writes={store.writes} deletes={store.deletes}")
    assert store.writes == [(key, ["tests.test_x::test_a"])]
    assert store.deletes == []
    assert store.data[key] == ["tests.test_x::test_a"]


def test_failed_only_no_write_forward_when_fail_on_test_failure_false():
    # Regression: with fail_on_test_failure=False a failing run does NOT fail the
    # task, so Airflow never retries -- writing the failed set forward would
    # orphan a Variable that nothing ever consumes. The operator must skip the
    # write. (An empty store means it touches nothing at all beyond the read.)
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
    # Not final and tests failed -- but the task succeeds, so no retry will read
    # a written set. Must NOT raise and must NOT write.
    out = op.execute(
        _ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r")
    )
    print(
        f"[failed_only:no_fail_no_write] writes={store.writes} success={out['success']}"
    )
    assert out["success"] is False  # XCom still reports the failure honestly
    assert store.writes == []  # no orphan left behind
    assert key not in store.data


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
    )
    # Final attempt: try_number (3) > max_tries (2). fail_on_test_failure=True so
    # the *only* thing that suppresses the write is the final-attempt gate.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=3, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:terminal] writes={store.writes} deletes={store.deletes}")
    assert store.deletes == [key]  # consumed on read
    assert store.writes == []  # final attempt never writes -> no orphan
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
    )
    with pytest.raises(TestsFailedError):
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
    )
    # no max_tries -> undeterminable -> treated as non-final -> write forward.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=9, dag_id="d", task_id="t", run_id="r"))
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
    assert "--collect-only" in args  # dry-run itself still applies
    # Narrowing is skipped: the full suite is collected, store is untouched.
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


# ---------------------------------------------------------------------------
# Collaborator safety: an *injected* runner/store that violates the "never
# raise" contract must never mask the real outcome of execute().
# ---------------------------------------------------------------------------


class _ExplodingCleanupRunner(FakeRunner):
    """A runner whose cleanup() violates the best-effort contract."""

    def cleanup(self, *, success=True):
        self.cleanup_calls.append(success)
        raise RuntimeError("cleanup boom")


class _ExplodingStore:
    """A store that raises on every method (satisfies the protocol shape)."""

    def read(self, key):
        raise RuntimeError("read boom")

    def write(self, key, node_ids):
        raise RuntimeError("write boom")

    def delete(self, key):
        raise RuntimeError("delete boom")


class _DeleteExplodingStore(FakeStore):
    """Reads/writes normally but raises on delete (consume-on-read path)."""

    def delete(self, key):
        raise RuntimeError("delete boom")


def test_cleanup_error_does_not_mask_tests_failed_error():
    runner = _ExplodingCleanupRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(failed=2)),
    )
    # The genuine TestsFailedError wins -- not the cleanup RuntimeError.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())
    assert runner.cleanup_calls == [False]  # cleanup was attempted


def test_cleanup_error_does_not_mask_success_summary():
    runner = _ExplodingCleanupRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=2)),
    )
    out = op.execute(_ctx())  # cleanup error swallowed
    assert out["success"] is True


def test_store_errors_do_not_break_failed_only_run():
    # read/delete/write all raise; the operator degrades to the full suite and
    # the real outcome (TestsFailedError) still surfaces.
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=0)),
        store=_ExplodingStore(),
    )
    # read raises -> degrades to full suite; write raises at the end -> swallowed.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    # The full suite ran (read failure did not narrow it).
    assert runner.calls[0]["test_path"] == "tests/"


def test_store_delete_error_during_consume_does_not_break_run():
    # delete raises while consuming the Variable on read; it's swallowed, the
    # narrowed run still proceeds and the task succeeds.
    key = _key()
    store = _DeleteExplodingStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=FakeParser(_res([], passed=1)),  # the narrowed run passes
        store=store,
    )
    out = op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    assert out["success"] is True
    # It narrowed to the stored failures despite the delete error.
    assert runner.calls[0]["test_path"] == ["tests/test_x.py::test_a"]


def test_failed_only_warns_when_final_attempt_undeterminable():
    # No max_tries on the ti -> is_final_attempt can't decide -> the operator
    # writes forward AND logs a warning to the task log about a possible orphan.
    from unittest import mock

    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=0)),
        store=store,
    )
    with mock.patch.object(op.log, "warning") as warning:
        # try_number present but no max_tries -> undeterminable. Default
        # fail_on_test_failure=True -> the attempt raises and writes forward.
        with pytest.raises(TestsFailedError):
            op.execute(_ctx(try_number=9, dag_id="d", task_id="t", run_id="r"))

    logged = " ".join(str(c) for c in warning.call_args_list)
    print(f"[final_undeterminable] warnings={logged!r}")
    assert "final attempt" in logged
    # It still wrote the failures forward (treated as non-final).
    assert store.writes == [(_key(), ["tests.test_x::test_a"])]


# ---------------------------------------------------------------------------
# rerun_failed + failed_only compose: the in-process reruns run first, and only
# the tests STILL failing after them are carried forward to the next Airflow
# retry -- not the first run's larger failure set.
# ---------------------------------------------------------------------------


def test_rerun_failed_and_failed_only_write_post_rerun_set_forward():
    # First attempt (empty store) with both features on. The full run fails two
    # tests; one in-process rerun recovers one of them. The task still fails, so
    # the failed_only Variable is handed to the next Airflow retry -- and it must
    # contain ONLY the post-rerun survivor, not both original failures.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            # Full run: a and b fail.
            _res(["tests.test_x::test_a", "tests.test_y::test_b"], passed=3),
            # In-process rerun: a recovers, b still fails.
            _res(["tests.test_y::test_b"], passed=1),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=1,
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    # try_number (1) <= max_tries (2): not final, so the survivor is written
    # forward. b never recovered, so the task itself still fails.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[rerun+failed_only] calls={len(runner.calls)} writes={store.writes}")
    # Two pytest invocations: the full run, then one in-process rerun narrowed
    # to the converted failed selectors.
    assert len(runner.calls) == 2
    assert runner.calls[0]["test_path"] == "tests/"
    assert runner.calls[1]["test_path"] == [
        "tests/test_x.py::test_a",
        "tests/test_y.py::test_b",
    ]
    # The crux: the next retry inherits only the post-rerun survivor (b), so it
    # won't waste time re-running a, which the in-process rerun already fixed.
    assert store.writes == [(key, ["tests.test_y::test_b"])]
    assert store.data[key] == ["tests.test_y::test_b"]
    # First attempt: the store was read once but held nothing to consume.
    assert store.reads == [key]
    assert store.deletes == []


# ---------------------------------------------------------------------------
# env_file / env_file_overrides: operator-level params, forwarded to the runner
# (the runner owns the file reading + merge; the operator only passes them on)
# ---------------------------------------------------------------------------


def test_env_file_and_overrides_forwarded_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env={"A": "1"},  # explicit env and a file together is fine
        env_file="/cfg/test.env",
        env_file_overrides=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    call = runner.calls[0]
    print(
        f"[env_file:forward] env={call['env']} env_file={call['env_file']!r} "
        f"overrides={call['env_file_overrides']}"
    )
    # The operator forwards all three verbatim; precedence/merge is the runner's
    # job (os.environ < env_file < env), tested in test_subprocess_runner.py.
    assert call["env"] == {"A": "1"}
    assert call["env_file"] == "/cfg/test.env"
    assert call["env_file_overrides"] is True


def test_env_file_defaults_forwarded_as_none_and_false():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    call = runner.calls[0]
    assert call["env_file"] is None
    assert call["env_file_overrides"] is False


def test_env_file_is_a_template_field():
    # Templated so the path can depend on the environment/run.
    assert "env_file" in PytestOperator.template_fields


def test_env_file_forwarded_in_dry_run():
    # dry_run still runs pytest (--collect-only), so env_file must be forwarded.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env_file="/cfg/.env",
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    assert runner.calls[0]["env_file"] == "/cfg/.env"
    assert runner.calls[0]["pytest_args"][-1] == "--collect-only"


def test_env_file_forwarded_on_every_rerun():
    # rerun_failed re-invokes run(); env_file must travel to each rerun too.
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=1),
            _res(["tests.test_x::test_a"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env_file="/cfg/.env",
        rerun_failed=1,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())
    print(f"[env_file:reruns] calls={len(runner.calls)}")
    assert len(runner.calls) == 2  # full run + 1 rerun
    assert all(c["env_file"] == "/cfg/.env" for c in runner.calls)


class _RecordingCustomRunner:
    """A minimal *custom* runner accepting the new run() kwargs (interface check).

    Duck-typed (no PytestRunner base) to prove the operator only relies on the
    structural contract, and that env_file/env_file_overrides reach a custom
    runner unchanged.
    """

    def __init__(self, artifacts):
        self._artifacts = artifacts
        self.calls = []

    def run(
        self,
        test_path,
        *,
        pytest_args=None,
        env=None,
        env_file=None,
        env_file_overrides=False,
        report_request,
    ):
        report_request("/fake/dir")
        self.calls.append(
            {"env_file": env_file, "env_file_overrides": env_file_overrides}
        )
        return self._artifacts

    def cleanup(self, *, success=True):
        pass


def test_custom_runner_receives_env_file_through_operator():
    runner = _RecordingCustomRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env_file="/cfg/.env",
        env_file_overrides=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    assert runner.calls[0] == {"env_file": "/cfg/.env", "env_file_overrides": True}


# ---------------------------------------------------------------------------
# parallel / dist: drive pytest-xdist from the operator (-n / --dist)
# ---------------------------------------------------------------------------


def test_parallel_dist_default_none():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[parallel:default] parallel={op.parallel!r} dist={op.dist!r}")
    assert op.parallel is None
    assert op.dist is None


def test_parallel_int_appends_n_to_runner_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        parallel=4,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:int] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "smoke", "-n", "4"]
    assert "--dist" not in forwarded


def test_parallel_auto_keyword():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel="auto",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:auto] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "auto"]


def test_dist_appends_mode_after_n():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel=4,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[dist:mode] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "4", "--dist", "loadscope"]


def test_parallel_none_adds_nothing():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:none] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "smoke"]
    assert "-n" not in forwarded


def test_parallel_skipped_in_dry_run():
    # dry-run runs no test bodies, so workers would only add startup latency;
    # --collect-only is still appended, -n is not.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel=4,
        dist="loadscope",
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:dry_run] forwarded = {forwarded!r}")
    assert "--collect-only" in forwarded
    assert "-n" not in forwarded
    assert "--dist" not in forwarded


def test_parallel_defers_to_explicit_n_separate():
    # User already drives -n via pytest_args -> operator must not add a second
    # one, and must skip --dist too (the user owns parallelism).
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-n", "8"],
        parallel=4,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:defer_separate] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "8"]
    assert forwarded.count("-n") == 1
    assert "--dist" not in forwarded


def test_parallel_defers_to_explicit_n_equals_form():
    # The "-n=8" and "-nN" spellings are detected too, not only the split form.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-n=8"],
        parallel=4,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:defer_equals] forwarded = {forwarded!r}")
    assert forwarded == ["-n=8"]


def test_parallel_not_applied_to_in_process_reruns():
    # The full run is parallelised; the in-process rerun_failed round re-runs a
    # couple of node-ids serially (uses list(self.pytest_args), never -n).
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first full run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel=4,
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    first = runner.calls[0]["pytest_args"]
    rerun = runner.calls[1]["pytest_args"]
    print(f"[parallel:reruns] first={first!r} rerun={rerun!r}")
    assert len(runner.calls) == 2
    assert first == ["-n", "4"]  # full run parallelised
    assert "-n" not in rerun  # rerun stays serial


def test_parallel_does_not_mutate_user_pytest_args():
    # Like dry-run's --collect-only injection: the user's list is never mutated,
    # and a retry (second execute) appends exactly one -n, not two.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    user_args = ["-k", "smoke"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        parallel=2,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    op.execute(_ctx())  # retry simulation
    print(f"[parallel:no_mutation] op.pytest_args = {op.pytest_args!r}")
    assert op.pytest_args == ["-k", "smoke"]
    for call in runner.calls:
        assert call["pytest_args"].count("-n") == 1


def test_parallel_zero_raises_value_error():
    with pytest.raises(ValueError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel=0)


def test_parallel_bool_raises_type_error():
    # bool is an int subclass; True must not slip through as a worker count.
    with pytest.raises(TypeError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel=True)


def test_parallel_bad_string_raises_value_error():
    with pytest.raises(ValueError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel="lots")


def test_parallel_bad_type_raises_type_error():
    with pytest.raises(TypeError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel=2.5)


def test_dist_invalid_mode_raises_value_error():
    with pytest.raises(ValueError, match="dist"):
        PytestOperator(task_id="t", test_path="tests/", parallel=2, dist="bogus")


def test_dist_without_parallel_raises_value_error():
    # --dist is inert without -n; reject it rather than silently no-op.
    with pytest.raises(ValueError, match="dist requires parallel"):
        PytestOperator(task_id="t", test_path="tests/", dist="loadscope")


def test_dist_valid_with_parallel_is_accepted():
    op = PytestOperator(
        task_id="t", test_path="tests/", parallel="auto", dist="worksteal"
    )
    assert op.parallel == "auto"
    assert op.dist == "worksteal"


def test_dist_defers_to_explicit_dist_in_pytest_args():
    # User drives --dist via pytest_args but NOT -n: deference is keyed on -n,
    # so the operator still adds -n -- but it must NOT add a second --dist
    # (xdist's argparse keeps the last one, silently dropping the user's mode).
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--dist", "loadfile"],
        parallel=4,
        dist="load",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[dist:defer_split] forwarded = {forwarded!r}")
    assert forwarded == ["--dist", "loadfile", "-n", "4"]
    assert forwarded.count("--dist") == 1  # the user's mode survives


def test_dist_defers_to_explicit_dist_equals_form():
    # The "--dist=loadfile" spelling is detected too, not only the split form.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--dist=loadfile"],
        parallel=4,
        dist="load",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[dist:defer_equals] forwarded = {forwarded!r}")
    assert forwarded == ["--dist=loadfile", "-n", "4"]
    assert forwarded.count("--dist") == 0  # only the equals spelling present


def test_parallel_is_applied_to_failed_only_retry():
    # Unlike the in-process rerun_failed rounds (which stay serial), a
    # failed_only Airflow retry runs the narrowed set through effective_args, so
    # -n / --dist DO apply -- the narrowed set can still be large.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        parallel=4,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_res([], passed=1)),
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    forwarded = runner.calls[0]["pytest_args"]
    target = runner.calls[0]["test_path"]
    print(f"[parallel:failed_only] target={target!r} forwarded={forwarded!r}")
    assert target == ["tests/test_x.py::test_a"]  # narrowed to the prior failure
    assert forwarded == ["-n", "4", "--dist", "loadscope"]  # parallelism applied


# ---------------------------------------------------------------------------
# markers / keyword: ergonomic sugar for pytest's -m / -k selectors
# ---------------------------------------------------------------------------


def test_markers_keyword_default_none():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[sugar:default] markers={op.markers!r} keyword={op.keyword!r}")
    assert op.markers is None
    assert op.keyword is None


def test_markers_appends_dash_m():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke and not slow",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:markers] forwarded = {forwarded!r}")
    assert forwarded == ["-m", "smoke and not slow"]


def test_keyword_appends_dash_k():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        keyword="login or logout",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:keyword] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "login or logout"]


def test_markers_and_keyword_together_after_user_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        markers="smoke",
        keyword="fast",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:both] forwarded = {forwarded!r}")
    assert forwarded == ["-x", "-m", "smoke", "-k", "fast"]


def test_markers_applies_in_dry_run_alongside_collect_only():
    # Selection narrows what gets collected -- useful for a scoped pre-flight.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke",
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:dry_run] forwarded = {forwarded!r}")
    assert "-m" in forwarded and "smoke" in forwarded
    assert "--collect-only" in forwarded


def test_markers_defers_to_explicit_m_in_pytest_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-m", "regression"],
        markers="smoke",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:defer_m] forwarded = {forwarded!r}")
    assert forwarded == ["-m", "regression"]  # operator's markers not added
    assert forwarded.count("-m") == 1


def test_keyword_defers_to_concatenated_k_form():
    # The "-kfast" short concatenated spelling is detected, not only "-k fast".
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-kfast"],
        keyword="slow",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:defer_k] forwarded = {forwarded!r}")
    assert forwarded == ["-kfast"]


def test_empty_markers_is_skipped():
    # A Jinja template can render to "" at run time -> no flag added.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        markers="   ",  # whitespace-only, as a blank template would render
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:empty] forwarded = {forwarded!r}")
    assert forwarded == ["-x"]
    assert "-m" not in forwarded


def test_empty_keyword_is_skipped():
    # Symmetry with markers: a whitespace-only keyword (e.g. a template that
    # resolved to "") is skipped rather than passed as a blank -k selector.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        keyword="",  # empty, as a blank template would render
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:empty_keyword] forwarded = {forwarded!r}")
    assert forwarded == ["-x"]
    assert "-k" not in forwarded


def test_markers_not_applied_to_in_process_reruns():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke",
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    first = runner.calls[0]["pytest_args"]
    rerun = runner.calls[1]["pytest_args"]
    print(f"[sugar:reruns] first={first!r} rerun={rerun!r}")
    assert first == ["-m", "smoke"]
    assert "-m" not in rerun  # rerun targets explicit node-ids, no re-filtering


def test_markers_bad_type_raises_type_error():
    with pytest.raises(TypeError, match="markers"):
        PytestOperator(task_id="t", test_path="tests/", markers=["smoke"])


def test_keyword_bad_type_raises_type_error():
    with pytest.raises(TypeError, match="keyword"):
        PytestOperator(task_id="t", test_path="tests/", keyword=123)


def test_markers_keyword_do_not_mutate_user_pytest_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    user_args = ["-x"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        markers="smoke",
        keyword="fast",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    op.execute(_ctx())  # retry simulation
    print(f"[sugar:no_mutation] op.pytest_args = {op.pytest_args!r}")
    assert op.pytest_args == ["-x"]
    for call in runner.calls:
        assert call["pytest_args"].count("-m") == 1
        assert call["pytest_args"].count("-k") == 1


def test_has_flag_detects_all_spellings():
    # The helper now lives in operators/_constants.py; pin the spellings it
    # must recognise so the "defer to explicit user arg" logic stays correct.
    from airflow_pytest_operator.operators._constants import (
        DIST_FLAGS,
        NUMPROCESSES_FLAGS,
        has_flag,
    )

    assert has_flag(["-n", "4"], NUMPROCESSES_FLAGS)  # split
    assert has_flag(["-n=4"], NUMPROCESSES_FLAGS)  # equals
    assert has_flag(["-n4"], NUMPROCESSES_FLAGS)  # concatenated
    assert has_flag(["--numprocesses=4"], NUMPROCESSES_FLAGS)  # long equals
    assert not has_flag(["-k", "smoke"], NUMPROCESSES_FLAGS)  # unrelated flag
    assert not has_flag([], NUMPROCESSES_FLAGS)  # empty

    # --dist is long-only: the split and equals spellings match, and the
    # concatenated short-form heuristic must NOT fire for it.
    assert has_flag(["--dist", "loadscope"], DIST_FLAGS)  # split
    assert has_flag(["--dist=loadscope"], DIST_FLAGS)  # equals
    assert not has_flag(["-n", "4"], DIST_FLAGS)  # unrelated flag
    assert not has_flag(["--distribute"], DIST_FLAGS)  # no false concat match


def test_parallel_logical_keyword():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel="logical",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:logical] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "logical"]


def test_markers_and_parallel_compose_in_stable_order():
    # The two injection blocks (selectors, then xdist) must compose: user args,
    # then -m/-k, then -n/--dist.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        markers="smoke",
        keyword="fast",
        parallel=2,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[compose] forwarded = {forwarded!r}")
    assert forwarded == [
        "-x",
        "-m",
        "smoke",
        "-k",
        "fast",
        "-n",
        "2",
        "--dist",
        "loadscope",
    ]


def test_new_params_are_templated():
    # markers/keyword must be Jinja-rendered before execute(), like pytest_args.
    print(f"[template_fields] {PytestOperator.template_fields}")
    assert "markers" in PytestOperator.template_fields
    assert "keyword" in PytestOperator.template_fields
