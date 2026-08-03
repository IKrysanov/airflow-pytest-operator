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


"""min_pass_rate / max_failed: the failure-tolerance threshold that replaces the
binary fail_on_test_failure policy. Covers the verdict itself, its precedence,
the fail-closed paths, and how it composes with dry_run / reruns / failed_only /
the coverage gate. Shared fakes in _op_helpers."""

from __future__ import annotations

from unittest import mock

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    FakeStore,
    SequenceParser,
    _ctx,
    _key,
    _res,
    _result,
)

from airflow_pytest_operator.exceptions import (
    CoverageThresholdError,
    FailureThresholdError,
    TestsFailedError,
)
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def _rendered(log_mock):
    """The log lines as a reader sees them -- lazy %-args interpolated.

    ``call_args_list`` keeps the format string and its arguments apart, so
    asserting on the raw calls would miss anything that is substituted in.
    """
    out = []
    for call in log_mock.call_args_list:
        fmt, *args = call.args
        out.append(fmt % tuple(args) if args else fmt)
    return " ".join(out)


def _op(parser, *, runner=None, **kwargs):
    return PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner or FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml")),
        parser=parser,
        **kwargs,
    )


# -- defaults: the feature is off and changes nothing -------------------------


def test_thresholds_default_none():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[thr:default] min={op.min_pass_rate!r} max={op.max_failed!r}")
    assert op.min_pass_rate is None
    assert op.max_failed is None


def test_no_threshold_keeps_binary_policy_and_xcom_shape():
    # Without a threshold nothing changes: a red suite still raises
    # TestsFailedError and the summary gains no new keys.
    op = _op(FakeParser(_result(passed=9, failed=1)))
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())

    green = _op(FakeParser(_result(passed=10)))
    out = green.execute(_ctx())
    print(f"[thr:off] keys = {sorted(out)}")
    assert "pass_rate" not in out
    assert "threshold_passed" not in out


# -- min_pass_rate ------------------------------------------------------------


def test_min_pass_rate_tolerates_a_few_failures():
    # The issue's motivating case: 2 failing checks out of 500 are acceptable.
    op = _op(FakeParser(_result(passed=498, failed=2)), min_pass_rate=0.95)
    out = op.execute(_ctx())
    print(f"[thr:tolerated] pass_rate={out['pass_rate']!r} success={out['success']}")
    assert out["pass_rate"] == 498 / 500
    assert out["threshold_passed"] is True
    # The suite WAS red -- the summary keeps saying so; only the task is green.
    assert out["success"] is False
    assert out["failed"] == 2


def test_min_pass_rate_fails_below_threshold():
    # 50 of 500 failing is not acceptable -- the task fails, with the numbers.
    op = _op(FakeParser(_result(passed=450, failed=50)), min_pass_rate=0.95)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:below] {exc.value}")
    assert exc.value.pass_rate == 0.9
    assert exc.value.failures == 50
    assert exc.value.min_pass_rate == 0.95
    assert exc.value.max_failed is None
    assert "90.00%" in str(exc.value) and "95.00%" in str(exc.value)


def test_min_pass_rate_boundary_is_inclusive():
    # Exactly at the threshold passes (>=, not >) -- mirrors cov_fail_under.
    op = _op(FakeParser(_result(passed=95, failed=5)), min_pass_rate=0.95)
    out = op.execute(_ctx())
    print(f"[thr:boundary] pass_rate={out['pass_rate']!r}")
    assert out["pass_rate"] == 0.95
    assert out["threshold_passed"] is True


def test_min_pass_rate_counts_errors_as_not_passed():
    # Collection/setup errors are failures for the rate: 90 passed of 100
    # executed (5 failed + 5 errored) is 90%, below a 95% gate.
    op = _op(FakeParser(_result(passed=90, failed=5, errors=5)), min_pass_rate=0.95)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:errors] {exc.value}")
    assert exc.value.pass_rate == 0.9
    assert exc.value.failures == 10


def test_min_pass_rate_excludes_skipped_from_denominator():
    # The documented decision: passed / (total - skipped). 90 passed + 10 failed
    # + 400 skipped is a 90% pass rate, NOT 18% -- tests that never ran must not
    # dilute the rate.
    op = _op(
        FakeParser(_result(passed=90, failed=10, skipped=400)),
        min_pass_rate=0.9,
    )
    out = op.execute(_ctx())
    print(f"[thr:skipped] pass_rate={out['pass_rate']!r} total={out['total']}")
    assert out["total"] == 500
    assert out["pass_rate"] == 0.9
    assert out["threshold_passed"] is True


def test_min_pass_rate_zero_tolerates_everything():
    # The degenerate "never fail on rate" setting: a fully red suite still passes
    # the gate (0.0 >= 0.0) -- but the summary keeps success=False.
    op = _op(FakeParser(_result(passed=0, failed=10)), min_pass_rate=0.0)
    out = op.execute(_ctx())
    print(f"[thr:zero] pass_rate={out['pass_rate']!r}")
    assert out["pass_rate"] == 0.0
    assert out["threshold_passed"] is True
    assert out["success"] is False


def test_min_pass_rate_one_requires_a_green_suite():
    op = _op(FakeParser(_result(passed=99, failed=1)), min_pass_rate=1.0)
    with pytest.raises(FailureThresholdError):
        op.execute(_ctx())

    green = _op(FakeParser(_result(passed=100)), min_pass_rate=1.0)
    out = green.execute(_ctx())
    print(f"[thr:one] pass_rate={out['pass_rate']!r}")
    assert out["pass_rate"] == 1.0
    assert out["threshold_passed"] is True


# -- max_failed ---------------------------------------------------------------


def test_max_failed_within_cap_passes():
    op = _op(FakeParser(_result(passed=8, failed=2)), max_failed=3)
    out = op.execute(_ctx())
    print(f"[thr:cap_ok] failed={out['failed']} pass_rate={out['pass_rate']!r}")
    assert out["threshold_passed"] is True
    assert out["pass_rate"] == 0.8  # surfaced even with only max_failed set


def test_max_failed_boundary_is_inclusive():
    # failed + errors == max_failed passes; one more fails.
    ok = _op(FakeParser(_result(passed=7, failed=2, errors=1)), max_failed=3)
    assert ok.execute(_ctx())["threshold_passed"] is True

    over = _op(FakeParser(_result(passed=6, failed=3, errors=1)), max_failed=3)
    with pytest.raises(FailureThresholdError) as exc:
        over.execute(_ctx())
    print(f"[thr:cap_boundary] {exc.value}")
    assert exc.value.failures == 4
    assert exc.value.max_failed == 3
    assert "exceed max_failed=3" in str(exc.value)


def test_max_failed_zero_is_the_binary_policy():
    # max_failed=0 tolerates nothing -- the same verdict as fail_on_test_failure,
    # just with the threshold's error type and the pass_rate key.
    op = _op(FakeParser(_result(passed=99, failed=1)), max_failed=0)
    with pytest.raises(FailureThresholdError):
        op.execute(_ctx())


def test_max_failed_survives_a_huge_skipped_suite():
    # An absolute cap does not depend on the denominator: 400 skipped tests
    # change nothing about "at most 3 may fail".
    op = _op(FakeParser(_result(passed=96, failed=4, skipped=400)), max_failed=3)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:cap_skipped] {exc.value}")
    assert exc.value.failures == 4


# -- both together: every check must hold -------------------------------------


def test_both_thresholds_must_hold():
    # Rate is fine (99%), but the absolute cap is not (10 > 5): still a failure.
    op = _op(
        FakeParser(_result(passed=990, failed=10)),
        min_pass_rate=0.95,
        max_failed=5,
    )
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:both_cap] {exc.value}")
    assert exc.value.pass_rate == 0.99
    assert len(exc.value.reasons) == 1
    assert "max_failed=5" in str(exc.value)


def test_both_thresholds_report_every_breached_reason():
    # One task log should show every reason, not just the first.
    op = _op(
        FakeParser(_result(passed=50, failed=50)),
        min_pass_rate=0.95,
        max_failed=5,
    )
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:both_reasons] reasons={exc.value.reasons}")
    assert len(exc.value.reasons) == 2
    assert "min_pass_rate" in str(exc.value)
    assert "max_failed" in str(exc.value)


def test_both_thresholds_pass_together():
    op = _op(
        FakeParser(_result(passed=98, failed=2)),
        min_pass_rate=0.95,
        max_failed=5,
    )
    out = op.execute(_ctx())
    print(f"[thr:both_ok] pass_rate={out['pass_rate']!r}")
    assert out["threshold_passed"] is True


# -- fail-closed paths --------------------------------------------------------


def test_min_pass_rate_fails_closed_when_nothing_executed():
    # Every test skipped -> the rate is undefined. A gate that cannot be
    # evaluated must not silently pass (same rule as cov_fail_under): a config
    # error that skips all 500 data-quality checks is exactly the case where a
    # green task would be dangerous.
    op = _op(FakeParser(_result(passed=0, skipped=500)), min_pass_rate=0.95)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:fail_closed] {exc.value}")
    assert exc.value.pass_rate is None
    assert "undefined" in str(exc.value)
    assert "fail-closed" in str(exc.value)


def test_max_failed_alone_passes_when_nothing_executed():
    # An absolute cap IS evaluable with no executed tests (0 failures <= cap),
    # so it does not fail closed -- only the rate gate does. pass_rate is None.
    op = _op(FakeParser(_result(passed=0, skipped=500)), max_failed=3)
    out = op.execute(_ctx())
    print(f"[thr:cap_nothing_ran] pass_rate={out['pass_rate']!r}")
    assert out["pass_rate"] is None
    assert out["threshold_passed"] is True


def test_min_pass_rate_zero_still_fails_closed_when_unevaluable():
    # Even the most permissive rate cannot be checked against an undefined rate.
    op = _op(FakeParser(_result(passed=0, skipped=5)), min_pass_rate=0.0)
    with pytest.raises(FailureThresholdError):
        op.execute(_ctx())


@pytest.mark.parametrize("exit_code", [2, 3, 4, 5])
def test_abnormal_pytest_exit_fails_closed(exit_code):
    # THE dangerous case: pytest crashed / was interrupted / was misconfigured
    # after only a handful of tests. The recorded tally looks perfect (10 of 10
    # passed), so a naive tolerance check would turn a broken run GREEN. Exit
    # codes outside 0/1 are not a complete tally, so the tolerance refuses them.
    op = _op(FakeParser(_result(passed=10, exit_code=exit_code)), min_pass_rate=0.95)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:exit_{exit_code}] {exc.value}")
    assert f"exited with code {exit_code}" in str(exc.value)
    assert exc.value.pass_rate == 1.0  # the tally itself looked fine


def test_abnormal_exit_also_fails_closed_for_max_failed_only():
    op = _op(FakeParser(_result(passed=10, exit_code=3)), max_failed=5)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:exit_cap] {exc.value}")
    assert "exited with code 3" in str(exc.value)


# -- --maxfail / -x belong to the caller --------------------------------------


@pytest.mark.parametrize(
    "args,result,gate",
    [
        # A run that hit its limit is judged on the counters it produced, like
        # any other: 5 failures against a cap of 10 is within tolerance.
        (
            ["--maxfail=5"],
            _result(passed=400, failed=5, exit_code=1),
            {"max_failed": 10},
        ),
        (["-x"], _result(passed=400, failed=1, exit_code=1), {"max_failed": 10}),
        (
            ["--maxfail=1"],
            _result(passed=99, failed=1, exit_code=1),
            {"min_pass_rate": 0.45},
        ),
        (
            ["--maxfail=100"],
            _result(passed=90, failed=10, exit_code=1),
            {"max_failed": 10},
        ),
        (["--maxfail=0"], _result(passed=100), {"max_failed": 10}),
    ],
)
def test_maxfail_is_the_callers_business(args, result, gate):
    # The operator neither refuses a pairing nor reinterprets what --maxfail did
    # to the run: choosing how early to stop is an engineering decision, and the
    # tolerance is applied to whatever counters came back.
    op = _op(FakeParser(result), pytest_args=list(args), **gate)
    assert op.execute(_ctx())["threshold_passed"] is True


def test_exit_code_one_is_a_normal_tally():
    # Exit 1 just means "some tests failed" -- exactly what a tolerance is for.
    op = _op(FakeParser(_result(passed=99, failed=1, exit_code=1)), min_pass_rate=0.95)
    out = op.execute(_ctx())
    print(f"[thr:exit_1] threshold_passed={out['threshold_passed']}")
    assert out["threshold_passed"] is True


def test_exit_one_with_no_recorded_failure_fails_closed():
    # pytest exits 1 with a GREEN report when something other than a test failed
    # the run -- coverage.py's own fail_under is the everyday case. Tolerating
    # that would silently swallow a gate the user configured elsewhere, and
    # fail_on_test_failure=True fails it today.
    op = _op(FakeParser(_result(passed=100, exit_code=1)), min_pass_rate=0.95)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:exit_1_unexplained] {exc.value}")
    assert exc.value.pass_rate == 1.0  # the tally itself was perfect
    assert "records no failure" in str(exc.value)


def test_coverage_fail_under_exit_is_not_swallowed_end_to_end(tmp_path):
    # The same case through real pytest + real pytest-cov: a green suite under a
    # coverage.py fail_under that is not met exits 1, and the tolerance must not
    # turn that into a green task.
    pytest.importorskip("pytest_cov")
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    (tmp_path / "mod.py").write_text(
        "def add(a, b):\n    return a + b\n\n"
        "def unused(x):\n    if x > 0:\n        return 'p'\n    return 'n'\n"
    )
    (tmp_path / "test_mod.py").write_text(
        "from mod import add\ndef test_add():\n    assert add(1, 2) == 3\n"
    )
    (tmp_path / ".coveragerc").write_text(
        "[run]\nsource = mod\n[report]\nfail_under = 90\n"
    )

    op = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_mod.py"),
        pytest_args=["--cov=mod", "--cov-report=term"],
        min_pass_rate=0.5,  # the suite is 100% green -- only the exit code is not
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:real_covfail] {exc.value}")
    assert exc.value.pass_rate == 1.0
    assert "records no failure" in str(exc.value)


# -- precedence over fail_on_test_failure -------------------------------------


def test_threshold_overrides_fail_on_test_failure_false():
    # fail_on_test_failure=False says "never fail"; the threshold says "fail when
    # too red". The threshold wins -- it is the explicit policy.
    op = _op(
        FakeParser(_result(passed=50, failed=50)),
        min_pass_rate=0.95,
        fail_on_test_failure=False,
    )
    with pytest.raises(FailureThresholdError):
        op.execute(_ctx())


def test_threshold_overrides_fail_on_test_failure_true():
    # And the other direction: the default True no longer fails a tolerated run.
    op = _op(
        FakeParser(_result(passed=99, failed=1)),
        min_pass_rate=0.95,
        fail_on_test_failure=True,
    )
    out = op.execute(_ctx())
    assert out["threshold_passed"] is True


def test_a_tolerated_red_suite_says_so_in_the_log():
    # The reported confusion: "Failure tolerance: ... -> within tolerance" and
    # then a SUCCESS task, with fail_on_test_failure=True still set. The log has
    # to name what happened and which flag stopped deciding.
    op = _op(FakeParser(_result(passed=94, failed=6)), min_pass_rate=0.90)
    with mock.patch.object(op.log, "warning") as warning:
        out = op.execute(_ctx())
    logged = _rendered(warning)
    print(f"[thr:tolerated_log] {logged!r}")
    assert "6 test(s) failed" in logged
    assert "will SUCCEED" in logged
    assert "supersede fail_on_test_failure=True" in logged
    assert out["success"] is False  # the summary still tells the truth


def test_a_green_run_does_not_warn_about_tolerated_failures():
    # No failures, nothing surprising -- the warning must not fire every run.
    op = _op(FakeParser(_result(passed=100)), min_pass_rate=0.90)
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    assert "will SUCCEED" not in _rendered(warning)


def test_the_failed_only_warning_quotes_no_invented_numbers():
    # It used to print a worked example ("45 of 50 ... 90% ... 99%") as if those
    # were this run's numbers. Only the real count may appear.
    key = _key()
    ids = [f"tests.t::test_{i}" for i in range(7)]
    store = FakeStore({key: ids})
    op = _op(
        FakeParser(_res([], passed=7)),
        test_retry_strategy="failed_only",
        min_pass_rate=0.90,
        store=store,
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    logged = _rendered(warning)
    print(f"[thr:no_invented_numbers] {logged!r}")
    assert "these 7 re-run test(s)" in logged
    for invented in ("45 of 50", "90%", "99%"):
        assert invented not in logged


def test_contradictory_fail_on_test_failure_is_warned_about():
    op = _op(
        FakeParser(_result(passed=100)),
        min_pass_rate=0.95,
        fail_on_test_failure=False,
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    logged = " ".join(str(c) for c in warning.call_args_list)
    print(f"[thr:contradiction_warn] {logged!r}")
    assert "fail_on_test_failure=False" in logged
    assert "superseded" in logged


def test_threshold_raises_failure_threshold_error_not_tests_failed():
    # The exception type is part of the contract (a DAG may catch it), and
    # TestsFailedError must not also fire.
    op = _op(FakeParser(_result(passed=1, failed=9)), max_failed=0)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    assert not isinstance(exc.value, TestsFailedError)


# -- dry_run ------------------------------------------------------------------


def test_threshold_inert_in_dry_run():
    # --collect-only runs no test body, so there is no pass rate to gate on. The
    # threshold adds no keys and cannot fail the task.
    op = _op(FakeParser(_result(passed=0, total=0)), min_pass_rate=0.95, dry_run=True)
    out = op.execute(_ctx())  # must NOT raise (a 0/0 rate would fail closed)
    print(f"[thr:dry_run] keys={sorted(out)}")
    assert "pass_rate" not in out
    assert "threshold_passed" not in out


def test_dry_run_warns_that_the_threshold_is_ignored():
    op = _op(FakeParser(_result(passed=0, total=0)), max_failed=2, dry_run=True)
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    logged = " ".join(str(c) for c in warning.call_args_list)
    print(f"[thr:dry_run_warn] {logged!r}")
    assert "ignored in dry_run" in logged


def test_dry_run_collection_error_still_fails_the_task():
    # The threshold is inert, so the binary policy still applies to a failed
    # collection -- a pre-flight task must not go green on a broken tree.
    runner = FakeRunner(RunArtifacts(exit_code=2, report_path="/x.xml"))
    op = _op(
        FakeParser(_result(passed=0, errors=1, exit_code=2)),
        runner=runner,
        min_pass_rate=0.95,
        dry_run=True,
    )
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())


# -- composition with rerun_failed -------------------------------------------


def test_threshold_judges_the_first_full_run_not_the_reruns():
    # The rerun rounds re-run only the failures, so their counters describe a
    # narrow subset (1 test, 100% passed) and cannot be read as the suite's.
    # The verdict comes from the first full run -- like the coverage gate.
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=1),  # first run: 1 of 2 passed
            _res([], passed=1),  # rerun: recovered
        ]
    )
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = _op(parser, runner=runner, min_pass_rate=0.95, rerun_failed=1)
    with pytest.raises(FailureThresholdError) as exc:
        op.execute(_ctx())
    print(f"[thr:reruns_first_run] {exc.value}")
    assert exc.value.pass_rate == 0.5  # the first run's rate, not the rerun's


def test_threshold_and_rerun_keys_coexist():
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=99),  # 99 of 100 passed
            _res([], passed=1),  # rerun recovers it
        ]
    )
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = _op(parser, runner=runner, min_pass_rate=0.95, rerun_failed=1)
    out = op.execute(_ctx())
    print(
        f"[thr:reruns_ok] pass_rate={out['pass_rate']!r} rounds={out['rerun_rounds']}"
    )
    assert out["pass_rate"] == 0.99  # first run's numbers
    assert out["threshold_passed"] is True
    assert out["rerun_rounds"] == 1
    assert out["success"] is True  # reruns recovered the suite


# -- composition with failed_only --------------------------------------------


def test_failed_only_variable_written_when_the_threshold_breaches():
    # The integration the issue asks for: the still-failing set is handed to the
    # next Airflow attempt exactly when THIS attempt fails the task.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = _op(
        FakeParser(_res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=2)),
        runner=runner,
        test_retry_strategy="failed_only",
        min_pass_rate=0.95,
        store=store,
    )
    with pytest.raises(FailureThresholdError):
        op.execute(_ctx(try_number=1, dag_id="d", task_id="t", run_id="r", max_tries=2))
    print(f"[thr:failed_only_write] writes={store.writes}")
    assert store.writes == [(key, ["tests.test_x::test_a", "tests.test_x::test_b"])]


def test_failed_only_variable_not_written_when_within_tolerance():
    # A tolerated red run does NOT fail the task, so no retry will read the
    # Variable -- writing it would orphan it (the bug the old
    # `fail_on_test_failure` condition guarded against).
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = _op(
        FakeParser(_res(["tests.test_x::test_a"], passed=99)),
        runner=runner,
        test_retry_strategy="failed_only",
        min_pass_rate=0.95,
        store=store,
    )
    out = op.execute(
        _ctx(try_number=1, dag_id="d", task_id="t", run_id="r", max_tries=2)
    )
    print(
        f"[thr:failed_only_skip] writes={store.writes} passed={out['threshold_passed']}"
    )
    assert store.writes == []


def test_min_pass_rate_on_a_failed_only_retry_sees_only_the_narrowed_set():
    # A trap worth pinning: the retry re-runs exactly the previous attempt's
    # failures, so the rate is measured over a set biased towards failing. Here
    # 45 of 50 recover -- the suite is at 495/500 = 99% -- but the gate sees 90%
    # and fails again. Only node-ids cross attempts, so the operator cannot
    # restore the suite's totals; it warns instead. max_failed does not have
    # this problem: a cap on the still-failing count composes fine.
    key = _key()
    ids = [f"tests.t::test_{i}" for i in range(50)]
    store = FakeStore({key: ids})
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = _op(
        FakeParser(_res(ids[:5], passed=45)),
        runner=runner,
        test_retry_strategy="failed_only",
        min_pass_rate=0.95,
        store=store,
    )
    with mock.patch.object(op.log, "warning") as warning:
        with pytest.raises(FailureThresholdError) as exc:
            op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    logged = " ".join(str(c) for c in warning.call_args_list)
    print(
        f"[thr:failed_only_rate] {exc.value} | warned={'NOT the whole suite' in logged}"
    )
    assert exc.value.pass_rate == 0.9  # 45 of the 50 re-run, not 495 of 500
    assert "NOT the whole suite" in logged
    assert "max_failed" in logged


def test_max_failed_composes_with_a_failed_only_retry():
    # The same retry under an absolute cap: 5 still failing is within a cap of
    # 10, so the task recovers -- and no warning is emitted.
    key = _key()
    ids = [f"tests.t::test_{i}" for i in range(50)]
    store = FakeStore({key: ids})
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = _op(
        FakeParser(_res(ids[:5], passed=45)),
        runner=runner,
        test_retry_strategy="failed_only",
        max_failed=10,
        store=store,
    )
    with mock.patch.object(op.log, "warning") as warning:
        out = op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    logged = " ".join(str(c) for c in warning.call_args_list)
    print(f"[thr:failed_only_cap] threshold_passed={out['threshold_passed']}")
    assert out["threshold_passed"] is True
    assert "NOT the whole suite" not in logged


def test_failed_only_still_narrows_the_targets_under_a_threshold():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    op = _op(
        FakeParser(_res([], passed=1)),
        test_retry_strategy="failed_only",
        min_pass_rate=0.95,
        store=store,
    )
    out = op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    target = op._runner.calls[0]["test_path"]
    print(f"[thr:failed_only_narrow] target={target!r}")
    assert target == ["tests/test_x.py::test_a"]
    assert out["threshold_passed"] is True


# -- composition with the coverage gate ---------------------------------------


def test_threshold_breach_precedes_the_coverage_gate():
    # Mirrors "test failures take precedence over cov_fail_under": the tolerance
    # verdict is the test-outcome verdict, so it reports first.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout="TOTAL  20  3  85%\n")
    )
    op = _op(
        FakeParser(_result(passed=50, failed=50)),
        runner=runner,
        min_pass_rate=0.95,
        cov_fail_under=0.99,
    )
    with pytest.raises(FailureThresholdError):
        op.execute(_ctx())


def test_coverage_gate_still_applies_when_the_threshold_passes():
    # A tolerated red suite no longer raises, so the coverage gate becomes the
    # active gate -- exactly as it does under fail_on_test_failure=False.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout="TOTAL  20  3  85%\n")
    )
    op = _op(
        FakeParser(_result(passed=99, failed=1)),
        runner=runner,
        min_pass_rate=0.95,
        cov_fail_under=0.90,
    )
    with pytest.raises(CoverageThresholdError):
        op.execute(_ctx())


def test_threshold_and_coverage_keys_coexist():
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout="TOTAL  20  3  85%\n")
    )
    op = _op(
        FakeParser(_result(passed=99, failed=1)),
        runner=runner,
        min_pass_rate=0.95,
        cov_fail_under=0.80,
    )
    out = op.execute(_ctx())
    print(f"[thr:with_coverage] keys={sorted(out)}")
    assert out["pass_rate"] == 0.99
    assert out["threshold_passed"] is True
    assert out["coverage"] == 0.85
    assert out["coverage_passed"] is True


def test_all_optional_summary_keys_are_declared():
    # Contract guard covering the maximal run: coverage + coverage gate +
    # tolerance + reruns must emit only keys declared in RunSummary.
    from airflow_pytest_operator import RunSummary

    declared = set(RunSummary.__required_keys__) | set(RunSummary.__optional_keys__)
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout="TOTAL  20  3  85%\n")
    )
    parser = SequenceParser(
        [_res(["tests.test_x::test_a"], passed=99), _res([], passed=1)]
    )
    op = _op(
        parser,
        runner=runner,
        min_pass_rate=0.95,
        max_failed=5,
        cov_fail_under=0.80,
        rerun_failed=1,
    )
    out = op.execute(_ctx())
    print(f"[thr:contract] keys={sorted(out)}")
    assert set(out) <= declared, set(out) - declared
    assert {"pass_rate", "threshold_passed"} <= set(out)


# -- logging ------------------------------------------------------------------


def test_verdict_is_logged_with_the_numbers():
    op = _op(FakeParser(_result(passed=98, failed=2)), min_pass_rate=0.95, max_failed=5)
    with mock.patch.object(op.log, "info") as info:
        op.execute(_ctx())
    logged = " ".join(str(c) for c in info.call_args_list)
    print(f"[thr:log] {logged[-220:]!r}")
    assert "Failure tolerance" in logged
    assert "within tolerance" in logged


# -- validation ---------------------------------------------------------------


def test_min_pass_rate_percentage_style_value_rejected():
    # The "I meant 95%" footgun, same hint as cov_fail_under.
    with pytest.raises(ValueError, match="0.95 for 95"):
        PytestOperator(task_id="t", test_path="tests/", min_pass_rate=95)


def test_min_pass_rate_negative_rejected():
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        PytestOperator(task_id="t", test_path="tests/", min_pass_rate=-0.1)


def test_min_pass_rate_bool_rejected():
    with pytest.raises(TypeError, match="min_pass_rate"):
        PytestOperator(task_id="t", test_path="tests/", min_pass_rate=True)  # type: ignore[arg-type]


def test_min_pass_rate_string_rejected():
    with pytest.raises(TypeError, match="min_pass_rate"):
        PytestOperator(task_id="t", test_path="tests/", min_pass_rate="0.95")  # type: ignore[arg-type]


def test_min_pass_rate_nan_rejected():
    # A NaN threshold compares False against every rate, silently disabling the
    # gate -- reject it at construction rather than shipping a dead gate.
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        PytestOperator(task_id="t", test_path="tests/", min_pass_rate=float("nan"))


def test_min_pass_rate_int_normalized_to_float():
    op = PytestOperator(task_id="t", test_path="tests/", min_pass_rate=1)
    print(f"[thr:normalize] min_pass_rate={op.min_pass_rate!r}")
    assert op.min_pass_rate == 1.0
    assert isinstance(op.min_pass_rate, float)


def test_max_failed_negative_rejected():
    with pytest.raises(ValueError, match="non-negative"):
        PytestOperator(task_id="t", test_path="tests/", max_failed=-1)


def test_max_failed_bool_rejected():
    with pytest.raises(TypeError, match="max_failed"):
        PytestOperator(task_id="t", test_path="tests/", max_failed=True)  # type: ignore[arg-type]


def test_max_failed_float_rejected():
    # A count, not a fraction: 0.5 is a misuse of min_pass_rate.
    with pytest.raises(TypeError, match="max_failed"):
        PytestOperator(task_id="t", test_path="tests/", max_failed=0.5)  # type: ignore[arg-type]


def test_a_cap_that_can_never_apply_is_rejected_at_construction():
    # min_pass_rate=1.0 already forbids every failure, so "tolerate 5" would be
    # silently downgraded to "tolerate none". Caught in the DAG, not in prod.
    with pytest.raises(ValueError, match="can never apply"):
        PytestOperator(task_id="t", test_path="tests/", min_pass_rate=1.0, max_failed=5)


def test_the_all_must_pass_pair_is_allowed():
    # 1.0 + a cap of 0 say the same thing; restating it surprises nobody.
    op = PytestOperator(
        task_id="t", test_path="tests/", min_pass_rate=1.0, max_failed=0
    )
    assert (op.min_pass_rate, op.max_failed) == (1.0, 0)


def test_ordinary_pairs_are_untouched():
    # The common shape from the README: a rate plus an absolute safety net.
    op = PytestOperator(
        task_id="t", test_path="tests/", min_pass_rate=0.95, max_failed=10
    )
    assert (op.min_pass_rate, op.max_failed) == (0.95, 10)


# -- end to end through a real pytest subprocess ------------------------------


def test_threshold_end_to_end_with_real_pytest(tmp_path):
    # Everything above feeds canned results; this drives the whole path -- real
    # pytest, real JUnit XML, real parse -- so the rate is computed from numbers
    # pytest actually produced. 3 of 4 pass (one skips, so 3 of 3 executed).
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    (tmp_path / "test_gate.py").write_text(
        "import pytest\n"
        "def test_a(): assert True\n"
        "def test_b(): assert True\n"
        "def test_c(): assert False\n"
        "@pytest.mark.skip(reason='n/a here')\n"
        "def test_d(): assert False\n"
    )

    tolerant = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_gate.py"),
        min_pass_rate=0.6,
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    out = tolerant.execute(_ctx())
    print(
        f"[thr:real_e2e] summary={ {k: out[k] for k in ('total', 'passed', 'failed', 'skipped', 'pass_rate')} }"
    )
    assert out["total"] == 4 and out["skipped"] == 1
    assert out["pass_rate"] == 2 / 3  # 2 passed of 3 executed
    assert out["threshold_passed"] is True
    assert out["success"] is False  # the suite really was red

    strict = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_gate.py"),
        min_pass_rate=0.9,
        max_failed=0,
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    with pytest.raises(FailureThresholdError) as exc:
        strict.execute(_ctx())
    print(f"[thr:real_e2e] strict -> {exc.value}")
    assert exc.value.pass_rate == 2 / 3
    assert exc.value.failures == 1


def test_threshold_end_to_end_with_the_json_parser(tmp_path):
    # The other parser path, and the one that matters most here: JSONResultParser
    # reads total/passed/failed/skipped straight out of the report's own summary
    # block rather than deriving them from the cases. The gate must land on the
    # same numbers there as it does with JUnit.
    pytest.importorskip("pytest_jsonreport")
    from airflow_pytest_operator.reporters import JSONResultParser
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    (tmp_path / "test_dq.py").write_text(
        "import pytest\n"
        "@pytest.mark.parametrize('i', range(100))\n"
        "def test_check(i):\n"
        "    if i % 25 == 0:\n"
        "        pytest.skip('n/a')\n"
        "    assert i % 20 != 1\n"
    )
    # 100 cases: 4 skipped (i % 25 == 0), 96 executed, 5 failing (i % 20 == 1)
    # -> 91 of 96 executed passed = 94.79%.
    op = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_dq.py"),
        min_pass_rate=0.90,
        parser=JSONResultParser(),
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    out = op.execute(_ctx())
    print(
        f"[thr:json_e2e] { ({k: out[k] for k in ('total', 'passed', 'failed', 'skipped', 'pass_rate')}) }"
    )
    assert (out["total"], out["passed"], out["failed"], out["skipped"]) == (
        100,
        91,
        5,
        4,
    )
    assert out["pass_rate"] == 91 / 96
    assert out["threshold_passed"] is True

    strict = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_dq.py"),
        min_pass_rate=0.95,  # just above the real 94.79%
        parser=JSONResultParser(),
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    with pytest.raises(FailureThresholdError) as exc:
        strict.execute(_ctx())
    print(f"[thr:json_e2e] strict -> {exc.value}")
    assert exc.value.pass_rate == 91 / 96


def test_nothing_collected_end_to_end_fails_closed(tmp_path):
    # The realistic silent-bypass: a wrong path or a `-k` that matches nothing.
    # Real pytest exits 5 and DOES write a report -- an empty, perfectly clean
    # one (0 total, 0 failed). A tolerance that only counted failures would call
    # that a pass and turn a suite that never ran GREEN. Both parameters must
    # refuse it, so this is checked for each independently.
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    (tmp_path / "test_empty.py").write_text("def helper():\n    return 1\n")

    for kwargs in ({"min_pass_rate": 0.5}, {"max_failed": 5}):
        op = PytestOperator(
            task_id="t",
            test_path=str(tmp_path / "test_empty.py"),
            runner=SubprocessPytestRunner(cwd=str(tmp_path)),
            **kwargs,
        )
        with pytest.raises(FailureThresholdError) as exc:
            op.execute(_ctx())
        print(f"[thr:real_empty] {kwargs} -> {exc.value}")
        assert "exited with code 5" in str(exc.value)
