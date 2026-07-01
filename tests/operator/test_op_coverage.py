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


"""coverage: splice pytest-cov flags into the first full run, defer to explicit
user --cov / --no-cov, and stay out of dry_run and in-process reruns. Shared
fakes in _op_helpers."""

from __future__ import annotations

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
    TestExecutionError,
    TestsFailedError,
)
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_coverage_default_false():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[coverage:default] coverage={op.coverage!r}")
    assert op.coverage is False


def test_coverage_false_adds_nothing():
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
    print(f"[coverage:off] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "smoke"]
    assert "--cov" not in forwarded
    assert not any(a.startswith("--cov") for a in forwarded)


def test_coverage_true_splices_cov_and_term_missing():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:on] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "smoke", "--cov", "--cov-report=term-missing"]


def test_coverage_skipped_in_dry_run():
    # dry-run runs no test bodies, so coverage measurement is meaningless;
    # --collect-only is still appended, --cov is not.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:dry_run] forwarded = {forwarded!r}")
    assert "--collect-only" in forwarded
    assert "--cov" not in forwarded
    assert not any(a.startswith("--cov") for a in forwarded)


def test_coverage_defers_to_explicit_cov_in_pytest_args():
    # User already drives --cov with their own source -> operator must not add
    # a second --cov / --cov-report so their explicit measurement is preserved.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--cov=mypkg", "--cov-report=html"],
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:defer_cov_equals] forwarded = {forwarded!r}")
    assert forwarded == ["--cov=mypkg", "--cov-report=html"]
    assert forwarded.count("--cov") == 0  # the bare --cov was not appended


def test_coverage_defers_to_bare_cov_in_pytest_args():
    # The bare ``--cov`` spelling is detected too, not only the equals form.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--cov"],
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:defer_cov_bare] forwarded = {forwarded!r}")
    assert forwarded == ["--cov"]
    assert "--cov-report=term-missing" not in forwarded


def test_coverage_defers_to_no_cov_opt_out():
    # --no-cov is the explicit pytest-cov opt-out (overrides any earlier --cov,
    # e.g. one set by addopts). If the user has it, coverage=True must defer:
    # forcing --cov on top would silently undo their opt-out.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--no-cov"],
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:defer_no_cov] forwarded = {forwarded!r}")
    assert forwarded == ["--no-cov"]
    assert "--cov" not in forwarded[1:]


def test_coverage_not_applied_to_in_process_reruns():
    # The full run is measured; the in-process rerun_failed round re-runs a
    # narrow subset and would distort the measurement -- it uses
    # list(self.pytest_args), not effective_args, so --cov is never re-added.
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
        coverage=True,
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    first = runner.calls[0]["pytest_args"]
    rerun = runner.calls[1]["pytest_args"]
    print(f"[coverage:reruns] first={first!r} rerun={rerun!r}")
    assert len(runner.calls) == 2
    assert first == ["--cov", "--cov-report=term-missing"]
    assert "--cov" not in rerun  # rerun stays uncovered
    assert not any(a.startswith("--cov") for a in rerun)


def test_coverage_does_not_mutate_user_pytest_args():
    # Like dry-run's --collect-only injection: the user's list is never mutated,
    # and a retry (second execute) appends exactly one --cov, not two.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    user_args = ["-k", "smoke"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    op.execute(_ctx())  # retry simulation
    print(f"[coverage:no_mutation] op.pytest_args = {op.pytest_args!r}")
    assert op.pytest_args == ["-k", "smoke"]
    for call in runner.calls:
        assert call["pytest_args"].count("--cov") == 1
        assert call["pytest_args"].count("--cov-report=term-missing") == 1


def test_coverage_is_applied_to_failed_only_retry():
    # Unlike the in-process rerun_failed rounds (which run uncovered), a
    # failed_only Airflow retry runs the narrowed set through effective_args,
    # so --cov DOES apply -- the narrowed set may still be a large slice users
    # want measured.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        coverage=True,
        runner=runner,
        parser=FakeParser(_res([], passed=1)),
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    forwarded = runner.calls[0]["pytest_args"]
    target = runner.calls[0]["test_path"]
    print(f"[coverage:failed_only] target={target!r} forwarded={forwarded!r}")
    assert target == ["tests/test_x.py::test_a"]  # narrowed to the prior failure
    assert forwarded == ["--cov", "--cov-report=term-missing"]


def test_coverage_combines_with_parallel():
    # coverage / parallel are independent: --cov is spliced before -n, and both
    # apply to the first full run.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        parallel=2,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:with_parallel] forwarded = {forwarded!r}")
    assert "--cov" in forwarded
    assert "--cov-report=term-missing" in forwarded
    assert "-n" in forwarded
    assert forwarded[forwarded.index("--cov") + 1] == "--cov-report=term-missing"


def test_coverage_logs_warning_when_user_owns_cov():
    # The deferral path emits a one-line warning so users can see why their
    # coverage=True did not take effect (mirrors the parallel/dist warnings).
    from unittest import mock

    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--cov=mypkg"],
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())

    logged = " ".join(str(c) for c in warning.call_args_list)
    print(f"[coverage:defer_warn] warnings = {logged!r}")
    assert "coverage measurement" in logged
    assert "--cov" in logged
    assert "deferring" in logged


# -- coverage value pushed to XCom ------------------------------------------
#
# The operator reads the overall % back from the run's stdout (the TOTAL row of
# pytest-cov's terminal table) and puts the fraction under summary["coverage"].

# A representative --cov-report=term-missing tail; TOTAL row at 85%.
_COV_STDOUT = (
    "Name        Stmts   Miss  Cover   Missing\n"
    "-----------------------------------------\n"
    "pkg/a.py       20      3    85%   10-12\n"
    "-----------------------------------------\n"
    "TOTAL          20      3    85%\n"
)


def test_coverage_fraction_pushed_to_xcom():
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:xcom] coverage = {out['coverage']!r}")
    assert out["coverage"] == 0.85  # 85% TOTAL row -> 0.85 fraction


def test_coverage_key_absent_when_disabled():
    # coverage=False -> the XCom shape is unchanged (no 'coverage' key), even if
    # the stdout happens to contain a coverage-looking table.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:xcom_absent] keys = {sorted(out)}")
    assert "coverage" not in out


def test_coverage_none_when_no_total_row():
    # coverage=True but no TOTAL row in the output (e.g. --cov-report without a
    # terminal report): the key is present and None, signalling "requested but
    # unavailable" rather than a misleading 0.0.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout="no table here\n")
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:xcom_none] coverage = {out['coverage']!r}")
    assert "coverage" in out
    assert out["coverage"] is None


def test_coverage_fraction_present_after_reruns():
    # Coverage is measured on the first full run only; the rerun rounds run
    # uncovered. The first run's fraction must survive into the final summary.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    out = op.execute(_ctx())
    print(
        f"[coverage:xcom_reruns] coverage={out['coverage']!r} rounds={out.get('rerun_rounds')}"
    )
    assert out["coverage"] == 0.85
    assert out["rerun_rounds"] == 1  # reruns happened, coverage still from run 1


def test_coverage_fraction_pushed_on_failed_only_retry():
    # The failed_only Airflow retry runs the narrowed set through effective_args
    # (with --cov), so its coverage is surfaced too.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        coverage=True,
        runner=runner,
        parser=FakeParser(_res([], passed=1)),
        store=store,
    )
    out = op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[coverage:xcom_failed_only] coverage = {out['coverage']!r}")
    assert out["coverage"] == 0.85


def test_coverage_key_absent_in_dry_run():
    # dry-run never measures coverage (no --cov), so the key must be absent even
    # with coverage=True and a coverage-looking stdout.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:xcom_dry_run] keys = {sorted(out)}")
    assert "coverage" not in out


def test_coverage_surfaced_for_user_supplied_cov():
    # The operator defers the splice to a user's explicit --cov, but still reads
    # the fraction back from the terminal TOTAL row -- so a hand-driven --cov gets
    # the XCom value for free (coverage param left at its default False).
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--cov=mypkg"],
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:xcom_user_cov] coverage = {out['coverage']!r}")
    assert out["coverage"] == 0.85


def test_coverage_key_absent_on_no_cov_opt_out():
    # --no-cov is the explicit opt-out: no measurement, so no XCom key even though
    # coverage=True (the operator defers to the opt-out).
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--no-cov"],
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:xcom_no_cov] keys = {sorted(out)}")
    assert "coverage" not in out


def test_coverage_surfaced_when_tests_fail_without_raising():
    # The meaningful red-suite path: with fail_on_test_failure=False the task does
    # NOT raise, so its summary (coverage included) is the XCom return value. A
    # failing suite must still surface its coverage number.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        fail_on_test_failure=False,
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=3)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:red_suite] success={out['success']} coverage={out['coverage']!r}")
    assert out["success"] is False  # the suite was red ...
    assert out["coverage"] == 0.85  # ... but coverage is still reported


def test_coverage_combines_with_markers():
    # coverage / markers are independent sugar: -m is spliced first, then --cov,
    # and both reach the first full run in a stable order.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke",
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[coverage:with_markers] forwarded = {forwarded!r}")
    assert forwarded == ["-m", "smoke", "--cov", "--cov-report=term-missing"]


def test_coverage_non_bool_raises_type_error():
    # A bare ``1`` would silently enable coverage if we accepted truthy ints;
    # reject any non-bool so the contract is unambiguous.
    with pytest.raises(TypeError, match="coverage"):
        PytestOperator(task_id="t", test_path="tests/", coverage=1)  # type: ignore[arg-type]


def test_coverage_none_raises_type_error():
    with pytest.raises(TypeError, match="coverage"):
        PytestOperator(task_id="t", test_path="tests/", coverage=None)  # type: ignore[arg-type]


def test_coverage_string_raises_type_error():
    with pytest.raises(TypeError, match="coverage"):
        PytestOperator(task_id="t", test_path="tests/", coverage="yes")  # type: ignore[arg-type]


# -- cov_fail_under: the native coverage gate (fraction in [0, 1]) -----------


def test_cov_fail_under_default_none():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[gate:default] cov_fail_under={op.cov_fail_under!r}")
    assert op.cov_fail_under is None


def test_cov_fail_under_auto_enables_coverage():
    # Setting only cov_fail_under (coverage left False) still measures coverage
    # AND surfaces the value -- the gate implies measurement.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.80,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[gate:auto_enable] forwarded={forwarded!r} out_cov={out.get('coverage')!r}")
    assert "--cov" in forwarded and "--cov-report=term-missing" in forwarded
    assert out["coverage"] == 0.85
    assert out["coverage_passed"] is True


def test_cov_fail_under_passes_at_threshold_boundary():
    # Boundary: coverage exactly equal to the threshold passes (>=, not >).
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.85,  # == measured 0.85
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[gate:boundary] coverage_passed={out.get('coverage_passed')}")
    assert out["coverage_passed"] is True


def test_cov_fail_under_fails_below_threshold():
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.90,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with pytest.raises(CoverageThresholdError) as exc:
        op.execute(_ctx())
    print(
        f"[gate:below] {exc.value} cov={exc.value.coverage} thr={exc.value.threshold}"
    )
    assert exc.value.coverage == 0.85
    assert exc.value.threshold == 0.90


def test_cov_fail_under_fail_closed_when_unmeasurable():
    # Gate set, coverage active, but no TOTAL row -> fail-closed (coverage=None).
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout="no table here\n")
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.80,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with pytest.raises(CoverageThresholdError) as exc:
        op.execute(_ctx())
    print(f"[gate:fail_closed] {exc.value}")
    assert exc.value.coverage is None
    assert exc.value.threshold == 0.80


def test_cov_fail_under_skipped_in_dry_run():
    # dry-run measures nothing -> the gate is inert: no raise, no coverage keys.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.99,
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    out = op.execute(_ctx())  # must NOT raise
    print(f"[gate:dry_run] keys={sorted(out)}")
    assert "coverage" not in out
    assert "coverage_passed" not in out


def test_cov_fail_under_deferred_to_no_cov_opt_out():
    # An explicit --no-cov wins; the gate is inert (no raise) -- consistent with
    # the operator's "defer to your explicit flag" rule.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--no-cov"],
        cov_fail_under=0.99,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())  # must NOT raise
    print(f"[gate:no_cov] keys={sorted(out)}")
    assert "coverage_passed" not in out


def test_cov_fail_under_test_failure_takes_precedence():
    # A red suite raises TestsFailedError first, even when coverage is also below
    # the gate -- the more fundamental failure is reported.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.99,
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=1)),
    )
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())


def test_cov_fail_under_gates_red_suite_when_failures_not_fatal():
    # With fail_on_test_failure=False the suite does not raise on red, so the
    # coverage gate becomes the active gate and can still fail the task.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.90,
        fail_on_test_failure=False,
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=1)),
    )
    with pytest.raises(CoverageThresholdError):
        op.execute(_ctx())


def test_coverage_passed_key_absent_without_gate():
    # coverage=True without cov_fail_under measures but does NOT add coverage_passed.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    print(f"[gate:no_gate] keys={sorted(out)}")
    assert "coverage_passed" not in out


def test_cov_fail_under_bool_raises_type_error():
    with pytest.raises(TypeError, match="cov_fail_under"):
        PytestOperator(task_id="t", test_path="tests/", cov_fail_under=True)  # type: ignore[arg-type]


def test_cov_fail_under_string_raises_type_error():
    with pytest.raises(TypeError, match="cov_fail_under"):
        PytestOperator(task_id="t", test_path="tests/", cov_fail_under="0.8")  # type: ignore[arg-type]


def test_cov_fail_under_above_one_raises_value_error():
    # The "I meant 80%" footgun: 80 is rejected with a pointed hint.
    with pytest.raises(ValueError, match="0.8 for 80"):
        PytestOperator(task_id="t", test_path="tests/", cov_fail_under=80)


def test_cov_fail_under_negative_raises_value_error():
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        PytestOperator(task_id="t", test_path="tests/", cov_fail_under=-0.1)


# -- extra edge coverage requested in review --------------------------------


def test_coverage_real_pytest_cov_end_to_end(tmp_path):
    # The unit tests above feed synthetic TOTAL rows; this one drives the WHOLE
    # path through real pytest-cov so a future change to its terminal format
    # (which would silently break the regex) is caught here. Skipped where
    # pytest-cov is not installed (the bare-pytest CI job).
    pytest.importorskip("pytest_cov")
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    (tmp_path / "mod.py").write_text(
        "def add(a, b):\n    return a + b\n\n"
        "def unused(x):\n    if x > 0:\n        return 'p'\n    return 'n'\n"
    )
    (tmp_path / "test_mod.py").write_text(
        "from mod import add\ndef test_add():\n    assert add(1, 2) == 3\n"
    )
    op = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_mod.py"),
        coverage=True,
        pytest_args=["--cov=mod"],
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    out = op.execute(_ctx())
    print(f"[coverage:real_e2e] coverage = {out.get('coverage')!r}")
    # mod.py: 6 statements, 3 covered -> 50% -> 0.5, parsed from REAL output.
    assert out["coverage"] == 0.5


def test_cov_fail_under_boundary_requires_full_coverage():
    # Threshold extreme 1.0 (require 100%): 85% fails, exactly 100% passes.
    below = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)  # 85%
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=1.0,
        runner=below,
        parser=FakeParser(_result(passed=1)),
    )
    with pytest.raises(CoverageThresholdError) as exc:
        op.execute(_ctx())
    print(f"[gate:require_100] below -> {exc.value}")
    assert exc.value.threshold == 1.0
    assert exc.value.coverage == 0.85

    full = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout="TOTAL  20  0  100%\n")
    )
    op2 = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=1.0,
        runner=full,
        parser=FakeParser(_result(passed=1)),
    )
    out = op2.execute(_ctx())
    print(f"[gate:require_100] full -> coverage_passed={out.get('coverage_passed')}")
    assert out["coverage"] == 1.0
    assert out["coverage_passed"] is True


def test_cov_fail_under_with_user_supplied_cov():
    # The gate evaluates the fraction surfaced from the user's OWN --cov (the
    # defer path), and the operator does not add a second --cov.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)  # 85%
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--cov=mypkg"],
        cov_fail_under=0.90,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with pytest.raises(CoverageThresholdError) as exc:
        op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[gate:user_cov] forwarded={forwarded!r} cov={exc.value.coverage}")
    assert exc.value.coverage == 0.85
    assert forwarded == ["--cov=mypkg"]  # deferred -- no second --cov spliced


def test_summary_keys_are_all_declared_in_run_summary():
    # Contract guard: a run that populates the MOST keys (coverage + gate pass +
    # reruns) must emit only keys declared in RunSummary. Catches a new summary
    # key added to the operator but not to the RunSummary TypedDict.
    from airflow_pytest_operator import RunSummary

    declared = set(RunSummary.__required_keys__) | set(RunSummary.__optional_keys__)
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout=_COV_STDOUT)  # 85%
    )
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.80,  # gate passes (0.85 >= 0.80) -> coverage_passed
        rerun_failed=1,  # -> rerun_rounds / recovered / still_failing
        runner=runner,
        parser=parser,
    )
    out = op.execute(_ctx())
    print(f"[contract] emitted keys = {sorted(out)}")
    assert set(out) <= declared, set(out) - declared
    # this scenario should populate every optional key:
    assert {
        "coverage",
        "coverage_passed",
        "rerun_rounds",
        "recovered_node_ids",
        "still_failing_node_ids",
    } <= set(out)


def test_cov_fail_under_int_normalized_to_float():
    # An int threshold (e.g. 1 == 100%) is stored as a float so the gate compares
    # and formats uniformly -- this is why the controller is built from self.*.
    op = PytestOperator(task_id="t", test_path="tests/", cov_fail_under=1)
    print(f"[gate:normalize] cov_fail_under={op.cov_fail_under!r}")
    assert op.cov_fail_under == 1.0
    assert isinstance(op.cov_fail_under, float)


def test_cov_fail_under_passes_with_reruns_using_first_run_coverage():
    # Gate evaluates the FIRST run's coverage even after in-process reruns; here
    # reruns recover the suite (green) and 0.85 >= 0.80 -> the gate passes.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout=_COV_STDOUT)  # 85%
    )
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.80,
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    out = op.execute(_ctx())
    print(
        f"[gate:reruns_pass] {out.get('coverage')} passed={out.get('coverage_passed')} rounds={out.get('rerun_rounds')}"
    )
    assert out["coverage"] == 0.85
    assert out["coverage_passed"] is True
    assert out["rerun_rounds"] == 1  # reruns happened, gate still passed
    assert out["success"] is True


def test_cov_fail_under_fails_after_reruns_recover():
    # Same shape, but the threshold is above the first-run coverage: even though
    # reruns made the suite green, the gate fires on the first run's fraction.
    runner = FakeRunner(
        RunArtifacts(exit_code=1, report_path="/x.xml", stdout=_COV_STDOUT)  # 85%
    )
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),
            _res([], passed=1),  # recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.90,
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    with pytest.raises(CoverageThresholdError) as exc:
        op.execute(_ctx())
    print(f"[gate:reruns_fail] {exc.value}")
    assert exc.value.coverage == 0.85


def test_coverage_gate_combines_with_parallel():
    # The gate and xdist are independent: --cov and -n both reach the run, and
    # the gate evaluates normally.
    runner = FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml", stdout=_COV_STDOUT)
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        cov_fail_under=0.80,
        parallel=2,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    out = op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[gate:with_parallel] forwarded={forwarded!r}")
    assert "--cov" in forwarded and "-n" in forwarded
    assert out["coverage_passed"] is True


def test_cov_fail_under_real_runner_gate(tmp_path):
    # End-to-end gate through real pytest-cov: a 50%-covered module fails an 80%
    # gate and passes a 40% gate. Skipped where pytest-cov is not installed.
    pytest.importorskip("pytest_cov")
    from airflow_pytest_operator.runners import SubprocessPytestRunner

    (tmp_path / "mod.py").write_text(
        "def add(a, b):\n    return a + b\n\n"
        "def unused(x):\n    if x > 0:\n        return 'p'\n    return 'n'\n"
    )
    (tmp_path / "test_mod.py").write_text(
        "from mod import add\ndef test_add():\n    assert add(1, 2) == 3\n"
    )

    fail = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_mod.py"),
        cov_fail_under=0.80,
        pytest_args=["--cov=mod"],
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    with pytest.raises(CoverageThresholdError) as exc:
        fail.execute(_ctx())
    print(f"[gate:real_e2e] FAIL -> {exc.value}")
    assert exc.value.coverage == 0.5

    ok = PytestOperator(
        task_id="t",
        test_path=str(tmp_path / "test_mod.py"),
        cov_fail_under=0.40,
        pytest_args=["--cov=mod"],
        runner=SubprocessPytestRunner(cwd=str(tmp_path)),
    )
    out = ok.execute(_ctx())
    print(f"[gate:real_e2e] PASS -> {out.get('coverage')} {out.get('coverage_passed')}")
    assert out["coverage"] == 0.5
    assert out["coverage_passed"] is True


def test_coverage_missing_pytest_cov_gives_actionable_error():
    # coverage active + pytest rejected --cov (pytest-cov absent) -> an actionable
    # error naming the [coverage] extra, not the generic "no report" message.
    runner = FakeRunner(
        RunArtifacts(
            exit_code=4,
            report_path=None,
            stderr="error: unrecognized arguments: --cov --cov-report=term-missing",
        )
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        coverage=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    msg = str(exc.value)
    print(f"[coverage:missing_plugin] {msg[:90]!r}")
    assert "pytest-cov is not installed" in msg
    assert "airflow-pytest-operator[coverage]" in msg


def test_no_report_without_coverage_keeps_generic_error():
    # Without coverage active, a no-report run keeps the generic message even if
    # stderr happens to mention --cov -- the hint is gated on measure_coverage.
    runner = FakeRunner(
        RunArtifacts(
            exit_code=2,
            report_path=None,
            stderr="unrecognized arguments: --cov (coverage is off here)",
        )
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",  # coverage defaults to False
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    msg = str(exc.value)
    print(f"[coverage:generic_no_report] {msg[:90]!r}")
    assert "produced no report" in msg
    assert "airflow-pytest-operator[coverage]" not in msg
