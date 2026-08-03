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


"""Direct unit tests for FailureThresholdController (operators/_threshold.py).

The operator's integration tests (test_op_threshold.py) drive the controller
through execute(); these pin its pieces in isolation -- the pass-rate
arithmetic, the verdict, and how both behave on a report whose counters do not
add up (the JSON parser reads them straight out of the report's own summary
block, so they are attacker/corruption-influenced input, not derived facts).
"""

from __future__ import annotations

import pytest

from airflow_pytest_operator.exceptions import (
    AirflowPytestError,
    FailureThresholdError,
)
from airflow_pytest_operator.models import TestRunResult
from airflow_pytest_operator.operators._threshold import FailureThresholdController


def _ctl(min_pass_rate=None, max_failed=None):
    return FailureThresholdController(
        min_pass_rate=min_pass_rate, max_failed=max_failed
    )


def _r(*, total, passed=0, failed=0, skipped=0, errors=0, exit_code=0):
    # Deliberately unconstrained: each counter is set independently so a test
    # can build a report whose numbers contradict each other.
    return TestRunResult(
        total=total,
        passed=passed,
        failed=failed,
        skipped=skipped,
        errors=errors,
        duration=0.0,
        exit_code=exit_code,
    )


# -- enabled ------------------------------------------------------------------


def test_enabled_reflects_either_parameter():
    assert _ctl().enabled is False
    assert _ctl(min_pass_rate=0.5).enabled is True
    assert _ctl(max_failed=0).enabled is True  # 0 is a threshold, not "unset"
    assert _ctl(min_pass_rate=0.0).enabled is True


def test_disabled_controller_never_breaches():
    # Even a catastrophic run: with no threshold configured there is nothing to
    # enforce, and the operator's binary policy stays in charge.
    verdict = _ctl().check(_r(total=10, failed=10, exit_code=3))
    assert verdict is None


# -- pass_rate ----------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"total": 100, "passed": 100}, 1.0),
        ({"total": 100, "passed": 0, "failed": 100}, 0.0),
        ({"total": 100, "passed": 90, "failed": 10}, 0.9),
        ({"total": 100, "passed": 90, "failed": 5, "errors": 5}, 0.9),
        # skipped leaves the denominator: 9 of 10 executed, not 9 of 100.
        ({"total": 100, "passed": 9, "failed": 1, "skipped": 90}, 0.9),
        ({"total": 3, "passed": 2, "failed": 1}, 2 / 3),
    ],
)
def test_pass_rate_arithmetic(kwargs, expected):
    assert FailureThresholdController.pass_rate(_r(**kwargs)) == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {"total": 0},  # empty run
        {"total": 500, "skipped": 500},  # everything skipped
        {"total": 10, "skipped": 99},  # malformed: more skipped than total
    ],
)
def test_pass_rate_is_none_when_nothing_was_executed(kwargs):
    # Undefined, not zero -- zero would read as "everything failed" and is a
    # different (wrong) story. A negative denominator can only come from a
    # malformed report and is treated the same way.
    assert FailureThresholdController.pass_rate(_r(**kwargs)) is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"total": 10, "passed": 500},  # more passed than ever collected
        {"total": 10, "passed": 5, "failed": 900},  # outcomes exceed total
        {"total": 10, "passed": -5},  # negative counters
        {"total": 100, "passed": 100, "failed": -50, "skipped": -1000},
        {"total": -1},
    ],
)
def test_incoherent_counters_make_the_rate_undefined(kwargs):
    # Clamping an impossible rate to 1.0 would read as a perfect run -- exactly
    # the wrong direction. A report that cannot describe a real run yields no
    # rate at all, and check() refuses it outright.
    assert FailureThresholdController.counters_are_coherent(_r(**kwargs)) is False
    assert FailureThresholdController.pass_rate(_r(**kwargs)) is None


def test_incoherent_counters_defeat_an_absolute_cap_too():
    # The dangerous one: failed=-50 satisfies "at most 0 failures" arithmetically.
    err = _ctl(max_failed=0).check(_r(total=100, passed=100, failed=-50, exit_code=1))
    assert err is not None
    assert "not coherent" in str(err)


def test_a_partial_run_is_coherent_and_rated_down():
    # Outcomes summing to LESS than total is normal (pytest-json-report's total
    # is the collected count): the uncounted tests stay in the denominator, so
    # the rate drops -- the safe direction, and not an incoherence.
    partial = _r(total=500, passed=10, exit_code=2)
    assert FailureThresholdController.counters_are_coherent(partial) is True
    assert FailureThresholdController.pass_rate(partial) == 0.02


def test_a_single_failure_never_rounds_up_to_a_perfect_rate():
    # float64 cannot represent 1 - 1/n below 1.0 past ~9e15 executed tests, so a
    # colossal report could round a failing run up to exactly 1.0 and slip past
    # min_pass_rate=1.0. The rate is nudged to the largest double below 1.0.
    huge = _r(total=10**18, passed=10**18 - 1, failed=1, exit_code=1)
    assert huge.passed / huge.total == 1.0  # the naive arithmetic
    assert FailureThresholdController.pass_rate(huge) < 1.0
    assert _ctl(min_pass_rate=1.0).check(huge) is not None
    # A genuinely perfect run is untouched.
    assert FailureThresholdController.pass_rate(_r(total=10**18, passed=10**18)) == 1.0


def test_pass_rate_is_not_rounded():
    # The number surfaced in XCom must be the one the gate compared, or a user
    # sees "pass_rate 0.95" next to "below min_pass_rate=0.95".
    rate = FailureThresholdController.pass_rate(
        _r(total=10000, passed=9499, failed=501)
    )
    assert rate == 0.9499
    assert _ctl(min_pass_rate=0.95).check(_r(total=10000, passed=9499, failed=501))


# -- check: the verdict -------------------------------------------------------


def test_min_pass_rate_verdict_boundaries():
    ctl = _ctl(min_pass_rate=0.95)
    assert ctl.check(_r(total=100, passed=95, failed=5, exit_code=1)) is None
    assert ctl.check(_r(total=100, passed=94, failed=6, exit_code=1)) is not None


def test_max_failed_verdict_boundaries():
    ctl = _ctl(max_failed=3)
    assert ctl.check(_r(total=100, passed=97, failed=3, exit_code=1)) is None
    assert ctl.check(_r(total=100, passed=96, failed=4, exit_code=1)) is not None
    # failed and errors are summed -- both are "not a pass".
    assert ctl.check(_r(total=100, passed=96, failed=2, errors=2, exit_code=1))


def test_error_carries_the_numbers_behind_the_verdict():
    err = _ctl(min_pass_rate=0.95, max_failed=1).check(
        _r(total=100, passed=50, failed=40, errors=10, exit_code=1)
    )
    assert isinstance(err, FailureThresholdError)
    assert isinstance(err, AirflowPytestError)  # catchable by the package base
    assert err.pass_rate == 0.5
    assert err.failures == 50
    assert err.min_pass_rate == 0.95
    assert err.max_failed == 1
    assert len(err.reasons) == 2
    assert str(err) == "; ".join(err.reasons)


def test_abnormal_exit_is_reported_alongside_the_other_reasons():
    err = _ctl(min_pass_rate=0.95).check(_r(total=10, passed=5, failed=5, exit_code=3))
    assert err is not None
    assert len(err.reasons) == 2
    assert any("exited with code 3" in r for r in err.reasons)


@pytest.mark.parametrize("exit_code", [-1, 2, 3, 4, 5, 99])
def test_any_non_test_outcome_exit_breaches(exit_code):
    # Unknown/negative codes (a signal-killed child reports -N) are abnormal
    # too: the allowlist is 0/1, not a denylist of the codes we happen to know.
    assert _ctl(max_failed=0).check(_r(total=1, passed=1, exit_code=exit_code))


def test_exit_one_must_be_explained_by_a_recorded_failure():
    # Exit 1 means "some tests failed". A report showing none contradicts it --
    # the run was failed by something else (coverage fail_under, a plugin gate)
    # or the report is truncated. Either way the tolerance cannot judge it.
    err = _ctl(max_failed=5).check(_r(total=10, passed=10, exit_code=1))
    assert err is not None
    assert "records no failure" in str(err)
    # ... and the consistent case is untouched.
    assert (
        _ctl(max_failed=5).check(_r(total=10, passed=9, failed=1, exit_code=1)) is None
    )


def test_exit_zero_with_recorded_failures_is_still_judged_normally():
    # The mirror inconsistency (exit 0 but failures recorded) stays with the
    # tolerance: it is judged on the numbers, which is the conservative reading.
    assert _ctl(max_failed=0).check(_r(total=10, passed=9, failed=1, exit_code=0))
    assert (
        _ctl(max_failed=5).check(_r(total=10, passed=9, failed=1, exit_code=0)) is None
    )


def test_empty_reasons_never_produce_a_passing_error():
    # Guard on the error itself: an empty reason list still yields a message, so
    # a future caller cannot construct a silent, message-less failure.
    err = FailureThresholdError([], pass_rate=None, failures=0)
    assert str(err) == "failure threshold breached"
