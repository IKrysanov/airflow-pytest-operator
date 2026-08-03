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

import math

from ..exceptions import FailureThresholdError
from ..models import TestRunResult
from ._constants import EXIT_TESTS_FAILED, TEST_OUTCOME_EXIT_CODES


class FailureThresholdController:
    """Turn a parsed run into a "how red is too red" verdict.

    Holds ``min_pass_rate`` / ``max_failed`` for one operator instance. Mirrors
    :class:`~airflow_pytest_operator.operators._coverage.CoverageController`: the
    operator keeps the logging and the XCom summary, this owns the arithmetic.
    """

    def __init__(self, *, min_pass_rate: float | None, max_failed: int | None) -> None:
        self.min_pass_rate = min_pass_rate
        self.max_failed = max_failed

    @property
    def enabled(self) -> bool:
        """True when a tolerance threshold is configured (either parameter)."""
        return self.min_pass_rate is not None or self.max_failed is not None

    @staticmethod
    def counters_are_coherent(result: TestRunResult) -> bool:
        """True when the report's counters can describe a real run.

        The JSON parser reads them straight out of the report's own ``summary``
        block, so they are input, not derived facts: a corrupt report can carry
        negatives or outcomes adding up to more than ``total``, and "500 passed
        out of 10" would otherwise be a 100% pass rate.

        One-sided on purpose: outcomes summing to *less* than ``total`` is a
        normal partial run (``total`` is the collected count). Those uncounted
        tests stay in the denominator and lower the rate -- the safe direction.
        """
        counters = (
            result.total,
            result.passed,
            result.failed,
            result.skipped,
            result.errors,
        )
        if any(c < 0 for c in counters):
            return False
        return result.passed + result.failed + result.errors + result.skipped <= (
            result.total
        )

    @classmethod
    def pass_rate(cls, result: TestRunResult) -> float | None:
        """Fraction of *executed* tests that passed -- ``passed / (total - skipped)``.

        Skipped tests leave the denominator: a suite that skips what does not
        apply should not have its rate diluted by tests that never ran.

        ``None`` when the rate is undefined -- an empty denominator, or
        incoherent counters. Not zero, which would read as "everything failed".
        :meth:`check` fails closed on it. Never rounded, so the number in XCom is
        the one the gate compared.
        """
        if not cls.counters_are_coherent(result):
            return None
        executed = result.total - result.skipped
        if executed <= 0:
            return None
        rate = result.passed / executed
        # Past ~9e15 executed tests float64 cannot tell 1 - 1/n from 1.0, which
        # would round a run that DID fail past min_pass_rate=1.0. The largest
        # double below 1.0 is the honest answer; a clean run never gets here.
        if rate == 1.0 and result.failed + result.errors > 0:
            return math.nextafter(1.0, 0.0)
        return rate

    def check(self, result: TestRunResult) -> FailureThresholdError | None:
        """The verdict: an error to raise, or ``None`` when within tolerance.

        Returns rather than raises so the operator can do its ``failed_only``
        bookkeeping between "this attempt fails" and actually failing.

        Breaches on a rate below ``min_pass_rate`` (or undefined -- fail-closed,
        like ``cov_fail_under``), on ``failed + errors`` above ``max_failed``, on
        incoherent counters (which also invalidate a cap a negative "failed"
        would satisfy), and on an exit code the report does not explain. All
        reasons are reported together so one log line shows the whole picture.

        A guard against truncated and inconsistent reports, not a hostile one:
        the report is written by the suite's own process, so test code that
        rewrites it can misreport results here exactly as under
        ``fail_on_test_failure``.
        """
        if not self.enabled:
            return None

        rate = self.pass_rate(result)
        failures = result.failed + result.errors
        reasons: list[str] = []

        if self.min_pass_rate is not None:
            if rate is None:
                reasons.append(
                    "pass rate is undefined -- no test was executed "
                    f"(total={result.total}, skipped={result.skipped}), so "
                    f"min_pass_rate={self.min_pass_rate:.2%} could not be "
                    "evaluated (fail-closed)"
                )
            elif rate < self.min_pass_rate:
                reasons.append(
                    f"pass rate {rate:.2%} ({result.passed} passed of "
                    f"{result.total - result.skipped} executed) is below "
                    f"min_pass_rate={self.min_pass_rate:.2%}"
                )

        if self.max_failed is not None and failures > self.max_failed:
            reasons.append(
                f"{failures} failed/errored test(s) exceed max_failed={self.max_failed}"
            )

        if not self.counters_are_coherent(result):
            reasons.append(
                f"the report's counters are not coherent (total={result.total}, "
                f"passed={result.passed}, failed={result.failed}, "
                f"errors={result.errors}, skipped={result.skipped}): no real run "
                "produces these, so the tolerance cannot be applied to them "
                "(fail-closed)"
            )

        if result.exit_code not in TEST_OUTCOME_EXIT_CODES:
            reasons.append(
                f"pytest exited with code {result.exit_code}, which is not a "
                "test-outcome exit (0 = all passed, 1 = tests failed): the run "
                "was interrupted, crashed, or was misconfigured, so its counters "
                "are not a complete tally and the tolerance does not apply "
                "(fail-closed)"
            )
        elif result.exit_code == EXIT_TESTS_FAILED and failures == 0:
            reasons.append(
                "pytest exited with code 1 (tests failed) but the report records "
                "no failure: something other than a failing test failed the run "
                "-- coverage.py's own fail_under, a plugin's gate, or a truncated "
                "report. A tolerance for test failures cannot absorb it "
                "(fail-closed)"
            )

        if not reasons:
            return None
        return FailureThresholdError(
            reasons,
            pass_rate=rate,
            failures=failures,
            min_pass_rate=self.min_pass_rate,
            max_failed=self.max_failed,
        )
