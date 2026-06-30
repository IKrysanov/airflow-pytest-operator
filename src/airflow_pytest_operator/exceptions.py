"""Exception hierarchy for the operator.

A small, focused hierarchy lets callers (and Airflow's retry logic)
distinguish *test failures* from *infrastructure failures*. That
distinction matters: a failing test usually shouldn't be retried,
but a missing pytest binary or unreadable report might be.
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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import TestRunResult


class AirflowPytestError(Exception):
    """Base class for all errors raised by this package."""


class TestExecutionError(AirflowPytestError):
    """The runner could not execute pytest at all (binary missing, etc.).

    When the failure happened *after* the child started producing output
    (most importantly a timeout), the captured streams are attached as
    ``stdout`` / ``stderr`` so callers (operators, UIs) can surface the
    diagnostic programmatically instead of digging through worker logs.
    Both default to ``None`` for failures that have no associated output
    (e.g. a missing interpreter), preserving the plain
    ``TestExecutionError("message")`` construction.
    """

    __test__ = False

    def __init__(
        self,
        message: str,
        *,
        stdout: str | None = None,
        stderr: str | None = None,
    ) -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr


class ReportParseError(AirflowPytestError):
    """A report file was produced but could not be parsed."""


class TestsFailedError(AirflowPytestError):
    """Pytest ran successfully but one or more tests failed.

    Carries the structured result so downstream handlers can inspect it.
    """

    __test__ = False

    def __init__(self, result: TestRunResult) -> None:
        self.result = result
        super().__init__(
            f"{result.failed} failed, {result.errors} errors "
            f"out of {result.total} tests"
        )


class CoverageThresholdError(AirflowPytestError):
    """Coverage fell below (or could not be measured for) the ``cov_fail_under`` gate.

    Raised by :class:`PytestOperator` when ``cov_fail_under`` is set and the
    run's overall coverage fraction is below it -- or could not be read at all
    (fail-closed). Carries the measured ``coverage`` (a fraction in ``[0, 1]``,
    or ``None`` when no total could be parsed) and the ``threshold`` so handlers
    can inspect both.
    """

    def __init__(self, coverage: float | None, threshold: float) -> None:
        self.coverage = coverage
        self.threshold = threshold
        if coverage is None:
            super().__init__(
                f"coverage gate cov_fail_under={threshold} could not be "
                "evaluated: no coverage total was read from the run (ensure a "
                "terminal coverage report -- term / term-missing)"
            )
        else:
            super().__init__(
                f"coverage {coverage:.2%} is below the cov_fail_under "
                f"threshold of {threshold:.2%}"
            )
