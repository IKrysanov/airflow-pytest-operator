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

import re

from ..exceptions import CoverageThresholdError
from ._constants import COV_FLAGS, has_flag

_COVERAGE_TOTAL_RE = re.compile(r"^TOTAL\s+.*?(\d+(?:\.\d+)?)%", re.MULTILINE)


class CoverageController:
    """Splice/measure/gate coverage for one operator instance.

    Holds the two operator parameters that drive coverage and exposes small,
    mostly-pure operations the operator calls in order: :meth:`augment_args`
    (decide the flags), :meth:`extract` (read the fraction), and
    :meth:`evaluate_gate` (enforce ``cov_fail_under``).
    """

    def __init__(self, *, coverage: bool, cov_fail_under: float | None) -> None:
        self.coverage = coverage
        self.cov_fail_under = cov_fail_under

    @property
    def requested(self) -> bool:
        """True when coverage was asked for -- by the flag or by the gate."""
        return self.coverage or self.cov_fail_under is not None

    @property
    def gate_enabled(self) -> bool:
        """True when a ``cov_fail_under`` gate is configured."""
        return self.cov_fail_under is not None

    def augment_args(
        self, effective_args: list[str], *, dry_run: bool
    ) -> tuple[bool, bool]:
        """Splice ``--cov``/report flags into ``effective_args`` *in place*.

        Adds ``--cov --cov-report=term-missing`` on the first full run when
        coverage is requested and the user is not already driving it. Skipped in
        dry-run. Returns ``(active, deferred)``:

        - ``active``   -- a coverage ``TOTAL`` row will be produced (``--cov`` is
          in effect and not opted out with ``--no-cov``), so a fraction can be
          read and the gate evaluated;
        - ``deferred`` -- the user already set ``--cov``/``--no-cov`` in
          ``pytest_args``, so nothing was spliced (the operator logs a warning).
        """
        deferred = False
        if not dry_run and self.requested:
            if has_flag(effective_args, COV_FLAGS):
                deferred = True
            else:
                effective_args.extend(["--cov", "--cov-report=term-missing"])
        active = (
            not dry_run
            and has_flag(effective_args, ("--cov",))
            and not has_flag(effective_args, ("--no-cov",))
        )
        return active, deferred

    @staticmethod
    def extract(stdout: str) -> float | None:
        """Overall line-coverage fraction parsed from pytest-cov's term report.

        Scans ``stdout`` for the ``TOTAL`` row of the coverage table (printed by
        ``--cov-report=term`` / ``term-missing``) and returns it as a fraction in
        ``[0, 1]`` -- a ``TOTAL ... 85%`` row yields ``0.85``. The match anchors
        on the row's trailing percentage, so the extra columns ``--cov-branch``
        adds do not confuse it, and a configured ``[tool.coverage.report]
        precision`` (e.g. ``85.25%`` -> ``0.8525``) is honoured since we read the
        very number shown in the log. Returns ``None`` when no coverage table is
        present (pytest-cov absent, a ``--cov-report`` without a terminal report,
        or no data collected), so the caller records "requested but unavailable"
        rather than a wrong ``0``.

        Takes the *last* matching row, not the first: pytest-cov prints its table
        in the terminal summary at the very end, so a test that happened to emit a
        ``TOTAL ... NN%``-looking line of its own earlier in stdout cannot shadow
        the real coverage total.
        """
        matches = _COVERAGE_TOTAL_RE.findall(stdout)
        if not matches:
            return None
        return float(matches[-1]) / 100.0

    def evaluate_gate(self, coverage: float | None) -> None:
        """Enforce ``cov_fail_under``; raise on failure, return on pass.

        Fail-closed: an unmeasurable run (``coverage is None``) under an active
        gate is a failure, not a silent pass. The caller invokes this only when
        coverage was active and :attr:`gate_enabled` is True.
        """
        assert self.cov_fail_under is not None  # gate_enabled checked by caller
        if coverage is None or coverage < self.cov_fail_under:
            raise CoverageThresholdError(coverage, self.cov_fail_under)
