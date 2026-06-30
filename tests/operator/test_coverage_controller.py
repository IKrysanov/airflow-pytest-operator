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


"""Direct unit tests for CoverageController (operators/_coverage.py).

The operator's integration tests (test_op_coverage.py) drive the controller
through execute(); these pin its pieces in isolation: the splice/active/defer
decision, the gate, the TOTAL-row parser, and the request/gate properties.
"""

from __future__ import annotations

import time

import pytest

from airflow_pytest_operator.exceptions import (
    AirflowPytestError,
    CoverageThresholdError,
)
from airflow_pytest_operator.operators._coverage import CoverageController


def _ctl(coverage=False, cov_fail_under=None):
    return CoverageController(coverage=coverage, cov_fail_under=cov_fail_under)


# -- properties --------------------------------------------------------------


def test_requested_and_gate_enabled():
    off = _ctl()
    assert off.requested is False and off.gate_enabled is False
    flag = _ctl(coverage=True)
    assert flag.requested is True and flag.gate_enabled is False
    gate = _ctl(cov_fail_under=0.5)
    assert gate.requested is True and gate.gate_enabled is True


# -- augment_args ------------------------------------------------------------


def test_augment_splices_when_coverage_requested():
    args = ["-k", "smoke"]
    active, deferred = _ctl(coverage=True).augment_args(args, dry_run=False)
    assert args == ["-k", "smoke", "--cov", "--cov-report=term-missing"]
    assert active is True and deferred is False


def test_augment_auto_enables_for_gate_only():
    args = []
    active, deferred = _ctl(cov_fail_under=0.8).augment_args(args, dry_run=False)
    assert args == ["--cov", "--cov-report=term-missing"]
    assert active is True and deferred is False


def test_augment_noop_when_not_requested():
    args = ["-q"]
    active, deferred = _ctl().augment_args(args, dry_run=False)
    assert args == ["-q"]
    assert active is False and deferred is False


def test_augment_skips_dry_run():
    args = []
    active, deferred = _ctl(coverage=True).augment_args(args, dry_run=True)
    assert args == []
    assert active is False and deferred is False


def test_augment_defers_to_user_cov():
    args = ["--cov=pkg"]
    active, deferred = _ctl(coverage=True).augment_args(args, dry_run=False)
    assert args == ["--cov=pkg"]  # nothing spliced
    assert active is True and deferred is True


def test_augment_defers_to_no_cov_opt_out():
    args = ["--no-cov"]
    active, deferred = _ctl(coverage=True).augment_args(args, dry_run=False)
    assert args == ["--no-cov"]
    assert active is False and deferred is True


# -- evaluate_gate -----------------------------------------------------------


def test_gate_passes_above_and_at_boundary():
    _ctl(cov_fail_under=0.80).evaluate_gate(0.85)  # above -> no raise
    _ctl(cov_fail_under=0.80).evaluate_gate(0.80)  # equal -> no raise


def test_gate_fails_below():
    with pytest.raises(CoverageThresholdError) as exc:
        _ctl(cov_fail_under=0.80).evaluate_gate(0.79)
    assert exc.value.coverage == 0.79
    assert exc.value.threshold == 0.80


def test_gate_fail_closed_on_unmeasurable():
    with pytest.raises(CoverageThresholdError) as exc:
        _ctl(cov_fail_under=0.80).evaluate_gate(None)
    assert exc.value.coverage is None
    assert exc.value.threshold == 0.80


def test_coverage_threshold_error_message_and_base():
    # The error is catchable as the package base, and its message is actionable
    # (it reaches the Airflow task log, so the wording is part of the contract).
    below = CoverageThresholdError(0.5, 0.8)
    assert isinstance(below, AirflowPytestError)
    assert below.coverage == 0.5 and below.threshold == 0.8
    assert "50.00%" in str(below) and "80.00%" in str(below) and "below" in str(below)

    unmeasured = CoverageThresholdError(None, 0.8)
    assert unmeasured.coverage is None
    assert "could not be" in str(unmeasured)


# -- extract (the TOTAL-row parser) ------------------------------------------


def test_extract_handles_formats():
    extract = CoverageController.extract
    # plain row, --cov-branch columns, configured precision, 0/100 bounds, none.
    assert extract("TOTAL          20      3    85%\n") == 0.85
    assert extract("TOTAL          20      3     8      2    88%\n") == 0.88
    assert extract("TOTAL          20      3    85.25%\n") == 0.8525
    assert extract("TOTAL          20     20     0%\n") == 0.0  # 0.0, not None
    assert extract("TOTAL          20      0   100%\n") == 1.0
    assert extract("nothing to see\n") is None
    assert extract("") is None


def test_extract_edge_cases():
    extract = CoverageController.extract
    assert extract("TOTALS booked 100% done\n") is None  # word boundary
    assert extract("TOTAL  20  19  5%\n") == 0.05  # single digit
    assert extract("TOTAL\t20\t3\t85%\n") == 0.85  # tabs
    assert extract("TOTAL  20  3  85%\r\n") == 0.85  # CRLF
    assert extract("Name  Stmts\nTOTAL  20  3  85%") == 0.85  # no trailing newline


def test_extract_takes_last_total_row():
    # A test that prints its own "TOTAL ... NN%" earlier must not shadow the real
    # coverage table, which pytest-cov prints last in the run summary.
    stdout = (
        "test_report.py::test_summary PASSED\n"
        "TOTAL revenue 42%  <- printed by the test itself\n"
        "==================== tests coverage ====================\n"
        "TOTAL          20      3    85%\n"
    )
    assert CoverageController.extract(stdout) == 0.85


def test_extract_no_catastrophic_backtracking():
    # Security/load guard: a ~1.8 MiB "TOTAL"-prefixed line with NO trailing '%'
    # is the worst case for the lazy ``.*?``. A vulnerable (ReDoS-prone) pattern
    # would hang here; the anchored lazy match is linear, so it returns fast.
    adversarial = "TOTAL " + "1.2" * 300_000  # ~1.8 MiB, no '%'
    start = time.perf_counter()
    assert CoverageController.extract(adversarial) is None
    assert time.perf_counter() - start < 5.0  # generous; real time is ~0.1s


def test_extract_huge_percent_does_not_crash():
    # A malformed >100% row (only possible from hostile/buggy output) must yield a
    # number, never raise -- ``float(...) / 100`` cannot fail on the regex capture.
    assert CoverageController.extract("TOTAL 1 1 999999%\n") == 9999.99
