"""Tests for the framework-agnostic domain models.

These cover small but real branches: node-id reconstruction with and
without a classname, the success property, and the XCom projection that
drops per-case detail. They need no Airflow, no subprocess, no XML.
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

from airflow_pytest_operator.models import CaseResult, TestRunResult


def test_node_id_with_classname():
    case = CaseResult(
        name="test_a", classname="tests.test_mod", time=0.0, outcome="passed"
    )
    assert case.node_id == "tests.test_mod::test_a"


def test_node_id_without_classname_falls_back_to_name():
    # Covers the branch where classname is empty: the node id is just the
    # bare test name (models.py node_id fallback).
    case = CaseResult(name="test_a", classname="", time=0.0, outcome="passed")
    assert case.node_id == "test_a"


def test_success_true_when_no_failures_or_errors():
    result = TestRunResult(
        total=2, passed=2, failed=0, skipped=0, errors=0, duration=0.2, exit_code=0
    )
    assert result.success is True
    assert result.failed_node_ids == []


def test_success_false_with_failures():
    result = TestRunResult(
        total=1, passed=0, failed=1, skipped=0, errors=0, duration=0.1, exit_code=1
    )
    assert result.success is False


def test_failed_node_ids_include_errors_and_failures_only():
    cases = [
        CaseResult(name="t_ok", classname="m", time=0.0, outcome="passed"),
        CaseResult(name="t_fail", classname="m", time=0.0, outcome="failed"),
        CaseResult(name="t_err", classname="m", time=0.0, outcome="error"),
        CaseResult(name="t_skip", classname="m", time=0.0, outcome="skipped"),
    ]
    result = TestRunResult(
        total=4,
        passed=1,
        failed=1,
        skipped=1,
        errors=1,
        duration=0.4,
        exit_code=1,
        cases=cases,
    )
    assert result.failed_node_ids == ["m::t_fail", "m::t_err"]


def test_to_xcom_drops_cases_and_adds_derived_fields():
    cases = [CaseResult(name="t", classname="m", time=0.0, outcome="failed")]
    result = TestRunResult(
        total=1,
        passed=0,
        failed=1,
        skipped=0,
        errors=0,
        duration=0.1,
        exit_code=1,
        cases=cases,
    )
    payload = result.to_xcom()
    # Per-case blobs are dropped from the compact XCom summary...
    assert "cases" not in payload
    # ...and derived fields are present.
    assert payload["success"] is False
    assert payload["failed_node_ids"] == ["m::t"]
    assert payload["total"] == 1
