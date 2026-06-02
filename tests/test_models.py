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

import pytest

from airflow_pytest_operator.models import CaseResult, ReportRequest, TestRunResult


def test_node_id_with_classname():
    case = CaseResult(
        name="test_a", classname="tests.test_mod", time=0.0, outcome="passed"
    )
    print(f"node_id: {case.node_id!r}")
    assert case.node_id == "tests.test_mod::test_a"


def test_node_id_without_classname_falls_back_to_name():
    # Covers the branch where classname is empty: the node id is just the
    # bare test name (models.py node_id fallback).
    case = CaseResult(name="test_a", classname="", time=0.0, outcome="passed")
    print(f"node_id (no classname): {case.node_id!r}")
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
    print(f"failed_node_ids: {result.failed_node_ids}")
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
    print(f"xcom payload: {payload}")
    # Per-case blobs are dropped from the compact XCom summary...
    assert "cases" not in payload
    # ...and derived fields are present.
    assert payload["success"] is False
    assert payload["failed_node_ids"] == ["m::t"]
    assert payload["total"] == 1


def test_report_request_carries_args_and_path():
    spec = ReportRequest(
        pytest_args=("--junitxml=/tmp/r.xml", "-o", "junit_logging=all"),
        report_path="/tmp/r.xml",
    )
    assert spec.pytest_args == ("--junitxml=/tmp/r.xml", "-o", "junit_logging=all")
    assert spec.report_path == "/tmp/r.xml"


def test_report_request_allows_none_report_path():
    # None is the documented signal for parsers that read stdout instead
    # of a file (no implementation ships in 0.4.0, but the type permits it).
    spec = ReportRequest(pytest_args=("-v",), report_path=None)
    assert spec.report_path is None


def test_report_request_is_frozen():
    import dataclasses

    spec = ReportRequest(pytest_args=("--x",), report_path="/p")
    # frozen=True is part of the contract: parsers must not mutate a spec
    # after declaring it (runners may pass it around).
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.report_path = "/other"  # type: ignore[misc]
