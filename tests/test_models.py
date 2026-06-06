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
    spec = ReportRequest(pytest_args=("-v",), report_path=None)
    assert spec.report_path is None


def test_report_request_is_frozen():
    import dataclasses

    spec = ReportRequest(pytest_args=("--x",), report_path="/p")
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.report_path = "/other"  # type: ignore[misc]


def test_success_false_when_exit_code_nonzero_but_no_test_failures():
    """exit_code != 0 alone is enough to flip success to False."""
    result = TestRunResult(
        total=1, passed=1, failed=0, skipped=0, errors=0, duration=0.0, exit_code=2
    )
    assert result.success is False


def test_success_false_with_errors_only():
    result = TestRunResult(
        total=1, passed=0, failed=0, skipped=0, errors=1, duration=0.0, exit_code=0
    )
    assert result.success is False


def test_exception_hierarchy():
    from airflow_pytest_operator.exceptions import (
        AirflowPytestError,
        ReportParseError,
        TestExecutionError,
        TestsFailedError,
    )

    assert issubclass(TestExecutionError, AirflowPytestError)
    assert issubclass(ReportParseError, AirflowPytestError)
    assert issubclass(TestsFailedError, AirflowPytestError)


def test_tests_failed_error_carries_result():
    from airflow_pytest_operator.exceptions import TestsFailedError

    result = TestRunResult(
        total=2, passed=1, failed=1, skipped=0, errors=0, duration=0.1, exit_code=1
    )
    exc = TestsFailedError(result)
    assert exc.result is result
    assert "1 failed" in str(exc)
    assert "2 tests" in str(exc)


def test_cases_is_immutable_tuple_after_construction():
    """``cases`` is stored as a tuple so the frozen-dataclass claim is real.

    Before 0.4.1 this field was a list, which made ``frozen=True`` a
    half-measure: ``result.cases = [...]`` raised FrozenInstanceError,
    but ``result.cases.append(...)`` silently mutated the "frozen"
    instance. After the fix the field is a tuple and append() raises.
    """
    import dataclasses

    result = TestRunResult(
        total=1,
        passed=1,
        failed=0,
        skipped=0,
        errors=0,
        duration=0.0,
        exit_code=0,
        cases=[CaseResult(name="t", classname="", time=0.0, outcome="passed")],
    )

    # Reassignment is blocked by frozen=True (was already true before fix).
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.cases = ()  # type: ignore[misc]

    # NEW: in-place mutation is also blocked because cases is a tuple.
    # Tuples do not support .append/.extend/.__setitem__/.clear/etc.
    print(f"[cases_immutable] type(cases) = {type(result.cases).__name__}")
    assert isinstance(result.cases, tuple)
    with pytest.raises(AttributeError):
        result.cases.append(  # type: ignore[attr-defined]
            CaseResult(name="x", classname="", time=0.0, outcome="passed")
        )
    with pytest.raises(TypeError):
        result.cases[0] = CaseResult(  # type: ignore[index]
            name="x", classname="", time=0.0, outcome="passed"
        )


def test_cases_accepts_list_input_for_caller_convenience():
    """Parsers accumulate cases via ``.append`` on a local list and
    pass that list straight into ``TestRunResult(cases=...)``. The
    ``__post_init__`` coerces it to a tuple so callers don't have to
    write ``tuple(cases)`` everywhere. The end result is still a
    tuple inside the instance.
    """
    cases_list = [
        CaseResult(name="a", classname="m", time=0.0, outcome="passed"),
        CaseResult(name="b", classname="m", time=0.0, outcome="failed"),
    ]
    result = TestRunResult(
        total=2,
        passed=1,
        failed=1,
        skipped=0,
        errors=0,
        duration=0.0,
        exit_code=1,
        cases=cases_list,  # list, not tuple
    )

    assert isinstance(result.cases, tuple)
    assert len(result.cases) == 2
    print(
        f"[cases_list_input] passed list of {len(cases_list)} "
        f"-> stored as {type(result.cases).__name__} "
        f"of len {len(result.cases)}"
    )

    # And the original list MUST be decoupled from the stored tuple:
    # mutating the caller's list after construction must not leak
    # into the result. tuple(list) produces a fresh tuple, so this
    # decoupling is automatic, but worth pinning.
    cases_list.append(CaseResult(name="c", classname="m", time=0.0, outcome="error"))
    assert len(result.cases) == 2


def test_cases_default_is_empty_tuple():
    # The default for cases was a list literal via field(default_factory).
    # It's now a plain tuple default, which is safe to share (immutable).
    result = TestRunResult(
        total=0,
        passed=0,
        failed=0,
        skipped=0,
        errors=0,
        duration=0.0,
        exit_code=0,
    )
    print(f"[cases_default] cases = {result.cases!r}")
    assert result.cases == ()
    assert isinstance(result.cases, tuple)


def test_to_xcom_keys_match_intentional_contract():
    import dataclasses

    EXPECTED_KEYS = {
        "total",
        "passed",
        "failed",
        "skipped",
        "errors",
        "duration",
        "exit_code",
        "success",
        "failed_node_ids",
    }
    EXPECTED_EXCLUDED = {
        "cases",
    }

    schema_fields = {f.name for f in dataclasses.fields(TestRunResult)}
    print(f"[xcom_contract] dataclass fields: {sorted(schema_fields)}")
    print(f"[xcom_contract] expected keys:    {sorted(EXPECTED_KEYS)}")

    unaccounted = schema_fields - EXPECTED_KEYS - EXPECTED_EXCLUDED
    assert not unaccounted, (
        f"New TestRunResult field(s) {unaccounted} are neither shipped "
        "through XCom nor explicitly excluded. Update to_xcom() AND this "
        "test's EXPECTED_KEYS/EXPECTED_EXCLUDED to make the decision "
        "explicit. (Why this test exists: to_xcom is hand-rolled for "
        "performance; without this gate, new fields would silently miss "
        "the wire payload.)"
    )

    result = TestRunResult(
        total=1,
        passed=1,
        failed=0,
        skipped=0,
        errors=0,
        duration=0.1,
        exit_code=0,
        cases=[],
    )
    actual_keys = set(result.to_xcom().keys())
    print(f"[xcom_contract] to_xcom keys:     {sorted(actual_keys)}")
    assert actual_keys == EXPECTED_KEYS


def test_to_xcom_does_not_include_cases():
    result = TestRunResult(
        total=2,
        passed=1,
        failed=1,
        skipped=0,
        errors=0,
        duration=0.5,
        exit_code=1,
        cases=[
            CaseResult(name="a", classname="m", time=0.1, outcome="passed"),
            CaseResult(
                name="b", classname="m", time=0.4, outcome="failed", message="boom"
            ),
        ],
    )
    payload = result.to_xcom()
    print(f"[xcom_no_cases] payload: {payload}")
    assert "cases" not in payload
    # failed_node_ids IS in the payload, derived from the cases we
    # nonetheless dropped.
    assert payload["failed_node_ids"] == ["m::b"]


def test_to_xcom_is_json_serializable():
    import json

    result = TestRunResult(
        total=3,
        passed=2,
        failed=1,
        skipped=0,
        errors=0,
        duration=0.25,
        exit_code=1,
        cases=[
            CaseResult(
                name="x", classname="y", time=0.1, outcome="failed", message="msg"
            )
        ],
    )
    payload = result.to_xcom()
    roundtripped = json.loads(json.dumps(payload))
    print(f"[xcom_jsonable] roundtripped: {roundtripped}")
    assert roundtripped == payload


def test_to_xcom_is_faster_than_asdict_on_large_suite(capsys):
    import time
    from dataclasses import asdict

    cases = [
        CaseResult(
            name=f"test_method_{i:05d}",
            classname=f"tests.test_module_{i % 100}",
            time=0.001,
            outcome="passed" if i % 7 else "failed",
            message=None if i % 7 else f"failure_{i}",
        )
        for i in range(5000)
    ]
    result = TestRunResult(
        total=len(cases),
        passed=sum(1 for c in cases if c.outcome == "passed"),
        failed=sum(1 for c in cases if c.outcome == "failed"),
        skipped=0,
        errors=0,
        duration=12.34,
        exit_code=1,
        cases=cases,
    )

    # Warm up: import paths, JIT-ish caches, allocator state.
    result.to_xcom()
    _ = asdict(result)

    iters = 20

    t0 = time.perf_counter()
    for _ in range(iters):
        new = result.to_xcom()
    new_total = time.perf_counter() - t0

    def old_to_xcom(r):
        d = asdict(r)
        d.pop("cases", None)
        d["success"] = r.success
        d["failed_node_ids"] = r.failed_node_ids
        return d

    t0 = time.perf_counter()
    for _ in range(iters):
        old = old_to_xcom(result)
    old_total = time.perf_counter() - t0

    speedup = old_total / max(new_total, 1e-9)
    print(
        f"[xcom_perf] cases={len(cases)} iters={iters} "
        f"new_total={new_total * 1000:.1f}ms "
        f"old_total={old_total * 1000:.1f}ms "
        f"speedup={speedup:.1f}x"
    )

    assert new == old
    assert new_total < old_total, (
        f"new to_xcom ({new_total * 1000:.1f}ms) is not faster than the "
        f"old asdict-based one ({old_total * 1000:.1f}ms) -- something "
        "regressed the optimisation."
    )
