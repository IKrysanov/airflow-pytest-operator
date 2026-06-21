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


"""Collaborator safety: an injected runner/store that violates the "never raise"
contract must not mask execute()'s real outcome. Shared fakes in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    FakeStore,
    _ctx,
    _DeleteExplodingStore,
    _ExplodingCleanupRunner,
    _ExplodingStore,
    _key,
    _res,
    _result,
)

from airflow_pytest_operator.exceptions import TestsFailedError
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_cleanup_error_does_not_mask_tests_failed_error():
    runner = _ExplodingCleanupRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(failed=2)),
    )
    # The genuine TestsFailedError wins -- not the cleanup RuntimeError.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx())
    assert runner.cleanup_calls == [False]  # cleanup was attempted


def test_cleanup_error_does_not_mask_success_summary():
    runner = _ExplodingCleanupRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=2)),
    )
    out = op.execute(_ctx())  # cleanup error swallowed
    assert out["success"] is True


def test_store_errors_do_not_break_failed_only_run():
    # read/delete/write all raise; the operator degrades to the full suite and
    # the real outcome (TestsFailedError) still surfaces.
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=0)),
        store=_ExplodingStore(),
    )
    # read raises -> degrades to full suite; write raises at the end -> swallowed.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    # The full suite ran (read failure did not narrow it).
    assert runner.calls[0]["test_path"] == "tests/"


def test_store_delete_error_during_consume_does_not_break_run():
    # delete raises while consuming the Variable on read; it's swallowed, the
    # narrowed run still proceeds and the task succeeds.
    key = _key()
    store = _DeleteExplodingStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=FakeParser(_res([], passed=1)),  # the narrowed run passes
        store=store,
    )
    out = op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    assert out["success"] is True
    # It narrowed to the stored failures despite the delete error.
    assert runner.calls[0]["test_path"] == ["tests/test_x.py::test_a"]


def test_failed_only_warns_when_final_attempt_undeterminable():
    # No max_tries on the ti -> is_final_attempt can't decide -> the operator
    # writes forward AND logs a warning to the task log about a possible orphan.
    from unittest import mock

    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=FakeParser(_res(["tests.test_x::test_a"], passed=0)),
        store=store,
    )
    with mock.patch.object(op.log, "warning") as warning:
        # try_number present but no max_tries -> undeterminable. Default
        # fail_on_test_failure=True -> the attempt raises and writes forward.
        with pytest.raises(TestsFailedError):
            op.execute(_ctx(try_number=9, dag_id="d", task_id="t", run_id="r"))

    logged = " ".join(str(c) for c in warning.call_args_list)
    print(f"[final_undeterminable] warnings={logged!r}")
    assert "final attempt" in logged
    # It still wrote the failures forward (treated as non-final).
    assert store.writes == [(_key(), ["tests.test_x::test_a"])]
