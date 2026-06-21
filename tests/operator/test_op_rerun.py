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


"""rerun_failed: in-process re-runs of ONLY the failed tests (no pytest cache, no
XCom, no Airflow retry). Shared fakes in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeRunner,
    SequenceParser,
    _ctx,
    _res,
)

from airflow_pytest_operator.exceptions import TestsFailedError
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_rerun_failed_default_is_zero():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[rerun:default] rerun_failed={op.rerun_failed}")
    assert op.rerun_failed == 0


def test_rerun_failed_negative_raises_value_error():
    # Right type, wrong value -> ValueError (Python convention).
    with pytest.raises(ValueError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=-1)


def test_rerun_failed_bool_raises_type_error():
    # bool is an int subclass; True must not slip through as a count. A wrong
    # *type* is a TypeError, not a ValueError.
    with pytest.raises(TypeError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=True)


def test_rerun_failed_non_int_raises_type_error():
    # 2.5 would otherwise blow up later at range(self.rerun_failed); reject the
    # wrong type up front with a TypeError.
    with pytest.raises(TypeError, match="rerun_failed"):
        PytestOperator(task_id="t", test_path="tests/", rerun_failed=2.5)


def test_rerun_failed_zero_does_not_rerun_even_with_failures():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser([_res(["tests.test_x::test_a"], passed=2)])
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    out = op.execute(_ctx())
    print(f"[rerun:zero] calls={len(runner.calls)} out={out}")
    assert len(runner.calls) == 1  # one pytest run, no reruns
    assert "rerun_rounds" not in out  # summary unchanged for rerun_failed=0
    assert out["failed"] == 1


def test_rerun_failed_recovers_all_makes_task_succeed():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=3),
            _res([], passed=2),  # rerun: both recovered
        ]
    )
    op = PytestOperator(
        task_id="t", test_path="tests/", rerun_failed=2, runner=runner, parser=parser
    )

    out = op.execute(_ctx())  # must NOT raise -- reruns recovered everything
    print(f"[rerun:recovered] out={out}")

    assert out["success"] is True
    assert out["rerun_rounds"] == 1  # stopped early once all passed
    assert sorted(out["recovered_node_ids"]) == [
        "tests.test_x::test_a",
        "tests.test_x::test_b",
    ]
    assert out["still_failing_node_ids"] == []
    # First run on the full path; second run on the converted failed selectors.
    assert runner.calls[0]["test_path"] == "tests/"
    assert runner.calls[1]["test_path"] == [
        "tests/test_x.py::test_a",
        "tests/test_x.py::test_b",
    ]


def test_rerun_failed_partial_recovery_fails_task():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=3),
            _res(["tests.test_x::test_b"], passed=1),
            _res(["tests.test_x::test_b"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t", test_path="tests/", rerun_failed=2, runner=runner, parser=parser
    )

    with pytest.raises(TestsFailedError):
        op.execute(_ctx())
    assert len(runner.calls) == 3  # full run + 2 reruns


def test_rerun_failed_partial_recovery_summary_when_not_failing_task():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a", "tests.test_x::test_b"], passed=3),
            _res(["tests.test_x::test_b"], passed=1),
            _res(["tests.test_x::test_b"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=2,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )

    out = op.execute(_ctx())
    print(f"[rerun:partial] out={out}")
    assert out["success"] is False
    assert out["rerun_rounds"] == 2
    assert out["recovered_node_ids"] == ["tests.test_x::test_a"]
    assert out["still_failing_node_ids"] == ["tests.test_x::test_b"]
    # XCom keeps the first full run's counts (honest picture of the suite).
    assert out["total"] == 5
    assert out["failed"] == 2


def test_rerun_failed_no_failures_no_reruns():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser([_res([], passed=5)])
    op = PytestOperator(
        task_id="t", test_path="tests/", rerun_failed=3, runner=runner, parser=parser
    )
    out = op.execute(_ctx())
    assert len(runner.calls) == 1
    assert "rerun_rounds" not in out
    assert out["success"] is True


def test_rerun_failed_ignored_in_dry_run():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser([_res(["tests.test_x::test_a"], passed=0)])
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=2,
        dry_run=True,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())
    print(
        f"[rerun:dry_run] calls={len(runner.calls)} args={runner.calls[0]['pytest_args']}"
    )
    assert len(runner.calls) == 1  # no reruns in dry-run
    assert runner.calls[0]["pytest_args"][-1] == "--collect-only"


def test_rerun_failed_cleans_report_dir_between_rounds():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=1),
            _res(["tests.test_x::test_a"], passed=0),
            _res(["tests.test_x::test_a"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=2,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())
    print(f"[rerun:cleanup] cleanup_calls={runner.cleanup_calls}")
    # cleanup(False) before each of the 2 reruns + final cleanup(False).
    assert runner.cleanup_calls == [False, False, False]
