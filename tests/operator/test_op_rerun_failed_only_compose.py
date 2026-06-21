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


"""rerun_failed + failed_only compose: in-process reruns run first; only the tests
STILL failing after them are carried to the next Airflow retry. Shared fakes in
_op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeRunner,
    FakeStore,
    SequenceParser,
    _ctx,
    _key,
    _res,
)

from airflow_pytest_operator.exceptions import TestsFailedError
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_rerun_failed_and_failed_only_write_post_rerun_set_forward():
    # First attempt (empty store) with both features on. The full run fails two
    # tests; one in-process rerun recovers one of them. The task still fails, so
    # the failed_only Variable is handed to the next Airflow retry -- and it must
    # contain ONLY the post-rerun survivor, not both original failures.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            # Full run: a and b fail.
            _res(["tests.test_x::test_a", "tests.test_y::test_b"], passed=3),
            # In-process rerun: a recovers, b still fails.
            _res(["tests.test_y::test_b"], passed=1),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=1,
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    # try_number (1) <= max_tries (2): not final, so the survivor is written
    # forward. b never recovered, so the task itself still fails.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[rerun+failed_only] calls={len(runner.calls)} writes={store.writes}")
    # Two pytest invocations: the full run, then one in-process rerun narrowed
    # to the converted failed selectors.
    assert len(runner.calls) == 2
    assert runner.calls[0]["test_path"] == "tests/"
    assert runner.calls[1]["test_path"] == [
        "tests/test_x.py::test_a",
        "tests/test_y.py::test_b",
    ]
    # The crux: the next retry inherits only the post-rerun survivor (b), so it
    # won't waste time re-running a, which the in-process rerun already fixed.
    assert store.writes == [(key, ["tests.test_y::test_b"])]
    assert store.data[key] == ["tests.test_y::test_b"]
    # First attempt: the store was read once but held nothing to consume.
    assert store.reads == [key]
    assert store.deletes == []
