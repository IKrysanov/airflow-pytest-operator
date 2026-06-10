"""Example DAG: robust "retry only failed" across any executor.

Instead of pytest's worker-local ``.pytest_cache`` (the operator-level
``test_retry_strategy="failed_only"`` / ``--lf`` option, which is best-effort
and degrades to a full run on a fresh worker), this pattern carries the failed
test ids through Airflow XCom (the metadata DB) from a first "run all" task to a
second "run failed" task.

Because the second task reads a *different* task's XCom, Airflow never clears it
(it only clears a task's own XCom on that task's own retry). So the rerun is
reliable on Kubernetes/Celery, survives a worker or pod dying between tasks, and
does not race with parallel tasks over a shared cache.

    run_all (all tests; never fails the task; records failures in XCom)
      -> has_failures? (short-circuit: stop if nothing failed)
      -> to_selectors  (dotted failed_node_ids -> pytest CLI selectors)
      -> run_failed    (only the previously-failed tests; fails the pipeline)
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

import pendulum
from airflow import DAG
from airflow.decorators import task

from airflow_pytest_operator import PytestOperator, failed_selectors
from airflow_pytest_operator.runners import SubprocessPytestRunner

# The pytest rootdir (where pytest.ini / pyproject.toml lives). The second task
# runs from here so the relative selectors from failed_selectors resolve.
ROOTDIR = "/opt/airflow"
TESTS = "/opt/airflow/tests"

with DAG(
    dag_id="retry_failed_dag_pattern",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "retry", "failed_only"],
) as dag:
    # 1) Run the whole suite. fail_on_test_failure=False so the task SUCCEEDS
    #    and pushes its summary (including failed_node_ids) to XCom.
    run_all = PytestOperator(
        task_id="run_all",
        test_path=TESTS,
        pytest_args=["-q"],
        fail_on_test_failure=False,
    )

    # 2) Stop the branch if there is nothing to re-run.
    @task.short_circuit
    def has_failures(summary: dict) -> bool:
        return bool(failed_selectors(summary))

    # 3) Turn the XCom summary into pytest selectors for the failed tests.
    @task
    def to_selectors(summary: dict) -> list[str]:
        return failed_selectors(summary)

    summary = run_all.output
    selectors = to_selectors(summary)

    # 4) Re-run ONLY the previously-failed tests, and fail the pipeline if they
    #    still fail. Give this task its own retries for several rounds if you
    #    like -- they stay reliable because the ids live in run_all's XCom.
    run_failed = PytestOperator(
        task_id="run_failed",
        test_path=selectors,
        pytest_args=["-q"],
        fail_on_test_failure=True,
        retries=2,
        runner=SubprocessPytestRunner(cwd=ROOTDIR),
    )

    run_all >> has_failures(summary) >> selectors >> run_failed
