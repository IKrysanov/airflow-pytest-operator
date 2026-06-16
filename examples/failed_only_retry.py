"""Example DAG: single-operator "retry only failed" via Airflow retries.

``test_retry_strategy="failed_only"`` makes a *single* PytestOperator, driven
by Airflow's own ``retries=``, re-run only the tests that failed on the
previous attempt. After each attempt the failing node-ids are saved in an
Airflow Variable keyed by ``(dag_id, task_id, run_id)``; on the next retry they
are converted back to pytest selectors and run in place of the full suite. The
Variable is deleted once no further retry will read it (on success, or on the
final attempt even if it failed).

A Variable -- not the task's own XCom -- is used because Airflow clears a task
instance's XCom at the start of every retry, and a Variable survives the task's
own retries and works identically on Airflow 2.x and 3.x.

  attempt 1: run the full suite        -> 3 of 500 fail, ids saved
  attempt 2: run only those 3          -> 1 still fails, id saved
  attempt 3: run only that 1           -> passes -> task succeeds, Variable gone

See the README "Retry strategy" section for two alternatives: ``rerun_failed``
(in-process reruns within one task, no Airflow retry and no store) and the
two-task ``run-all -> run-failed`` XCom pattern.
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

from airflow_pytest_operator import PytestOperator

with DAG(
    dag_id="failed_only_retry_example",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "retry", "failed_only"],
) as dag:
    PytestOperator(
        task_id="run_tests",
        test_path="/opt/airflow/tests",
        pytest_args=["-q"],
        retries=2,  # Airflow re-runs the task; each retry narrows to the failures
        test_retry_strategy="failed_only",
    )
