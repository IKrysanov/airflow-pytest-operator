"""Example DAG: built-in in-process re-run of only the failed tests.

``rerun_failed=N`` makes the operator run the full suite once and then re-run
ONLY the still-failing tests, up to ``N`` more times, all within one task --
no pytest cache, no XCom between attempts, no ``try_number``. It is robust on
any executor and is the recommended way to absorb flaky failures.

The single task ends up:
  - succeeding if the reruns clear every failure, or
  - failing if some tests still fail after all rounds.
The XCom summary carries ``rerun_rounds``, ``recovered_node_ids`` and
``still_failing_node_ids`` alongside the first full run's counts.
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
    dag_id="rerun_failed_example",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "rerun_failed"],
) as dag:
    PytestOperator(
        task_id="run_tests",
        test_path="/opt/airflow/tests",
        pytest_args=["-q"],
        rerun_failed=2,  # run all once, then re-run only the failures up to 2x
    )
