"""Example DAG: run a smoke suite, then a fuller suite that only reports."""

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
    dag_id="pytest_example",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing"],
) as dag:
    # Hard gate: fail the pipeline if smoke tests fail.
    smoke = PytestOperator(
        task_id="smoke",
        test_path="/opt/airflow/tests",
        pytest_args=["-k", "smoke", "-x", "-q"],
        fail_on_test_failure=True,
    )

    # Soft run: never fails the task, just records results in XCom.
    full = PytestOperator(
        task_id="full_report_only",
        test_path="/opt/airflow/tests",
        pytest_args=["-q"],
        fail_on_test_failure=False,
    )

    smoke >> full
