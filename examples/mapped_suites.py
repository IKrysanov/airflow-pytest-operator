"""Example DAG: fan one PytestOperator out over several test suites with
Airflow **dynamic task mapping** (``.expand``).

``PytestOperator.partial(...).expand(test_path=[...])`` creates **one mapped
task instance per target**, each with its own ``map_index`` (0, 1, 2, ...). The
suites run in parallel (subject to your pool/concurrency limits), and each
mapped instance is independent.

Why ``map_index`` matters here: with ``test_retry_strategy="failed_only"`` each
mapped instance carries its failed node-ids in an Airflow Variable keyed by
``(dag_id, task_id, run_id, map_index)``. The ``map_index`` is what keeps the
siblings from clobbering each other's failed set -- so a retry of suite 0 only
re-runs *suite 0's* previously-failed tests, never suite 1's. Without it all
map indices would share one key and overwrite each other.

See the README "Retry strategy" section for the non-mapped alternatives
(``rerun_failed``, the two-task ``run-all -> run-failed`` pattern).
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

from datetime import timedelta

import pendulum
from airflow import DAG

from airflow_pytest_operator import PytestOperator

# One mapped instance per entry. Each runs its own suite and, on a retry,
# narrows to only its OWN previously-failed tests (failed_only + map_index).
TEST_SUITES = [
    "/opt/airflow/tests/unit",
    "/opt/airflow/tests/integration",
    "/opt/airflow/tests/smoke",
]

with DAG(
    dag_id="pytest_mapped_suites",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "dynamic-task-mapping", "failed_only"],
) as dag:
    # .partial() holds the args shared by every mapped instance; .expand()
    # maps `test_path` across the list, producing one task instance (map_index
    # 0..N-1) per suite.
    run_suites = PytestOperator.partial(
        task_id="run_suite",
        pytest_args=["-q"],
        retries=2,  # Airflow retries; each map_index retries only its own failures
        # Override Airflow's 5-minute default so the narrowing retries kick in
        # promptly instead of parking each mapped instance in up_for_retry.
        retry_delay=timedelta(seconds=30),
        test_retry_strategy="failed_only",
    ).expand(test_path=TEST_SUITES)

# Tip: to discover the suites at runtime instead of hard-coding them, expand
# over an upstream task's return value (a list) -- the mapping (and its
# per-map_index failed_only scoping) works exactly the same:
#
#     from airflow.decorators import task
#
#     @task
#     def list_suites() -> list[str]:
#         return ["/opt/airflow/tests/unit", "/opt/airflow/tests/integration"]
#
#     PytestOperator.partial(
#         task_id="run_suite",
#         test_retry_strategy="failed_only",
#     ).expand(test_path=list_suites())
