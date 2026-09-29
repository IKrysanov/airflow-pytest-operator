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

"""DAGs the e2e tests run with ``dag.test()`` against a real Airflow.

Written the way a user would write them. Each run gets its throwaway pytest
suite through ``dag_run.conf["suite"]``, so every DAG also exercises Jinja
templating of ``test_path``. This folder is the DAGs folder of those runs:
Airflow 3.3's ``dag.test()`` syncs the DAG from its bundle first.
"""

from __future__ import annotations

import datetime

from airflow_pytest_operator import PytestOperator, partition_node_ids

try:  # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task

SUITE = "{{ dag_run.conf['suite'] }}"
_DAG_ARGS = {
    "schedule": None,
    "start_date": datetime.datetime(2026, 1, 1),
    "catchup": False,
}
_RETRY_NOW = {"retries": 1, "retry_delay": datetime.timedelta(0)}


with DAG("apo_e2e_basic", **_DAG_ARGS) as basic:
    PytestOperator(
        task_id="run",
        test_path=SUITE,
        env={"APO_E2E_GREETING": "{{ dag_run.conf['greeting'] }}"},
    )

with DAG("apo_e2e_failing", **_DAG_ARGS) as failing:
    PytestOperator(task_id="run", test_path=SUITE)

with DAG("apo_e2e_failed_only", **_DAG_ARGS) as failed_only:
    PytestOperator(
        task_id="run",
        test_path=SUITE,
        test_retry_strategy="failed_only",
        **_RETRY_NOW,
    )

with DAG("apo_e2e_sharded", **_DAG_ARGS) as sharded:

    @task
    def shards(**context):
        suite = context["dag_run"].conf["suite"]
        names = ["stable_a", "stable_b", "stable_c", "flaky"]
        return partition_node_ids([f"{suite}/test_s.py::test_{n}" for n in names], 2)

    PytestOperator.partial(
        task_id="run", test_retry_strategy="failed_only", **_RETRY_NOW
    ).expand(test_path=shards())

with DAG("apo_e2e_timeout", **_DAG_ARGS) as timeout:
    PytestOperator(
        task_id="run",
        test_path=SUITE,
        execution_timeout=datetime.timedelta(seconds=10),
    )
