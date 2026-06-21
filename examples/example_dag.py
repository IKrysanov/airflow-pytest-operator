"""Example DAG: a smoke suite, a marker/keyword-selected suite, then a fuller
suite that only reports."""

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
from airflow_pytest_operator.reporters import JSONResultParser

# Use additional features like custom runners and parsers by importing them directly.
from airflow_pytest_operator.runners import SubprocessPytestRunner

with DAG(
    dag_id="pytest_example",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="30 9 * * *",
    catchup=False,
    tags=["testing", "demo"],
) as dag:
    # Hard gate: fail the pipeline if smoke tests fail.
    smoke = PytestOperator(
        task_id="smoke",
        test_path="/opt/airflow/tests",
        pytest_args=["-k", "smoke", "-x", "-q"],
        fail_on_test_failure=True,
        do_xcom_push=False,
    )

    # Select tests with the markers= / keyword= sugar instead of hand-writing
    # -m / -k in pytest_args -- more discoverable, and both are templated so
    # you can drive them from the DAG run conf. Equivalent here to
    # pytest_args=["-m", "api and not slow", "-k", "login or logout"].
    selected = PytestOperator(
        task_id="selected",
        test_path="/opt/airflow/tests",
        markers="api and not slow",  # -> -m "api and not slow"
        keyword="login or logout",  # -> -k "login or logout"
        # e.g. markers="{{ dag_run.conf.get('markers', 'api') }}" to pick at runtime
        fail_on_test_failure=True,
    )

    # Soft run: never fails the task, just records results in XCom.
    full = PytestOperator(
        task_id="full_report_only",
        test_path="/opt/airflow/tests",
        pytest_args=["-v", "-s", "--cache-clear"],
        fail_on_test_failure=False,
        env={"EXAMPLE_ENV_VAR": "example_value"},
        # Load more vars from a .env (needs the [dotenv] extra on the worker:
        # pip install "airflow-pytest-operator[dotenv]"). Precedence:
        # os.environ < env_file < env, so EXAMPLE_ENV_VAR above wins on a clash.
        # env_file_overrides=False keeps the file from touching AIRFLOW* keys.
        env_file="/opt/airflow/tests/.envs/env-v1",
        env_file_overrides=False,
        # You can use any runner that implements the expected interface; the default is SubprocessPytestRunner.
        runner=SubprocessPytestRunner(timeout=1800, cleanup="on_success"),
        # You can use any parser that implements the expected interface; the default is JUnitResultParser.
        parser=JSONResultParser(),
        do_xcom_push=True,  # This is the default, but being explicit for clarity.
    )

    smoke >> selected >> full
