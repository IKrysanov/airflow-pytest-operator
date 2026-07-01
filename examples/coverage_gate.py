"""Example DAG: measure test coverage and gate a pipeline on it.

``coverage=True`` measures line coverage with pytest-cov on the first full run,
logs the coverage table to the task log, and pushes the overall fraction to XCom
under the ``coverage`` key (a float in ``[0, 1]``, or ``None`` when it could not
be read). Install the extra on every worker that runs these tasks:

    pip install "airflow-pytest-operator[coverage]"

Two ways to gate on it:

1. NATIVE gate -- set ``cov_fail_under`` (a fraction in ``[0, 1]``). The operator
   fails the TASK with a clear ``CoverageThresholdError`` when the run is below
   the threshold, and on pass the XCom summary gains ``coverage_passed=True``.
   Simplest option: no extra task, the pipeline stops on low coverage. Test
   failures still take precedence, and it is fail-closed if coverage cannot be
   measured.

2. XCom read -- measure with ``coverage=True`` (no gate) and let a downstream
   task read ``coverage`` from XCom and decide (branch, alert, record a trend).
   Use this when you want the number without necessarily failing the run.

The XCom summary is a :class:`~airflow_pytest_operator.RunSummary` (a
``TypedDict``): ``coverage`` is optional and is ``None`` when unavailable, so
always guard before comparing.

See the README "Coverage" section for what coverage does and does NOT measure --
notably, a REST/system test only covers your local client code, so a low number
there is expected, not a regression.
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
from airflow.operators.python import PythonOperator

from airflow_pytest_operator import PytestOperator, RunSummary

with DAG(
    dag_id="pytest_coverage_gate",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "coverage", "quality-gate"],
) as dag:
    # --- Option 1: native gate. The task itself fails below 80% coverage. ---
    PytestOperator(
        task_id="tests_with_gate",
        test_path="/opt/airflow/tests",
        # A fraction in [0, 1]; use 0.8 for 80%, not 80. Enables coverage
        # measurement automatically (no need to also pass coverage=True).
        cov_fail_under=0.80,
    )

    # --- Option 2: measure only, then decide downstream from XCom. ---
    measure = PytestOperator(
        task_id="tests_measured",
        test_path="/opt/airflow/tests",
        coverage=True,  # measure, but never fail the task on coverage
    )

    def _report_coverage(**context: object) -> None:
        ti = context["ti"]
        # xcom_pull returns the RunSummary dict (or None if the task was skipped).
        summary: RunSummary | None = ti.xcom_pull(task_ids="tests_measured")  # type: ignore[attr-defined]
        cov = summary.get("coverage") if summary else None
        if cov is None:
            print("coverage: unavailable (no terminal total parsed)")
        else:
            print(f"coverage: {cov:.0%} -- {'OK' if cov >= 0.80 else 'BELOW 80%'}")

    report = PythonOperator(
        task_id="report_coverage",
        python_callable=_report_coverage,
    )

    measure >> report
