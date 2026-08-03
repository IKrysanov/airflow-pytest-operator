"""Example DAG: run a data-quality suite behind a failure-tolerance threshold.

``fail_on_test_failure`` is binary -- one failing check out of 500 marks the task
red, or (``False``) the task is always green and the outcome lives only in XCom.
Neither fits a suite used as a data-quality gate, where 2 failing checks may be
acceptable and 50 are not. ``min_pass_rate`` / ``max_failed`` express that
directly:

* ``min_pass_rate`` -- a fraction in ``[0, 1]``, compared against
  ``passed / (total - skipped)``. Skipped checks are OUT of the denominator, so
  a suite that skips what does not apply to today's data is not punished for it.
* ``max_failed``    -- an absolute cap on ``failed + errors``.

Set either or both; when both are set, both must hold. Outside the tolerance the
task fails with ``FailureThresholdError``, whose message names every breached
check. A configured threshold REPLACES ``fail_on_test_failure`` -- including
``fail_on_test_failure=False``, which can no longer keep the task green.

It is fail-closed on the cases where the numbers cannot be trusted: nothing
executed (every check skipped), an abnormal pytest exit (crash, usage error,
nothing collected), or a report whose counters cannot describe a real run. A gate
that cannot be evaluated fails rather than waving the pipeline through.

The XCom summary gains ``pass_rate`` and, on pass, ``threshold_passed``. Note
that ``success`` keeps reporting the SUITE's outcome: a tolerated run is
``success=False`` with ``threshold_passed=True`` -- the checks really did fail,
the task simply accepted it. That is what makes the trend downstream useful.
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
    dag_id="pytest_failure_tolerance_gate",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "data-quality", "quality-gate"],
) as dag:
    # --- The gate: 95% of the checks that ran must pass, and never more than
    # --- 10 failures in absolute terms (a 95% rate on a 10,000-check suite
    # --- would still be 500 broken checks).
    checks = PytestOperator(
        task_id="dq_checks",
        test_path="/opt/airflow/dq_checks",
        min_pass_rate=0.95,  # a fraction: 0.95 for 95%, not 95
        max_failed=10,
        retries=2,  # a retry re-runs the whole suite, so the rate stays the suite's
    )

    # --- If you want retries to re-run only what failed
    # --- (``test_retry_strategy="failed_only"``), drop ``min_pass_rate`` and
    # --- gate on ``max_failed`` alone:
    #
    #     PytestOperator(
    #         task_id="dq_checks",
    #         test_path="/opt/airflow/dq_checks",
    #         max_failed=10,
    #         test_retry_strategy="failed_only",
    #         retries=2,
    #     )
    #
    # A failed_only retry runs exactly the previous attempt's failures, so the
    # pass rate over that run is the rate of a set selected for failing -- not
    # the suite's, and typically far below it, which can keep failing a run that
    # has already recovered. An absolute cap on the still-failing count means
    # the same thing whatever ran, so it composes. The operator logs a warning
    # if it narrows a run while a rate gate is configured.

    def _record_trend(**context: object) -> None:
        ti = context["ti"]
        # xcom_pull returns the RunSummary dict (or None if the task was skipped).
        summary: RunSummary | None = ti.xcom_pull(task_ids="dq_checks")  # type: ignore[attr-defined]
        if not summary:
            print("dq_checks produced no summary")
            return

        rate = summary.get("pass_rate")
        if rate is None:
            # Undefined: nothing executed. With min_pass_rate set the task would
            # already have failed, so reaching this means the gate was max_failed
            # only -- still worth flagging loudly.
            print("pass rate: UNDEFINED -- no check actually ran")
            return

        print(
            f"pass rate: {rate:.2%} "
            f"({summary['passed']} passed, {summary['failed']} failed, "
            f"{summary['errors']} errored, {summary['skipped']} skipped) -- "
            f"tolerated: {summary.get('threshold_passed', False)}"
        )
        # A tolerated-but-red run is the interesting signal: the pipeline is
        # running, but quality is drifting. Record it, alert on the trend.
        if not summary["success"]:
            print("NOTE: checks failed within tolerance -- record the trend")

    trend = PythonOperator(task_id="record_trend", python_callable=_record_trend)

    checks >> trend
