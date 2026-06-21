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

"""
Example DAG: build a daily sales data mart with a pytest quality gate.

A typical data-mart pipeline has three stages: **extract -> validate -> load**.
The validation stage is what stops a bad load from corrupting the mart -- this
example shows how to wire ``PytestOperator`` in as that gate.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from airflow_pytest_operator import PytestOperator

# Where extract/validate hand off the staged rows. In production this would be
# a shared volume, S3, or a staging table -- whatever the worker pool can read
# from the same path. Kept under /tmp here so the example runs on a fresh
# Airflow without extra setup.
STAGING_DIR = "/tmp/data_mart_pipeline"
ORDERS_STAGING_PATH = f"{STAGING_DIR}/orders_{{{{ ds }}}}.json"

# Where the data-quality test suite lives on the worker. Point this at your
# own tests/ folder (synced via git-sync, baked into the image, or mounted
# from a volume -- same as your DAG files).
TESTS_PATH = "/opt/airflow/tests/data_quality"


def extract_orders_from_source(ds: str, **_: Any) -> dict[str, Any]:
    """Emulate pulling the day's orders out of an operational database.

    In a real pipeline this would issue a SQL query against the source DB
    (Postgres, MySQL, ClickHouse, ...). Here we synthesise a small batch
    deterministically from the run's logical date so the example is
    reproducible without external infrastructure.

    Returns a small XCom summary -- the staging path and the row count --
    so downstream tasks (the pytest validator, the loader) can find the
    file without hard-coding the path twice.
    """
    os.makedirs(STAGING_DIR, exist_ok=True)
    staging_path = f"{STAGING_DIR}/orders_{ds}.json"

    # Simulated daily batch. Includes one deliberately-bad row (negative
    # amount) so the validation step has something to catch when you first
    # run the example -- delete it to see the green-path branch fire.
    rows = [
        {
            "order_id": 1001,
            "customer_id": 42,
            "amount": 199.90,
            "order_ts": f"{ds}T08:14:00Z",
        },
        {
            "order_id": 1002,
            "customer_id": 17,
            "amount": 49.00,
            "order_ts": f"{ds}T09:02:11Z",
        },
        {
            "order_id": 1003,
            "customer_id": 88,
            "amount": 1250.00,
            "order_ts": f"{ds}T11:47:55Z",
        },
        # bad: refund leaked as a negative order
        {
            "order_id": 1004,
            "customer_id": 42,
            "amount": -10.00,
            "order_ts": f"{ds}T12:03:00Z",
        },
        {
            "order_id": 1005,
            "customer_id": 5,
            "amount": 320.50,
            "order_ts": f"{ds}T13:20:08Z",
        },
    ]

    with open(staging_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"[extract] Wrote {len(rows)} rows to {staging_path}")
    return {"staging_path": staging_path, "row_count": len(rows), "ds": ds}


def load_orders_to_mart(**context: Any) -> dict[str, Any]:
    """Emulate the upsert into the target mart table.

    Reads the staged file written by the extract step (path travels through
    XCom). In a real pipeline this would be e.g.
    ``COPY ... FROM 's3://stage/...'`` into Snowflake, an ``INSERT ... ON
    CONFLICT`` into Postgres, or a ``MERGE`` into BigQuery. The retry semantics
    you want depend on whether your loader is idempotent -- for an upsert keyed
    by ``order_id`` like this one, replaying is safe.
    """
    extract_summary = context["ti"].xcom_pull(task_ids="extract_orders_from_source")
    staging_path = extract_summary["staging_path"]

    with open(staging_path, encoding="utf-8") as f:
        rows = json.load(f)

    # Pretend this is an UPSERT into mart.fact_orders.
    print(f"[load] Upserting {len(rows)} validated rows into mart.fact_orders")
    for row in rows:
        print(f"[load]   order_id={row['order_id']} amount={row['amount']}")

    return {"rows_loaded": len(rows), "target_table": "mart.fact_orders"}


def send_quality_alert(**context: Any) -> dict[str, Any]:
    """Emulate notifying on-call that the day's batch failed validation.

    Reads the pytest summary out of XCom -- it carries ``failed_node_ids``
    in the same dotted form regardless of which parser ran, so the alert can
    name the specific failing checks. In a real pipeline this would be a
    Slack webhook, a PagerDuty event, an email, or all three.
    """
    summary = context["ti"].xcom_pull(task_ids="validate_orders") or {}
    failed_ids = summary.get("failed_node_ids", []) or []
    still_failing = summary.get("still_failing_node_ids") or failed_ids

    print(
        "[alert] Data quality gate FAILED for "
        f"ds={context['ds']!r}: "
        f"{summary.get('failed', '?')} failed / {summary.get('total', '?')} total. "
        "Mart load skipped."
    )
    for nid in still_failing:
        print(f"[alert]   failing check: {nid}")
    return {"alerted": True, "failed_checks": list(still_failing)}


with DAG(
    dag_id="daily_sales_mart_pipeline",
    description=(
        "Extract orders -> validate with pytest -> branch on quality -> "
        "load to mart OR alert."
    ),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["data-mart", "etl", "data-quality", "pytest"],
) as dag:
    # 1) Extract: emulates a read from the operational DB.
    extract = PythonOperator(
        task_id="extract_orders_from_source",
        python_callable=extract_orders_from_source,
    )

    # 2) Validate: run the data-quality test suite against the staged file.
    #
    # fail_on_test_failure=False lets the task succeed even when tests fail,
    # so the XCom summary stays available for the branch task below. Without
    # it the branch would never run on the failure path.
    #
    # To absorb flaky checks without paging on-call, add rerun_failed=N (an
    # in-process re-run of only the failures) or test_retry_strategy="failed_only"
    # with Airflow retries -- see the README "Retry strategy" section.
    validate = PytestOperator(
        task_id="validate_orders",
        test_path=TESTS_PATH,
        pytest_args=["-q", "--tb=short"],
        # Per-value Jinja template: each env var is its own string, NOT the
        # whole env dict (see the README "Passing values ..." section).
        env={
            "ORDERS_STAGING_PATH": (
                "{{ ti.xcom_pull(task_ids='extract_orders_from_source')"
                "['staging_path'] }}"
            ),
            "BATCH_DS": "{{ ds }}",
        },
        fail_on_test_failure=False,
    )

    # 3) Branch on the validation outcome. Reading XCom is what makes the
    #    decision data-driven -- the branch sees the same summary the alert
    #    task will quote, so there's a single source of truth.
    @task.branch(task_id="quality_gate")
    def quality_gate(**context: Any) -> str:
        summary = context["ti"].xcom_pull(task_ids="validate_orders") or {}
        ok = bool(summary.get("success"))
        print(
            f"[gate] validate_orders summary: success={ok} "
            f"passed={summary.get('passed')} "
            f"failed={summary.get('failed')} "
            f"errors={summary.get('errors')}"
        )
        return "load_to_mart" if ok else "send_quality_alert"

    gate = quality_gate()

    # 4a) Happy path: load the validated rows into the mart.
    load = PythonOperator(
        task_id="load_to_mart",
        python_callable=load_orders_to_mart,
    )

    # 4b) Sad path: alert on-call. Use the same `python_callable` shape as
    #     the load so swapping in a real Slack / email hook is one edit.
    alert = PythonOperator(
        task_id="send_quality_alert",
        python_callable=send_quality_alert,
    )

    extract >> validate >> gate >> [load, alert]


# -----------------------------------------------------------------------------
# Sketch of what the matching data-quality tests look like. Put this file at
# the ``TESTS_PATH`` above on the worker -- the operator runs real pytest, so
# any pytest idiom (fixtures, parametrize, marks) works:
#
#     # tests/data_quality/test_orders.py
#     import json
#     import os
#     from datetime import datetime, timezone
#
#     import pytest
#
#     @pytest.fixture(scope="module")
#     def orders():
#         with open(os.environ["ORDERS_STAGING_PATH"], encoding="utf-8") as f:
#             return json.load(f)
#
#     def test_primary_key_is_unique(orders):
#         ids = [r["order_id"] for r in orders]
#         assert len(ids) == len(set(ids)), "duplicate order_id in batch"
#
#     def test_required_columns_are_not_null(orders):
#         for r in orders:
#             assert r.get("order_id") is not None
#             assert r.get("customer_id") is not None
#             assert r.get("amount") is not None
#
#     def test_amount_is_non_negative(orders):
#         bad = [r["order_id"] for r in orders if r["amount"] < 0]
#         assert not bad, f"orders with negative amount: {bad}"
#
#     def test_batch_is_for_the_expected_date(orders):
#         ds = os.environ["BATCH_DS"]
#         for r in orders:
#             ts = datetime.fromisoformat(r["order_ts"].replace("Z", "+00:00"))
#             assert ts.date().isoformat() == ds, (
#                 f"order {r['order_id']} ts {ts.isoformat()} outside batch {ds}"
#             )
# -----------------------------------------------------------------------------
