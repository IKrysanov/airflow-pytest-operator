"""Example DAG: shard ONE suite across workers with dynamic task mapping.

This is the **second axis** of parallelism (the first is ``parallel=`` / xdist,
which scales *within* one worker). Here the suite is split *across* several
Airflow tasks/pods:

  1. a ``collect`` task does a collect-only run through the library's
     ``SubprocessPytestRunner`` (the operator's own engine -- the same
     ``--collect-only`` pass as ``dry_run=True``) and reads the node-ids from
     its stdout (:func:`parse_collect_only_output`);
  2. a ``partition`` task splits those ids into N balanced, contiguous groups
     (:func:`partition_node_ids`);
  3. ``PytestOperator.partial(...).expand(test_path=<groups>)`` runs one mapped
     shard per group -- each on its own worker/pod, each independent.

The two axes are orthogonal and compose: every shard below also sets
``parallel="auto"``, so it xdists *within* its worker. Total processes are
roughly ``num_shards * (cores per worker)`` -- mind your pools / concurrency.

``failed_only`` composes too: the last-failed Variable key includes
``map_index``, so a retry of shard 2 re-runs only *shard 2's* previous
failures, never another shard's (see ``mapped_suites.py`` and the README).

Needs ``pytest`` (and, for the inner axis, the ``[xdist]`` extra) on every
worker. The collect task goes through the operator's runner, so it resolves the
worker's interpreter, cwd and env exactly like the shard tasks do.
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
from airflow.decorators import task

from airflow_pytest_operator import (
    PytestOperator,
    SubprocessPytestRunner,
    parse_collect_only_output,
    partition_node_ids,
)
from airflow_pytest_operator.reporters import JUnitResultParser

SUITE = "/opt/airflow/tests"
NUM_SHARDS = 4

with DAG(
    dag_id="pytest_sharded_mapped",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "dynamic-task-mapping", "sharding", "parallel"],
) as dag:

    @task
    def collect(suite: str) -> list[str]:
        """Collect node-ids via the operator's runner (no test bodies run).

        A collect-only run -- the same pass the operator does for
        ``dry_run=True`` -- driven through ``SubprocessPytestRunner`` so it uses
        the worker's interpreter, cwd and env exactly like the shards. We add
        ``-q`` and read the ids from stdout, since ``dry_run``'s XCom summary
        reports counts, not the id list. The report_request is required by the
        runner; collect-only writes no usable report, so we only read stdout.
        """
        artifacts = SubprocessPytestRunner().run(
            suite,
            pytest_args=["--collect-only", "-q"],
            report_request=JUnitResultParser().report_request,
        )
        node_ids = parse_collect_only_output(artifacts.stdout or "")
        if not node_ids:
            # Surface a collection error instead of silently expanding into zero
            # shards.
            raise RuntimeError(
                f"collected no tests from {suite!r} (exit {artifacts.exit_code}):\n"
                f"{(artifacts.stderr or '')[-2000:]}"
            )
        return node_ids

    @task
    def into_shards(node_ids: list[str]) -> list[list[str]]:
        """Split collected ids into up to NUM_SHARDS balanced groups."""
        return partition_node_ids(node_ids, NUM_SHARDS)

    # One mapped PytestOperator instance per shard. Each gets its group of
    # node-ids as ``test_path`` and xdists within its worker.
    run_shards = PytestOperator.partial(
        task_id="run_shard",
        parallel="auto",  # inner axis: xdist within each shard's worker
        test_retry_strategy="failed_only",  # per-shard, keyed by map_index
        retries=2,
        retry_delay=timedelta(seconds=30),
    ).expand(test_path=into_shards(collect(SUITE)))
