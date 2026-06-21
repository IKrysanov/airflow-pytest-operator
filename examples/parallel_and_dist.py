"""Example DAG: run a suite in parallel **on the worker** with pytest-xdist.

``parallel`` sets the worker count (``-n``) and ``dist`` selects xdist's
scheduler mode (``--dist``). Both are first-class operator parameters: they are
validated at construction, are applied to the first full run only (so the
in-process ``rerun_failed`` rounds stay serial), and defer to an explicit ``-n``
in ``pytest_args`` if you already drive parallelism yourself.

Install the extra on every worker that runs these tasks:

    pip install "airflow-pytest-operator[xdist]"

Two axes of parallelism:
  * ``parallel`` scales *within* one worker (one Airflow task, N pytest procs).
  * To spread a suite *across* workers/pods, fan ``test_path`` out with dynamic
    task mapping -- see ``mapped_suites.py`` -- and each mapped task can still
    set ``parallel`` to use its own cores.

On the "everything ran on gw0" gotcha: with ``dist="loadscope"`` (or
``loadgroup``) a whole module/class is kept on one worker, so a suite whose
tests share a single scope runs entirely on ``gw0`` by design. Use the default
``"load"`` to spread individual tests across workers.
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
    dag_id="pytest_parallel_and_dist",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["testing", "xdist", "parallel"],
) as dag:
    # Spread individual tests across 4 worker processes (xdist's default
    # "load" scheduler). Good when tests are independent.
    fast = PytestOperator(
        task_id="parallel_load",
        test_path="/opt/airflow/tests",
        parallel=4,  # -> -n 4
        # dist defaults to xdist's "load" when parallel is set.
    )

    # Use one process per core, but keep each module/class together on a single
    # worker -- right when tests share expensive per-module/class fixtures
    # (e.g. a database set up once per module) that must not be split apart.
    by_scope = PytestOperator(
        task_id="parallel_loadscope",
        test_path="/opt/airflow/tests",
        parallel="auto",  # -> -n auto (one worker per CPU)
        dist="loadscope",  # -> --dist loadscope
        # Reruns of just the failures still run serially.
        rerun_failed=1,
    )

    fast >> by_scope
