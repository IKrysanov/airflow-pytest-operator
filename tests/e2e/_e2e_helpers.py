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

"""Shared helpers for the e2e tests. Airflow is imported lazily: the fixtures
skip before any of these run where it is absent."""

from __future__ import annotations

from typing import Any

# A test that fails on its first run only; the marker file lives next to it.
FLAKY_TEST = """
import pathlib

def test_flaky():
    marker = pathlib.Path(__file__).with_name("flaky.seen")
    runs = int(marker.read_text()) if marker.exists() else 0
    marker.write_text(str(runs + 1))
    assert runs, "first run fails"
"""


def run_dag(dag: Any, **conf: Any) -> Any:
    """``dag.test()`` with ``conf`` as ``dag_run.conf``; the finished DagRun."""
    try:
        return dag.test(run_conf=conf)
    except BaseException as exc:
        # Airflow 2's dag.test() lets AirflowTaskTimeout -- a BaseException
        # there -- escape instead of recording the failed task.
        if type(exc).__name__ != "AirflowTaskTimeout":
            raise
    from airflow.models import DagRun

    return DagRun.find(dag_id=dag.dag_id)[-1]


def task_instances(dagrun: Any, task_id: str = "run") -> list[Any]:
    tis = [ti for ti in dagrun.get_task_instances() if ti.task_id == task_id]
    return sorted(tis, key=lambda ti: ti.map_index)


def summary(ti: Any) -> Any:
    """The RunSummary this task instance pushed to XCom, or None."""
    return ti.xcom_pull(task_ids=ti.task_id, map_indexes=ti.map_index)


def failed_only_keys() -> set[str]:
    """Keys of every failed_only Variable in the metadata DB."""
    from airflow.models.variable import Variable
    from airflow.utils.session import create_session
    from sqlalchemy import select

    with create_session() as session:
        query = select(Variable.key).where(Variable.key.like("apo_last_failed__%"))
        return set(session.scalars(query))
