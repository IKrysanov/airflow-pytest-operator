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

"""PytestOperator inside a real Airflow: the DAGs in ``dags/`` run end to end
with ``dag.test()``. What the unit suite fakes -- template rendering, the XCom
backend, real retries (try_number / max_tries), dynamic task mapping, the
Variable backend, execution_timeout -> on_kill -- happens for real here.

Everything skips unless Airflow and an initialised metadata DB are present: the
unit job stubs Airflow, so these run in the integration job (after ``airflow db
migrate``) or locally against an initialised ``AIRFLOW_HOME``. The fixtures
live here rather than in a conftest.py, which would shadow the top-level
``conftest`` module that other tests import from.
"""

from __future__ import annotations

import importlib.util
import os
import textwrap
import time
from pathlib import Path
from typing import NoReturn

import pytest
from _e2e_helpers import FLAKY_TEST, failed_only_keys, run_dag, summary, task_instances

from airflow_pytest_operator.stores import last_failed_var_key

DAGS_DIR = Path(__file__).parent / "dags"


def _unavailable(reason: str) -> NoReturn:
    # The integration job sets APO_REQUIRE_E2E: there a skip would be a silent
    # false green, so it fails instead.
    if os.environ.get("APO_REQUIRE_E2E"):
        pytest.fail(f"e2e tests required but cannot run: {reason}")
    pytest.skip(reason)


@pytest.fixture(scope="module")
def _airflow_db():
    try:
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        from sqlalchemy import select
    except ImportError:  # the unit job's stub has no airflow.models
        _unavailable("needs a real Airflow; runs in the integration CI job")
    try:
        with create_session() as session:
            session.scalar(select(DagRun.id).limit(1))
    except Exception as exc:
        _unavailable(
            f"no usable Airflow metadata DB ({type(exc).__name__}); "
            "run 'airflow db migrate' first"
        )


@pytest.fixture
def e2e_dags(_airflow_db, monkeypatch):
    """The module in ``dags/``, with Airflow pointed at that folder.

    Airflow 3.3's ``dag.test()`` syncs the DAG from its bundle before running
    it, and the default bundle reads ``settings.DAGS_FOLDER`` -- resolved once at
    import, hence patched as well as the env var. Examples stay off so the sync
    does not parse Airflow's own example DAGs.
    """
    from airflow import settings

    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_DIR))
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setattr(settings, "DAGS_FOLDER", str(DAGS_DIR))
    spec = importlib.util.spec_from_file_location(
        "apo_e2e_dags", DAGS_DIR / "apo_e2e_dags.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def make_suite(tmp_path):
    """Write ``test_s.py`` into a fresh suite dir and return the dir's path.

    The suite sits under ``tmp_path`` with no pytest config above it, so its
    rootdir is the suite dir itself -- never the worker's cwd, which is the
    setup that used to break reruns.
    """

    def make(source: str) -> str:
        suite = tmp_path / "suite"
        suite.mkdir()
        (suite / "test_s.py").write_text(textwrap.dedent(source))
        return str(suite)

    return make


def test_templated_run_pushes_its_summary_to_xcom(e2e_dags, make_suite):
    suite = make_suite(
        """
        import os

        def test_ok():
            pass

        def test_templated_env():
            assert os.environ["APO_E2E_GREETING"] == "hello"
        """
    )

    (ti,) = task_instances(run_dag(e2e_dags.basic, suite=suite, greeting="hello"))

    out = summary(ti)
    print(f"[e2e:basic] {ti.state} {out}")
    assert ti.state == "success"
    assert (out["total"], out["passed"], out["success"]) == (2, 2, True)
    assert out["failed_node_ids"] == []


def test_failing_suite_fails_the_task(e2e_dags, make_suite):
    suite = make_suite("def test_broken():\n    assert False\n")

    dagrun = run_dag(e2e_dags.failing, suite=suite)

    (ti,) = task_instances(dagrun)
    print(f"[e2e:failing] dagrun={dagrun.state} ti={ti.state}")
    assert (dagrun.state, ti.state) == ("failed", "failed")
    assert summary(ti) is None  # execute() raised, nothing was returned


def test_failed_only_retry_reruns_just_the_failure(e2e_dags, make_suite):
    suite = make_suite(FLAKY_TEST + "\ndef test_stable():\n    pass\n")

    (ti,) = task_instances(run_dag(e2e_dags.failed_only, suite=suite))

    out = summary(ti)
    print(f"[e2e:failed_only] {ti.state} try={ti.try_number} {out}")
    assert (ti.state, ti.try_number) == ("success", 2)
    assert (out["total"], out["passed"]) == (1, 1)  # only the previous failure
    assert last_failed_var_key({"ti": ti}) not in failed_only_keys()


def test_sharded_mapping_retries_each_shard_on_its_own(e2e_dags, make_suite):
    # Shards come from an upstream task's XCom. Shard 1 holds the flaky test:
    # it alone retries, and its failed_only key is its own (map_index).
    suite = make_suite(
        FLAKY_TEST
        + "".join(
            f"\ndef test_{name}():\n    pass\n"
            for name in ("stable_a", "stable_b", "stable_c")
        )
    )

    shards = task_instances(run_dag(e2e_dags.sharded, suite=suite))

    got = [(ti.map_index, ti.state, ti.try_number) for ti in shards]
    totals = [summary(ti)["total"] for ti in shards]
    print(f"[e2e:sharded] {got} totals={totals}")
    assert got == [(0, "success", 1), (1, "success", 2)]
    assert totals == [2, 1]
    assert not {last_failed_var_key({"ti": ti}) for ti in shards} & failed_only_keys()


def test_execution_timeout_kills_the_pytest_process(e2e_dags, make_suite):
    # The pid is written at import, well inside the 10s timeout even on a slow
    # CI runner; the test itself then outlives it.
    suite = make_suite(
        """
        import os
        import pathlib
        import time

        pathlib.Path(__file__).with_name("pid").write_text(str(os.getpid()))

        def test_hangs():
            time.sleep(300)
        """
    )

    started = time.monotonic()
    (ti,) = task_instances(run_dag(e2e_dags.timeout, suite=suite))
    elapsed = time.monotonic() - started

    pid = int((Path(suite) / "pid").read_text())
    print(f"[e2e:timeout] {ti.state} after {elapsed:.1f}s, pytest pid={pid}")
    assert ti.state != "success"
    assert elapsed < 90  # the 10s timeout, the kill, and dag.test() overhead
    assert not _alive(pid)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True
