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


"""failed_only retry strategy: re-run only the previous attempt's failures on the
next Airflow retry, carried as node-ids in an Airflow Variable
(consume-on-read, crash-safe lifecycle). Shared fakes in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    FakeStore,
    _ctx,
    _key,
    _res,
)

from airflow_pytest_operator.exceptions import TestsFailedError
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_test_retry_strategy_default_is_all():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[retry:default_pin] op.test_retry_strategy = {op.test_retry_strategy!r}")
    assert op.test_retry_strategy == "all"


def test_invalid_test_retry_strategy_raises_value_error():
    with pytest.raises(ValueError, match="test_retry_strategy"):
        PytestOperator(task_id="t", test_path="tests/", test_retry_strategy="bogus")


def test_invalid_store_raises_type_error_at_init():
    # A store missing read/write/delete must fail fast at init, not at execute().
    with pytest.raises(TypeError, match="LastFailedStore"):
        PytestOperator(task_id="t", test_path="tests/", store=42)


def test_duck_typed_store_is_accepted():
    # Structural typing: any object with the three methods satisfies the
    # protocol -- no subclassing required. Pin that the injected instance is
    # used as-is (not silently replaced by the default store).
    store = FakeStore()
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        store=store,
        runner=FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml")),
    )
    assert op._store is store


def test_failed_only_first_attempt_runs_full_suite_and_records_failures():
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=2))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    # Default fail_on_test_failure=True: the failing run raises (so Airflow
    # retries) AND records its failures forward for that retry to narrow to.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, dag_id="d", task_id="t", run_id="r"))

    print(
        f"[failed_only:first] test_path={runner.calls[0]['test_path']!r} "
        f"reads={store.reads} writes={store.writes}"
    )
    # First attempt: the store (keyed by this run_id) is empty, so the read
    # finds nothing and the full suite runs -- no explicit is-retry check needed.
    assert store.reads == [_key()]
    assert runner.calls[0]["test_path"] == "tests/"
    # The failing node-id is recorded for the next retry to narrow to.
    assert store.writes == [(_key(), ["tests.test_x::test_a"])]
    # No pytest --lf flag is involved anymore.
    assert "--lf" not in runner.calls[0]["pytest_args"]


def test_failed_only_retry_narrows_to_stored_failures():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a", "tests.test_y::test_b"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=2))  # the narrowed run passes
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:retry] test_path={runner.calls[0]['test_path']!r}")
    # The retry runs ONLY the previously-failed tests, converted to selectors.
    assert runner.calls[0]["test_path"] == [
        "tests/test_x.py::test_a",
        "tests/test_y.py::test_b",
    ]
    assert store.reads == [key]
    # User pytest_args are forwarded untouched -- no --lf appended.
    assert runner.calls[0]["pytest_args"] == ["-k", "smoke"]


def test_failed_only_retry_with_empty_store_runs_full_suite():
    store = FakeStore()  # nothing stored
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:retry_empty] test_path={runner.calls[0]['test_path']!r}")
    # No stored failures -> safe fallback to the full suite.
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == [_key()]


def test_strategy_all_ignores_store_even_on_retry():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="all",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=3, dag_id="d", task_id="t", run_id="r"))

    print(f"[failed_only:all] test_path={runner.calls[0]['test_path']!r}")
    assert runner.calls[0]["test_path"] == "tests/"
    # Strategy "all" never touches the store.
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_never_appends_lf():
    store = FakeStore({_key(): ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[failed_only:no_lf] forwarded = {forwarded_args!r}")
    assert forwarded_args == ["-k", "smoke"]  # no --lf, no -o cache_dir=...


def test_failed_only_does_not_mutate_user_config_across_retries():
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    user_args = ["-k", "smoke"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute(_ctx(try_number=1, dag_id="d", task_id="t", run_id="r"))
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    print(
        f"[failed_only:no_mutation] pytest_args={op.pytest_args!r} "
        f"test_path={op.test_path!r}"
    )
    # Neither the stored pytest_args nor test_path are mutated by narrowing.
    assert op.pytest_args == ["-k", "smoke"]
    assert op.test_path == "tests/"


def test_failed_only_logs_on_retry():
    from unittest import mock

    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    with mock.patch.object(op.log, "info") as info:
        op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))

    logged = " ".join(str(c) for c in info.call_args_list)
    print(f"[failed_only:log] info() calls: {logged!r}")
    assert "failed_only" in logged
    assert "narrowing" in logged


def test_failed_only_missing_ti_in_context_degrades_to_full_suite():
    """A context without a usable 'ti' must not crash execute(); it should
    behave as the first attempt (full suite, store untouched)."""
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )

    op.execute({})  # no "ti" key at all -> no derivable key

    print(f"[failed_only:no_ti] test_path={runner.calls[0]['test_path']!r}")
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_consumes_variable_on_read():
    # A retry reads the stored failures and deletes the Variable immediately --
    # before running a single test -- so a mid-run crash cannot orphan it.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=2))  # narrowed run passes
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:consume] reads={store.reads} deletes={store.deletes}")
    # Deleted on read (consumed); passing run writes nothing back.
    assert store.deletes == [key]
    assert store.writes == []
    assert key not in store.data


def test_failed_only_consume_then_rewrite_when_still_failing_non_final():
    # Non-final retry: consume the old set on read, then write the (narrowed)
    # still-failing set for the NEXT retry.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a", "tests.test_y::test_b"]})
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_y::test_b"], passed=1))  # one still fails
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    # try_number (2) <= max_tries (3) -> not final, so a rewrite is expected.
    # Default fail_on_test_failure=True -> the failing attempt raises (Airflow
    # will retry) and hands the narrowed set forward for that retry.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=2, max_tries=3, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:consume_rewrite] deletes={store.deletes} writes={store.writes}")
    assert store.deletes == [key]  # old set consumed on read
    assert store.writes == [(key, ["tests.test_y::test_b"])]  # narrowed set saved
    assert store.data[key] == ["tests.test_y::test_b"]


def test_failed_only_writes_for_next_retry_on_failing_mid_cycle_first_attempt():
    # First attempt (empty store) fails and is not final -> write the failures
    # forward; nothing was read so nothing is consumed.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    # Default fail_on_test_failure=True -> the attempt raises and writes forward.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:mid_cycle] writes={store.writes} deletes={store.deletes}")
    assert store.writes == [(key, ["tests.test_x::test_a"])]
    assert store.deletes == []
    assert store.data[key] == ["tests.test_x::test_a"]


def test_failed_only_no_write_forward_when_fail_on_test_failure_false():
    # Regression: with fail_on_test_failure=False a failing run does NOT fail the
    # task, so Airflow never retries -- writing the failed set forward would
    # orphan a Variable that nothing ever consumes. The operator must skip the
    # write. (An empty store means it touches nothing at all beyond the read.)
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    # Not final and tests failed -- but the task succeeds, so no retry will read
    # a written set. Must NOT raise and must NOT write.
    out = op.execute(
        _ctx(try_number=1, max_tries=2, dag_id="d", task_id="t", run_id="r")
    )
    print(
        f"[failed_only:no_fail_no_write] writes={store.writes} success={out['success']}"
    )
    assert out["success"] is False  # XCom still reports the failure honestly
    assert store.writes == []  # no orphan left behind
    assert key not in store.data


def test_failed_only_terminal_attempt_consumes_and_writes_nothing():
    # The final attempt reads+consumes whatever a prior attempt left, runs, and
    # -- crucially -- writes nothing back, so it cannot leave an orphan even if
    # it fails. (Here it fails on its narrowed targets.)
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    # Final attempt: try_number (3) > max_tries (2). fail_on_test_failure=True so
    # the *only* thing that suppresses the write is the final-attempt gate.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=3, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:terminal] writes={store.writes} deletes={store.deletes}")
    assert store.deletes == [key]  # consumed on read
    assert store.writes == []  # final attempt never writes -> no orphan
    assert key not in store.data


def test_failed_only_final_attempt_with_empty_store_does_nothing():
    # The durability property in its purest form: a final attempt that fails
    # with no prior store neither reads anything to delete nor writes anything,
    # so a crash right after it can leave nothing behind.
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=3, max_tries=2, dag_id="d", task_id="t", run_id="r"))
    print(f"[var:terminal_empty] writes={store.writes} deletes={store.deletes}")
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_writes_forward_when_no_max_tries():
    # max_tries missing -> treat as "more retries may come" -> write forward.
    key = _key()
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
    )
    # no max_tries -> undeterminable -> treated as non-final -> write forward.
    with pytest.raises(TestsFailedError):
        op.execute(_ctx(try_number=9, dag_id="d", task_id="t", run_id="r"))
    assert store.deletes == []
    assert store.data[key] == ["tests.test_x::test_a"]


def test_failed_only_no_store_when_ids_missing():
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = FakeParser(_res(["tests.test_x::test_a"], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        runner=runner,
        parser=parser,
        store=store,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())  # ti has no dag_id/task_id/run_id -> no derivable key
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_strategy_all_never_touches_store():
    store = FakeStore()
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=1))
    op = PytestOperator(
        task_id="t", test_path="tests/", runner=runner, parser=parser, store=store
    )
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []


def test_failed_only_skipped_in_dry_run():
    # dry-run + failed_only is meaningless: --collect-only never runs test
    # bodies, so there is no "last failed" to narrow to. The operator touches
    # no Variable and adds no --lf; only --collect-only applies.
    store = FakeStore({_key(): ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_res([], passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        dry_run=True,
        runner=runner,
        parser=parser,
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    args = runner.calls[0]["pytest_args"]
    print(f"[dry_run+failed_only] args={args}")
    assert "--lf" not in args
    assert "--collect-only" in args  # dry-run itself still applies
    # Narrowing is skipped: the full suite is collected, store is untouched.
    assert runner.calls[0]["test_path"] == "tests/"
    assert store.reads == []
    assert store.writes == []
    assert store.deletes == []
