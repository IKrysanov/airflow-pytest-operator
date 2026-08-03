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


"""cache: disable pytest's cacheprovider (-p no:cacheprovider) for ephemeral,
read-only, or sharded runs. Unlike the first-run-only splices, it applies to
every invocation. Shared fakes in _op_helpers."""

from __future__ import annotations

from unittest import mock

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    SequenceParser,
    _ctx,
    _res,
    _result,
)

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator
from airflow_pytest_operator.operators._constants import (
    cache_dependent_flags,
    disables_cacheprovider,
)

DISABLE = ["-p", "no:cacheprovider"]


def _op(**kwargs):
    """Operator wired to fakes, returning (op, runner)."""
    runner = kwargs.pop("runner", None) or FakeRunner(
        RunArtifacts(exit_code=0, report_path="/x.xml")
    )
    parser = kwargs.pop("parser", None) or FakeParser(_result(passed=1))
    op = PytestOperator(
        task_id="t", test_path="tests/", runner=runner, parser=parser, **kwargs
    )
    return op, runner


# -- defaults & basic splice ------------------------------------------------


def test_cache_defaults_to_true():
    # 0.6.1 is a patch release: pytest's own cache behaviour must be unchanged
    # unless the user opts out explicitly.
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[cache:default] cache={op.cache!r}")
    assert op.cache is True


def test_cache_true_splices_nothing():
    op, runner = _op(pytest_args=["-q"])
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:true] forwarded={forwarded!r}")
    assert forwarded == ["-q"]
    assert "no:cacheprovider" not in forwarded


def test_cache_false_appends_no_cacheprovider():
    op, runner = _op(pytest_args=["-q"], cache=False)
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:false] forwarded={forwarded!r}")
    assert forwarded == ["-q", *DISABLE]


def test_cache_false_with_no_user_args():
    op, runner = _op(cache=False)
    op.execute(_ctx())
    print(f"[cache:bare] forwarded={runner.calls[0]['pytest_args']!r}")
    assert runner.calls[0]["pytest_args"] == DISABLE


# -- deferring to explicit user args ----------------------------------------


def test_defers_to_explicit_two_token_form():
    # Already disabled by the user -> we must not pass -p twice.
    op, runner = _op(pytest_args=["-p", "no:cacheprovider", "-q"], cache=False)
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:defer-2token] forwarded={forwarded!r}")
    assert forwarded == ["-p", "no:cacheprovider", "-q"]
    assert forwarded.count("no:cacheprovider") == 1


def test_defers_to_concatenated_form():
    # "-pno:cacheprovider" is a spelling pytest genuinely accepts.
    op, runner = _op(pytest_args=["-pno:cacheprovider"], cache=False)
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:defer-concat] forwarded={forwarded!r}")
    assert forwarded == ["-pno:cacheprovider"]


def test_equals_form_is_not_treated_as_disabling():
    # "-p=no:cacheprovider" does NOT disable anything: pytest reads
    # "=no:cacheprovider" as the plugin NAME and dies importing it. Treating it
    # as "already disabled" would skip our correct splice and ship a broken run.
    op, runner = _op(pytest_args=["-p=no:cacheprovider"], cache=False)
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:equals-form] forwarded={forwarded!r}")
    assert forwarded == ["-p=no:cacheprovider", *DISABLE]


def test_unrelated_p_plugin_arg_is_not_mistaken_for_disabling():
    # A different -p value must not suppress our splice.
    op, runner = _op(pytest_args=["-p", "no:randomly"], cache=False)
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:other-plugin] forwarded={forwarded!r}")
    assert forwarded == ["-p", "no:randomly", *DISABLE]


def test_trailing_p_without_value_does_not_crash():
    # A dangling "-p" (templating artefact) must not IndexError on lookahead.
    op, runner = _op(pytest_args=["-p"], cache=False)
    op.execute(_ctx())
    print(f"[cache:dangling-p] forwarded={runner.calls[0]['pytest_args']!r}")
    assert runner.calls[0]["pytest_args"] == ["-p", *DISABLE]


# -- applies to EVERY invocation -------------------------------------------


def test_applied_in_dry_run_collection():
    op, runner = _op(cache=False, dry_run=True)
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:dry-run] forwarded={forwarded!r}")
    assert "--collect-only" in forwarded
    assert forwarded[-2:] == DISABLE


def test_applied_to_every_rerun_round():
    # The rerun rounds drop markers/coverage/xdist but must KEEP the cache
    # toggle -- a read-only fs is read-only on round 3 too.
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),
            _res(["tests.test_x::test_a"], passed=0),
            _res([], passed=1),
        ]
    )
    op, runner = _op(
        pytest_args=["-q"],
        cache=False,
        rerun_failed=2,
        fail_on_test_failure=False,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    per_call = [c["pytest_args"] for c in runner.calls]
    print(f"[cache:reruns] calls={len(per_call)} args={per_call!r}")
    assert len(per_call) == 3  # first run + 2 rerun rounds
    for args in per_call:
        assert args[-2:] == DISABLE, args


def test_rerun_rounds_keep_cache_enabled_when_cache_true():
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [_res(["tests.test_x::test_a"], passed=2), _res([], passed=1)]
    )
    op, runner = _op(
        cache=True,
        rerun_failed=1,
        fail_on_test_failure=False,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    per_call = [c["pytest_args"] for c in runner.calls]
    print(f"[cache:reruns-enabled] args={per_call!r}")
    assert all("no:cacheprovider" not in a for a in per_call)


# -- composition with the other splices ------------------------------------


def test_composes_with_parallel_and_selectors():
    op, runner = _op(pytest_args=["-q"], cache=False, parallel=2, markers="smoke")
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[cache:compose] forwarded={forwarded!r}")
    # Every splice lands, in order, and the cache toggle displaces none of them.
    assert forwarded == ["-q", *DISABLE, "-m", "smoke", "-n", "2"]


# -- the cache-dependent-flag warning --------------------------------------
#
# Airflow operator loggers do NOT always propagate to root (Airflow 3.2 routes
# them through structlog), so pytest's ``caplog`` fixture misses them and the
# assertions would silently pass against an empty string. We capture at the
# source instead -- the pattern used by test_op_dry_run.


def _warnings_of(op, ctx=None):
    """Run ``op`` and return every ``log.warning`` call flattened into a string."""
    with mock.patch.object(op.log, "warning") as warn:
        op.execute(ctx if ctx is not None else _ctx())
    return " ".join(str(c) for c in warn.call_args_list)


@pytest.mark.parametrize(
    "flag", ["--lf", "--last-failed", "--ff", "--nf", "--sw", "--cache-clear"]
)
def test_warns_when_user_flag_needs_the_cacheprovider(flag):
    # Disabling the provider unregisters these options, so pytest aborts with
    # "unrecognized arguments" and writes NO report -- which the operator would
    # otherwise surface as an opaque "produced no report". Warn with the cause.
    op, runner = _op(pytest_args=[flag], cache=False)
    logged = _warnings_of(op)
    print(f"[cache:warn {flag}] logged={logged!r}")
    assert "cacheprovider" in logged
    assert flag in logged


def test_no_warning_when_cache_left_enabled():
    op, runner = _op(pytest_args=["--lf"], cache=True)
    logged = _warnings_of(op)
    print(f"[cache:no-warn] logged={logged!r}")
    assert "cacheprovider" not in logged


def test_no_warning_for_unrelated_flags():
    op, runner = _op(pytest_args=["-q", "-x"], cache=False)
    logged = _warnings_of(op)
    print(f"[cache:no-warn-unrelated] logged={logged!r}")
    assert "unregisters" not in logged


# -- validation -------------------------------------------------------------


@pytest.mark.parametrize("bad", [1, 0, "yes", None])
def test_non_bool_cache_raises_type_error(bad):
    # Matches the ``coverage`` convention: no truthy ints, so a stray cache=0
    # cannot silently disable the provider.
    with pytest.raises(TypeError, match="cache"):
        PytestOperator(task_id="t", test_path="tests/", cache=bad)


# -- helper units -----------------------------------------------------------


@pytest.mark.parametrize(
    "args,expected",
    [
        ([], False),
        (["-q"], False),
        (["-p", "no:cacheprovider"], True),
        (["-pno:cacheprovider"], True),
        (["-p", "no:randomly"], False),
        (["-p=no:cacheprovider"], False),  # invalid spelling -> not disabling
        ([" -p", "no:cacheprovider"], False),  # argparse sees no option here
        # pytest strips the plugin name, so every padded form really does
        # disable the provider -- verified against pytest, which runs
        # ``parg = parg.strip()`` before loading.
        (["-p", " no:cacheprovider"], True),
        (["-p", "no:cacheprovider "], True),
        (["-pno:cacheprovider "], True),
        (["-p no:cacheprovider"], True),  # single argv token, inner space
        (["-p"], False),  # dangling, no lookahead crash
        (["-q", "-p", "no:cacheprovider", "-x"], True),
    ],
)
def test_disables_cacheprovider_unit(args, expected):
    got = disables_cacheprovider(args)
    print(f"[unit:disables] {args!r} -> {got}")
    assert got is expected


def test_cache_dependent_flags_unit():
    got = cache_dependent_flags(["-q", "--lf", "--cache-clear"])
    print(f"[unit:dependent] {got!r}")
    assert got == ["--lf", "--cache-clear"]
    assert cache_dependent_flags(["-q", "-x"]) == []


def test_cache_dependent_flags_matches_equals_form():
    # --lfnf takes a value, so the "=" spelling must be detected too.
    got = cache_dependent_flags(["--lfnf=all"])
    print(f"[unit:dependent-equals] {got!r}")
    assert got == ["--lfnf"]


# -- flags that never let the run finish --------------------------------------


@pytest.mark.parametrize(
    "args,offending",
    [
        (["-f"], "-f"),
        (["--looponfail"], "--looponfail"),
        # A single dash is a CLUSTER of short options, so -lf is -l plus -f --
        # measured: pytest prints LOOPONFAILING and blocks on "waiting for
        # changes". This is the spelling that actually reaches people, and the
        # one a naive `arg == "-f"` check misses.
        (["-lf"], "-lf"),
        (["-xf"], "-xf"),
        (["-qf"], "-qf"),
        (["-lvf"], "-lvf"),
        (["-q", "-lf"], "-lf"),
    ],
)
def test_never_terminating_flag_is_warned_about_before_the_run(args, offending):
    # --looponfail re-runs on filesystem changes and then blocks. Nobody edits
    # files on a worker, so the child never exits and the task holds its slot
    # until something outside kills it -- measured, not theorised. The warning
    # has to land BEFORE the launch: afterwards the task log stops moving and
    # this is the last line the user sees. It quotes the token as written, so
    # "-lf" is findable in the DAG.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=list(args),
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    logged = " ".join(
        c.args[0] % tuple(c.args[1:]) if len(c.args) > 1 else c.args[0]
        for c in warning.call_args_list
    )
    print(f"[cache:never_terminating {args}] {logged!r}")
    assert offending in logged
    assert "never exit" in logged
    assert "execution_timeout" in logged  # names the way out


@pytest.mark.parametrize(
    "args",
    [
        # -k and -n take a value, so the "f" is that value, not -f. Verified
        # against real pytest: -kf runs normally, -nf is a usage error.
        ["-kf"],
        ["-nf"],
        ["-k", "f"],
        ["-m", "fast"],
        # "-f" is two characters, so a naive prefix check would flag any --f*.
        ["--full-trace"],
        ["-x", "-q"],
        ["-p", "no:cacheprovider"],
    ],
)
def test_ordinary_args_do_not_trip_the_never_terminating_warning(args):
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=list(args),
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    logged = " ".join(str(c) for c in warning.call_args_list)
    assert "never exit" not in logged


@pytest.mark.parametrize(
    "addopts,offending",
    [
        ("-lf", "-lf"),
        ("-f", "-f"),
        ("--looponfail", "--looponfail"),
        ("-q -lf", "-lf"),
        ("--looponfail -q", "--looponfail"),
    ],
)
def test_pytest_addopts_in_env_is_checked_too(addopts, offending):
    # pytest prepends PYTEST_ADDOPTS to the command line, so a blocking flag
    # hides there exactly as well as in pytest_args -- measured: the run hangs.
    # env is the operator's own parameter, so this one it can see.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env={"PYTEST_ADDOPTS": addopts},
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    logged = " ".join(
        c.args[0] % tuple(c.args[1:]) if len(c.args) > 1 else c.args[0]
        for c in warning.call_args_list
    )
    print(f"[cache:addopts {addopts!r}] {logged!r}")
    assert offending in logged
    assert "never exit" in logged
    # The one source it cannot see is named, so the hunt continues in the
    # right place when the flag is not in the DAG at all.
    assert "pytest.ini" in logged


@pytest.mark.parametrize(
    "addopts", ["-q", "-kf", "--full-trace", "-p no:cacheprovider", "", "-k 'f or g'"]
)
def test_harmless_pytest_addopts_is_not_warned_about(addopts):
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env={"PYTEST_ADDOPTS": addopts},
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with mock.patch.object(op.log, "warning") as warning:
        op.execute(_ctx())
    assert "never exit" not in " ".join(str(c) for c in warning.call_args_list)


def test_unbalanced_quotes_in_addopts_do_not_break_the_run():
    # shlex raises on 'a "b' -- pytest will reject it too, but the operator must
    # not turn a bad env var into a crash of its own before the run even starts.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env={"PYTEST_ADDOPTS": 'a "b'},
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    assert op.execute(_ctx())["total"] == 1
