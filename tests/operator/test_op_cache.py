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

import logging

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


@pytest.mark.parametrize(
    "flag", ["--lf", "--last-failed", "--ff", "--nf", "--sw", "--cache-clear"]
)
def test_warns_when_user_flag_needs_the_cacheprovider(flag, caplog):
    # Disabling the provider unregisters these options, so pytest aborts with
    # "unrecognized arguments" and writes NO report -- which the operator would
    # otherwise surface as an opaque "produced no report". Warn with the cause.
    op, runner = _op(pytest_args=[flag], cache=False)
    with caplog.at_level(logging.WARNING):
        op.execute(_ctx())
    text = caplog.text
    print(f"[cache:warn {flag}] warned={flag in text}")
    assert "cacheprovider" in text
    assert flag in text


def test_no_warning_when_cache_left_enabled(caplog):
    op, runner = _op(pytest_args=["--lf"], cache=True)
    with caplog.at_level(logging.WARNING):
        op.execute(_ctx())
    print(f"[cache:no-warn] log={caplog.text!r}")
    assert "cacheprovider" not in caplog.text


def test_no_warning_for_unrelated_flags(caplog):
    op, runner = _op(pytest_args=["-q", "-x"], cache=False)
    with caplog.at_level(logging.WARNING):
        op.execute(_ctx())
    print(f"[cache:no-warn-unrelated] log={caplog.text!r}")
    assert "unregisters" not in caplog.text


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
