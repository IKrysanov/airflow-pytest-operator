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


"""Direct unit tests for the extracted constructor validators (_validation.py).

These pin each function's contract in isolation; the operator's feature suites
(test_op_*.py) also exercise them through PytestOperator.__init__. Convention:
wrong type -> TypeError, valid type / wrong value -> ValueError.
"""

from __future__ import annotations

from decimal import Decimal
from enum import IntEnum
from fractions import Fraction

import pytest
from _op_helpers import FakeStore

from airflow_pytest_operator.operators._validation import (
    validate_collaborators,
    validate_cov_fail_under,
    validate_coverage,
    validate_env,
    validate_markers_keyword,
    validate_max_failed,
    validate_min_pass_rate,
    validate_parallel_dist,
    validate_pytest_args,
    validate_rerun_failed,
    validate_store,
    validate_test_path,
    validate_test_retry_strategy,
    validate_threshold_combination,
)


class _Deferred:
    """Stand-in for an XComArg: a template field can legally hold one.

    The README's failed_only example passes one straight into ``test_path``, so
    the constructor validators must not type-check these fields into rejecting
    it. Anything asserting "accepts a deferred value" uses this.
    """


# -- test_retry_strategy -----------------------------------------------------


@pytest.mark.parametrize("value", ["all", "failed_only"])
def test_retry_strategy_valid(value):
    validate_test_retry_strategy(value)  # no raise


def test_retry_strategy_invalid():
    with pytest.raises(ValueError, match="test_retry_strategy"):
        validate_test_retry_strategy("nope")


@pytest.mark.parametrize("value", [None, 123, ["all"], {"all"}])
def test_retry_strategy_non_str_is_a_type_error(value):
    # Convention: wrong type -> TypeError. The unhashable cases (list, set) used
    # to die in the frozenset lookup with a bare "unhashable type", naming
    # neither the parameter nor the accepted values.
    with pytest.raises(TypeError, match="test_retry_strategy"):
        validate_test_retry_strategy(value)


# -- test_path ---------------------------------------------------------------


def test_test_path_none_rejected():
    # Otherwise surfaces on the worker as "'NoneType' object is not iterable"
    # from inside the runner, with nothing naming the parameter.
    with pytest.raises(TypeError, match="test_path is required"):
        validate_test_path(None)


@pytest.mark.parametrize(
    "value", ["tests/", ["a.py", "b.py"], ("a.py",), _Deferred(), "{{ params.p }}"]
)
def test_test_path_accepts_everything_else_including_deferred(value):
    # Deliberately only a None check: test_path is a template field and the
    # README passes an XComArg into it, so a type check would reject a
    # documented pattern. Empty/blank is the runner's own fail-closed check.
    validate_test_path(value)


# -- pytest_args -------------------------------------------------------------


def test_pytest_args_string_rejected():
    # THE silent-corruption case: list("--verbose") is nine single-character
    # arguments, forwarded to pytest as garbage without any error.
    with pytest.raises(TypeError, match="not a single string"):
        validate_pytest_args("--verbose")


def test_pytest_args_bytes_rejected():
    with pytest.raises(TypeError, match="not a single string"):
        validate_pytest_args(b"-x")


def test_pytest_args_dict_rejected():
    # Iterating a dict yields its keys, so the values would vanish silently.
    with pytest.raises(TypeError, match="reduced to its keys"):
        validate_pytest_args({"-k": "smoke"})


@pytest.mark.parametrize(
    "value", [None, [], ["-x"], ["-k", "smoke"], ("-q",), _Deferred()]
)
def test_pytest_args_accepts_sequences_and_deferred(value):
    validate_pytest_args(value)


# -- runner / parser ---------------------------------------------------------


class _DuckRunner:
    def run(self, *a, **k):
        pass

    def cleanup(self, **k):
        pass


class _DuckParser:
    def report_request(self, report_dir):
        pass

    def parse(self, report_path, **k):
        pass


def test_collaborators_accept_none_and_duck_typed_objects():
    # Structural like validate_store: PytestRunner / ResultParser are ABCs, but
    # a custom runner that subclasses neither is a documented extension point.
    validate_collaborators(None, None)
    validate_collaborators(_DuckRunner(), _DuckParser())


def test_collaborators_reject_a_runner_without_the_contract():
    with pytest.raises(TypeError, match="runner must be an object"):
        validate_collaborators(object(), None)


def test_collaborators_reject_a_parser_without_the_contract():
    with pytest.raises(TypeError, match="parser must be an object"):
        validate_collaborators(None, object())


def test_collaborators_name_the_missing_methods():
    class HalfRunner:
        def run(self, *a, **k):
            pass

    with pytest.raises(TypeError, match="missing cleanup"):
        validate_collaborators(HalfRunner(), None)


def test_collaborators_reject_a_non_callable_attribute():
    # An attribute that merely exists is not the contract.
    class NotCallable:
        run = "nope"
        cleanup = "nope"

    with pytest.raises(TypeError, match="runner must be an object"):
        validate_collaborators(NotCallable(), None)


# -- markers / keyword -------------------------------------------------------


def test_markers_keyword_valid():
    validate_markers_keyword(None, None)
    validate_markers_keyword("smoke and not slow", "login or logout")


@pytest.mark.parametrize("markers,keyword", [(123, None), (None, ["k"])])
def test_markers_keyword_non_str(markers, keyword):
    with pytest.raises(TypeError, match="markers|keyword"):
        validate_markers_keyword(markers, keyword)


# -- rerun_failed ------------------------------------------------------------


@pytest.mark.parametrize("value", [0, 1, 5])
def test_rerun_failed_valid(value):
    validate_rerun_failed(value)


def test_rerun_failed_bool_rejected():
    with pytest.raises(TypeError, match="rerun_failed"):
        validate_rerun_failed(True)


def test_rerun_failed_non_int():
    with pytest.raises(TypeError, match="rerun_failed"):
        validate_rerun_failed(1.5)


def test_rerun_failed_negative():
    with pytest.raises(ValueError, match="non-negative"):
        validate_rerun_failed(-1)


# -- parallel / dist ---------------------------------------------------------


@pytest.mark.parametrize("parallel", [None, 1, 8, "auto", "logical"])
def test_parallel_valid(parallel):
    validate_parallel_dist(parallel, None)


def test_parallel_bool_rejected():
    with pytest.raises(TypeError, match="parallel"):
        validate_parallel_dist(True, None)


def test_parallel_int_below_one():
    with pytest.raises(ValueError, match="parallel must be >= 1"):
        validate_parallel_dist(0, None)


def test_parallel_bad_keyword():
    with pytest.raises(ValueError, match="'auto', 'logical'"):
        validate_parallel_dist("many", None)


def test_parallel_bad_type():
    with pytest.raises(TypeError, match="parallel must be an int"):
        validate_parallel_dist(1.5, None)


def test_dist_valid_with_parallel():
    validate_parallel_dist(2, "loadscope")


def test_dist_bad_mode():
    with pytest.raises(ValueError, match="dist must be one of"):
        validate_parallel_dist(2, "nope")


@pytest.mark.parametrize("value", [123, ["load"], 1.5])
def test_dist_non_str_is_a_type_error(value):
    # Convention: wrong type -> TypeError, not the value-error listing modes.
    with pytest.raises(TypeError, match="dist must be a str"):
        validate_parallel_dist(2, value)


def test_dist_requires_parallel():
    with pytest.raises(ValueError, match="dist requires parallel"):
        validate_parallel_dist(None, "load")


# -- coverage ----------------------------------------------------------------


@pytest.mark.parametrize("value", [True, False])
def test_coverage_valid(value):
    validate_coverage(value)


def test_coverage_non_bool():
    with pytest.raises(TypeError, match="coverage must be a bool"):
        validate_coverage(1)


# -- cov_fail_under ----------------------------------------------------------


@pytest.mark.parametrize("value", [None, 0.0, 0.5, 1.0, 1, 0])
def test_cov_fail_under_valid(value):
    validate_cov_fail_under(value)


def test_cov_fail_under_bool_rejected():
    with pytest.raises(TypeError, match="cov_fail_under"):
        validate_cov_fail_under(True)


def test_cov_fail_under_non_number():
    with pytest.raises(TypeError, match="cov_fail_under"):
        validate_cov_fail_under("0.8")


def test_cov_fail_under_above_one():
    with pytest.raises(ValueError, match="0.8 for 80"):
        validate_cov_fail_under(80)


def test_cov_fail_under_negative():
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        validate_cov_fail_under(-0.5)


# -- min_pass_rate -----------------------------------------------------------


@pytest.mark.parametrize("value", [None, 0.0, 0.5, 0.95, 1.0, 1, 0])
def test_min_pass_rate_valid(value):
    validate_min_pass_rate(value)


def test_min_pass_rate_bool_rejected():
    with pytest.raises(TypeError, match="min_pass_rate"):
        validate_min_pass_rate(True)


def test_min_pass_rate_non_number():
    with pytest.raises(TypeError, match="min_pass_rate"):
        validate_min_pass_rate("0.95")


def test_min_pass_rate_above_one():
    with pytest.raises(ValueError, match="0.95 for 95"):
        validate_min_pass_rate(95)


def test_min_pass_rate_negative():
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        validate_min_pass_rate(-0.5)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_min_pass_rate_non_finite_rejected(value):
    # NaN compares False against everything (a gate that never fires) and the
    # infinities are outside [0, 1]; the range check rejects all three.
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        validate_min_pass_rate(value)


# The [0, 1] interval is closed, so the interesting values are the two endpoints
# and the two representable neighbours just outside them -- not "0.5 and -1".
@pytest.mark.parametrize(
    "value", [0, 1, 0.0, 1.0, -0.0, 5e-324, 1 - 2**-53, Fraction(19, 20).__float__()]
)
def test_min_pass_rate_accepts_the_closed_interval_and_its_edges(value):
    validate_min_pass_rate(value)


@pytest.mark.parametrize("value", [1 + 2**-52, -5e-324, 1.0000000000000002])
def test_min_pass_rate_rejects_one_ulp_outside_the_interval(value):
    # The nearest representable values on the wrong side of each endpoint. Guards
    # against a `<`/`<=` slip that would only show up at the very boundary.
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        validate_min_pass_rate(value)


@pytest.mark.parametrize(
    "value", [Decimal("0.95"), Fraction(19, 20), complex(0.95), [], object()]
)
def test_min_pass_rate_rejects_non_float_numeric_types(value):
    # Deliberately strict, and the same rule as cov_fail_under: only int/float
    # are accepted, so a Decimal or Fraction is a TypeError rather than a silent
    # coercion whose rounding nobody sees.
    with pytest.raises(TypeError, match="min_pass_rate"):
        validate_min_pass_rate(value)


def test_min_pass_rate_accepts_an_int_subclass_that_is_not_bool():
    # bool is the one int subclass rejected (it is not a fraction); an IntEnum
    # from a user's config layer genuinely is one.
    class Level(IntEnum):
        ALL = 1

    validate_min_pass_rate(Level.ALL)


@pytest.mark.parametrize("value", [0, 1, 10**9, 10**100])
def test_max_failed_accepts_any_non_negative_count(value):
    # No upper bound: an arbitrarily large cap is the honest way to say
    # "unlimited", and Python ints have no overflow to guard against.
    validate_max_failed(value)


@pytest.mark.parametrize("value", [-1, -(10**9)])
def test_max_failed_rejects_below_the_lower_bound(value):
    with pytest.raises(ValueError, match="non-negative"):
        validate_max_failed(value)


@pytest.mark.parametrize("value", [3.0, 0.0, Decimal(3), "3", [], None.__class__])
def test_max_failed_rejects_non_int_types(value):
    # 3.0 is rejected even though it is a whole number: a count is an int, and
    # accepting whole floats would make 0.5 the only rejected float -- a rule
    # nobody can predict. Matches rerun_failed.
    with pytest.raises(TypeError, match="max_failed"):
        validate_max_failed(value)


# -- min_pass_rate / max_failed together -------------------------------------


@pytest.mark.parametrize(
    "min_pass_rate,max_failed",
    [
        (None, None),
        (0.95, None),
        (None, 5),
        (0.95, 5),
        (0.0, 0),
        (0.999999, 1),
        # 1.0 with a cap of 0 says the same thing twice -- allowed on purpose.
        (1.0, 0),
        (1.0, None),
    ],
)
def test_threshold_combination_allows_every_satisfiable_pair(min_pass_rate, max_failed):
    # No pair is contradictory: a fully green run (rate 1.0, zero failures)
    # satisfies both gates, whatever they are. So the only thing to reject is a
    # gate that can never decide -- never a "conflict".
    validate_threshold_combination(min_pass_rate, max_failed)


@pytest.mark.parametrize("max_failed", [1, 5, 1000])
def test_threshold_combination_rejects_a_cap_that_can_never_apply(max_failed):
    # min_pass_rate=1.0 demands every executed test pass, so ANY failure breaches
    # it before the cap is consulted. A caller who wrote "tolerate 5" would get
    # "tolerate none" -- silently, and only in production.
    with pytest.raises(ValueError, match="can never apply"):
        validate_threshold_combination(1.0, max_failed)


def test_threshold_combination_rejection_names_both_ways_out():
    with pytest.raises(ValueError) as exc:
        validate_threshold_combination(1.0, 5)
    msg = str(exc.value)
    print(f"[combo:message] {msg}")
    assert "lower min_pass_rate" in msg
    assert "drop" in msg and "max_failed" in msg


def test_threshold_combination_accepts_an_int_one():
    # 1 == 1.0, so the int spelling of "everything must pass" is caught too.
    with pytest.raises(ValueError, match="can never apply"):
        validate_threshold_combination(1, 3)


# -- max_failed --------------------------------------------------------------


@pytest.mark.parametrize("value", [None, 0, 1, 500])
def test_max_failed_valid(value):
    validate_max_failed(value)


def test_max_failed_bool_rejected():
    with pytest.raises(TypeError, match="max_failed"):
        validate_max_failed(True)


def test_max_failed_float_rejected():
    # A count, not a fraction -- 0.5 means the user wanted min_pass_rate.
    with pytest.raises(TypeError, match="max_failed"):
        validate_max_failed(0.5)


def test_max_failed_negative():
    with pytest.raises(ValueError, match="non-negative"):
        validate_max_failed(-1)


# -- store -------------------------------------------------------------------


def test_store_valid():
    validate_store(None)
    validate_store(FakeStore())  # structurally implements read/write/delete


def test_store_invalid():
    with pytest.raises(TypeError, match="LastFailedStore"):
        validate_store(object())


# -- env ---------------------------------------------------------------------


def test_env_valid():
    validate_env(None)
    validate_env({"A": "1", "B": "2"})


def test_env_not_dict():
    with pytest.raises(TypeError, match="env must be a dict"):
        validate_env(["A=1"])


def test_env_non_str_key():
    with pytest.raises(TypeError, match="env keys must be str"):
        validate_env({1: "x"})


# The content rules below are the OS's own. Without them each of these reaches
# the worker and dies mid-execute with a bare ValueError from inside os/
# subprocess -- no parameter name, nothing pointing at env.


def test_env_empty_key_rejected():
    # Silently dropped otherwise: the run goes green without the variable.
    with pytest.raises(ValueError, match="must not be empty"):
        validate_env({"": "x"})


def test_env_key_with_equals_rejected():
    with pytest.raises(ValueError, match="must not contain '='"):
        validate_env({"A=B": "x"})


def test_env_nul_in_key_rejected():
    with pytest.raises(ValueError, match="NUL"):
        validate_env({"A\x00B": "x"})


def test_env_nul_in_value_rejected():
    with pytest.raises(ValueError, match="NUL"):
        validate_env({"A": "v\x00w"})


def test_env_rejection_names_the_offending_key():
    # The whole point: the message must say which entry is wrong.
    with pytest.raises(ValueError, match="MY_VAR"):
        validate_env({"OK": "1", "MY_VAR": "bad\x00value"})


@pytest.mark.parametrize(
    "env",
    [
        {"A": ""},  # empty VALUE is legal -- an exported empty variable
        {"lower_case": "1"},
        {"WITH.DOT": "1"},  # looser than POSIX names, but the OS accepts it
        {"WITH-DASH": "1"},
        {"UNICODE_Ключ": "значение"},
        {"MULTILINE": "a\nb"},  # newlines are fine in a value
        {"SPACES IN NAME": "1"},
    ],
)
def test_env_accepts_everything_the_os_accepts(env):
    # Deliberately no stricter than the OS: tools do use dotted/dashed names,
    # and rejecting them would break working DAGs for the sake of tidiness.
    validate_env(env)


def test_env_non_str_value():
    with pytest.raises(TypeError, match=r"env\["):
        validate_env({"A": 1})
