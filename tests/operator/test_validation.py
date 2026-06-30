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

import pytest
from _op_helpers import FakeStore

from airflow_pytest_operator.operators._validation import (
    validate_cov_fail_under,
    validate_coverage,
    validate_env,
    validate_markers_keyword,
    validate_parallel_dist,
    validate_rerun_failed,
    validate_store,
    validate_test_retry_strategy,
)

# -- test_retry_strategy -----------------------------------------------------


@pytest.mark.parametrize("value", ["all", "failed_only"])
def test_retry_strategy_valid(value):
    validate_test_retry_strategy(value)  # no raise


def test_retry_strategy_invalid():
    with pytest.raises(ValueError, match="test_retry_strategy"):
        validate_test_retry_strategy("nope")


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


def test_env_non_str_value():
    with pytest.raises(TypeError, match=r"env\["):
        validate_env({"A": 1})
