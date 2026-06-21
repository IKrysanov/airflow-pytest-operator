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

"""Unit tests for the sharding helpers (pure functions, no I/O)."""

from __future__ import annotations

import pytest

from airflow_pytest_operator import parse_collect_only_output, partition_node_ids

# ---------------------------------------------------------------------------
# parse_collect_only_output: pull node-ids out of `pytest --collect-only -q`
# ---------------------------------------------------------------------------


def test_parse_keeps_node_id_lines_drops_summary_and_blanks():
    out = (
        "tests/test_a.py::test_one\n"
        "tests/test_a.py::test_two\n"
        "tests/test_b.py::TestC::test_three\n"
        "\n"
        "3 tests collected in 0.04s\n"
    )
    ids = parse_collect_only_output(out)
    print(f"[collect:parse] {ids}")
    assert ids == [
        "tests/test_a.py::test_one",
        "tests/test_a.py::test_two",
        "tests/test_b.py::TestC::test_three",
    ]


def test_parse_keeps_parametrized_ids():
    out = "tests/t.py::test_x[1-a]\ntests/t.py::test_x[2-b]\n2 tests collected\n"
    assert parse_collect_only_output(out) == [
        "tests/t.py::test_x[1-a]",
        "tests/t.py::test_x[2-b]",
    ]


def test_parse_empty_when_no_tests_collected():
    # `--collect-only -q` on an empty selection prints only a summary line.
    assert parse_collect_only_output("no tests ran in 0.01s\n") == []
    assert parse_collect_only_output("") == []


def test_parse_strips_surrounding_whitespace():
    assert parse_collect_only_output("  tests/t.py::test_a  \n") == [
        "tests/t.py::test_a"
    ]


# ---------------------------------------------------------------------------
# partition_node_ids: balanced, contiguous, never-empty groups
# ---------------------------------------------------------------------------


def _ids(n):
    return [f"tests/t.py::test_{i}" for i in range(n)]


def test_partition_single_shard_returns_one_group():
    ids = _ids(5)
    assert partition_node_ids(ids, 1) == [ids]


def test_partition_even_split():
    groups = partition_node_ids(_ids(6), 3)
    print(f"[shard:even] {[len(g) for g in groups]}")
    assert groups == [
        ["tests/t.py::test_0", "tests/t.py::test_1"],
        ["tests/t.py::test_2", "tests/t.py::test_3"],
        ["tests/t.py::test_4", "tests/t.py::test_5"],
    ]


def test_partition_uneven_split_front_loads_remainder():
    # 7 into 3 -> sizes 3,2,2 (first `7 % 3 == 1` group gets the extra).
    groups = partition_node_ids(_ids(7), 3)
    print(f"[shard:uneven] {[len(g) for g in groups]}")
    assert [len(g) for g in groups] == [3, 2, 2]
    # Contiguous: concatenation round-trips the original order.
    assert [nid for g in groups for nid in g] == _ids(7)


def test_partition_is_contiguous_not_round_robin():
    # File locality: adjacent ids stay together, not scattered across shards.
    groups = partition_node_ids(_ids(4), 2)
    assert groups[0] == ["tests/t.py::test_0", "tests/t.py::test_1"]
    assert groups[1] == ["tests/t.py::test_2", "tests/t.py::test_3"]


def test_partition_more_shards_than_ids_yields_no_empty_groups():
    # Asking for 5 shards over 2 ids gives 2 single-id groups, never 5 with
    # empties (an empty test_path would make a shard run the whole suite).
    groups = partition_node_ids(_ids(2), 5)
    print(f"[shard:excess] {groups}")
    assert groups == [["tests/t.py::test_0"], ["tests/t.py::test_1"]]
    assert all(g for g in groups)


def test_partition_empty_ids_returns_empty_list():
    assert partition_node_ids([], 4) == []


def test_partition_consumes_arbitrary_iterable():
    groups = partition_node_ids((f"t::test_{i}" for i in range(3)), 2)
    assert [len(g) for g in groups] == [2, 1]


def test_partition_zero_shards_raises_value_error():
    with pytest.raises(ValueError, match="num_shards must be >= 1"):
        partition_node_ids(_ids(3), 0)


def test_partition_negative_shards_raises_value_error():
    with pytest.raises(ValueError, match="num_shards"):
        partition_node_ids(_ids(3), -2)


def test_partition_bool_shards_raises_type_error():
    # bool is an int subclass; reject it like the operator does for counts.
    with pytest.raises(TypeError, match="num_shards must be an int"):
        partition_node_ids(_ids(3), True)


def test_partition_non_int_shards_raises_type_error():
    with pytest.raises(TypeError, match="num_shards"):
        partition_node_ids(_ids(3), 2.5)
