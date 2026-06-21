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

"""Helpers for sharding a suite across mapped tasks (cross-worker parallelism).

The pattern (see ``examples/sharded_mapped.py``): a *collect* task runs
``pytest --collect-only -q`` and feeds its stdout to
:func:`parse_collect_only_output`; the node-ids are split with
:func:`partition_node_ids`; then ``PytestOperator.partial(...).expand(
test_path=<chunks>)`` runs one mapped task per shard. Each shard can still set
``parallel=`` for xdist *within* its worker -- the two axes are orthogonal.

Both functions are pure (no I/O), so the distribution logic is unit-testable on
its own; the operator and runner are untouched.
"""

from __future__ import annotations

from collections.abc import Iterable


def parse_collect_only_output(text: str) -> list[str]:
    """Extract pytest node-ids from ``pytest --collect-only -q`` stdout.

    ``--collect-only -q`` prints one node-id per line followed by a summary
    line (``"123 tests collected in 0.4s"``) and possibly blank/warning lines.
    Every collected item's id contains ``"::"`` (it always names a test), while
    the summary and noise do not -- so we keep the ``"::"`` lines, in order.

    :param text: captured stdout of a ``--collect-only -q`` run.
    :returns: node-ids in collection order (file-grouped, as pytest emits them),
        ready for :func:`partition_node_ids`. Empty if nothing was collected.
    """
    return [line.strip() for line in text.splitlines() if "::" in line]


def partition_node_ids(node_ids: Iterable[str], num_shards: int) -> list[list[str]]:
    """Split node-ids into up to ``num_shards`` balanced, contiguous groups.

    Groups are contiguous (not round-robin) to preserve file/module locality:
    pytest collects file by file, so adjacent ids share a module, and keeping
    them in the same shard avoids re-running module/class-scoped fixtures in
    several shards -- the same reasoning as xdist's ``loadscope``.

    Sizes differ by at most one (the first ``len % num_shards`` groups get one
    extra). Empty groups are never returned, so a shard can never end up with an
    empty ``test_path`` -- which pytest would interpret as "collect everything",
    making that shard re-run the whole suite. Consequently fewer than
    ``num_shards`` groups come back when there are fewer ids than shards.

    :param node_ids: node-ids, typically from :func:`parse_collect_only_output`.
    :param num_shards: desired shard count; must be an int >= 1 (``bool`` is
        rejected, matching the operator's validation convention).
    :returns: at most ``num_shards`` non-empty lists; ``[]`` for no ids.
    :raises TypeError: if ``num_shards`` is not an int (or is a ``bool``).
    :raises ValueError: if ``num_shards`` < 1.
    """
    if isinstance(num_shards, bool) or not isinstance(num_shards, int):
        raise TypeError(
            f"num_shards must be an int (not bool); got {type(num_shards).__name__}"
        )
    if num_shards < 1:
        raise ValueError(f"num_shards must be >= 1; got {num_shards!r}")

    ids = list(node_ids)
    shards = min(num_shards, len(ids))
    if shards == 0:
        return []

    base, extra = divmod(len(ids), shards)
    groups: list[list[str]] = []
    start = 0
    for i in range(shards):
        size = base + (1 if i < extra else 0)
        groups.append(ids[start : start + size])
        start += size
    return groups
