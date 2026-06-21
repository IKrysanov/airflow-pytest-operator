"""Internal vocabulary for :class:`PytestOperator`.

Module-private constants (the accepted option values and the pytest CLI flags
the operator knows about) plus one small pure helper, kept out of
``pytest_operator.py`` so that file stays focused on orchestration. Nothing here
is part of the public API.
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

from collections.abc import Sequence

# Cap on captured child stderr echoed into a TestExecutionError message, so a
# pathological run can't push a multi-megabyte blob into the task log.
MAX_STDERR_LEN = 4096

# Every spelling of pytest's collect-only flag; if the user already passed one
# in dry-run, the operator does not append a second.
COLLECT_ONLY_ALIASES: frozenset[str] = frozenset(
    {"--collect-only", "--collectonly", "--co"}
)

# Accepted values for ``test_retry_strategy``.
RETRY_STRATEGIES: frozenset[str] = frozenset({"all", "failed_only"})

# pytest-xdist --dist scheduler modes. "load" (xdist's default once -n is set)
# spreads individual tests; "loadscope"/"loadfile"/"loadgroup" keep a whole
# scope (module/class, file, or xdist_group) on one worker -- which is exactly
# why a suite sharing one scope can land entirely on gw0 there.
DIST_MODES: frozenset[str] = frozenset(
    {"load", "loadscope", "loadfile", "loadgroup", "worksteal", "each", "no"}
)

# Worker-count keywords -n accepts in addition to a plain integer.
PARALLEL_KEYWORDS: frozenset[str] = frozenset({"auto", "logical"})

# Flag families the operator's sugar / parallelism map onto. Each tuple lists
# every spelling so the operator can detect "the user already set this in
# pytest_args" and defer rather than configure it twice.
NUMPROCESSES_FLAGS: tuple[str, ...] = ("-n", "--numprocesses")
DIST_FLAGS: tuple[str, ...] = ("--dist",)
MARKER_FLAGS: tuple[str, ...] = ("-m",)
KEYWORD_FLAGS: tuple[str, ...] = ("-k",)


def has_flag(args: Sequence[str], names: tuple[str, ...]) -> bool:
    """True if ``args`` already contains any of ``names`` in any spelling.

    Matches the bare flag (``-n``), the ``=`` form (``-n=4``,
    ``--numprocesses=4``), and the short concatenated form (``-n4``), so the
    operator never appends a flag the user already set in ``pytest_args``.
    """
    for arg in args:
        for name in names:
            if arg == name or arg.startswith(name + "="):
                return True
            # Short concatenated form like "-n4" -- single-dash flags only.
            if (
                not name.startswith("--")
                and len(name) == 2
                and len(arg) > 2
                and arg.startswith(name)
            ):
                return True
    return False
