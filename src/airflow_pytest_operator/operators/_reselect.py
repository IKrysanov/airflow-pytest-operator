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

import os
from collections.abc import Sequence
from dataclasses import dataclass

from ..utils import node_id_to_pytest_args

# A report's node-ids are relative to pytest's *rootdir* -- the closest ancestor
# of the targets holding a pytest config file, else the targets' common
# directory. Re-running them needs two things the ids alone don't give:
#
# * a path from the worker's cwd. The runner resolves a relative target against
#   that cwd, a different directory whenever it isn't the rootdir
#   (``/opt/airflow/tests`` with no config above it, run from ``/opt/airflow``):
#   the failed tests were "not found" and the rerun ran nothing;
# * the same rootdir. Without a config file, a narrowed run's rootdir shrinks to
#   the failed tests' own directory, which drops the suite's upper conftest.py
#   files (pytest's confcutdir defaults to the rootdir) and makes the next
#   round's ids relative to that smaller directory.
#
# ``reselect`` locates each id in the original targets' tree and infers the
# rootdir from where it was found, so the caller can pin it with --rootdir.


@dataclass(frozen=True)
class Reselection:
    """How to re-run a set of failed node-ids.

    ``selectors`` re-run the ids that were located; ``missing`` are the ids with
    no file in the suite's tree (a collection error, a renamed or deleted file),
    which no selector can re-run. ``rootdir`` is the directory the ids are
    relative to -- the previous run's rootdir -- or ``None`` when it cannot be
    inferred, or when the ids alone are not enough to trust it (see
    :func:`_plausible_rootdir`). ``root`` is the tree the ids were looked up in.
    """

    selectors: list[str]
    missing: list[str]
    rootdir: str | None
    root: str | None


def suite_root(test_path: str | Sequence[str]) -> str | None:
    """The directory every collected test file lives under, or ``None``.

    Mirrors pytest's common ancestor of its arguments: each target's path
    portion (a node-id is anchored on what precedes ``::``) is reduced to a
    directory, and their ``commonpath`` is taken. Targets that do not exist from
    this worker's cwd are skipped -- a custom runner may resolve them elsewhere
    (an explicit ``cwd``, a container) -- and ``None`` means nothing to anchor on.
    """
    targets = [test_path] if isinstance(test_path, str) else list(test_path)
    dirs: list[str] = []
    for target in targets:
        path_part = target.partition("::")[0]
        if not path_part or not os.path.exists(path_part):
            continue
        path = os.path.abspath(path_part)
        dirs.append(path if os.path.isdir(path) else os.path.dirname(path))
    return os.path.commonpath(dirs) if dirs else None


def reselect(node_ids: Sequence[str], test_path: str | Sequence[str]) -> Reselection:
    """Locate ``node_ids`` in the tree of ``test_path`` to re-run them.

    pytest's rootdir is the suite root or one of its ancestors, so each id's
    relative path is tried against the root and then each parent in turn; the
    first candidate that exists **inside** the root wins (every collected file
    lives there, which also keeps a ``..`` path from escaping the suite). An
    absolute path is never located: pytest's ids are relative, so one can only
    come from a hand-edited failed_only Variable.

    When the suite's targets are not on this worker's filesystem there is
    nothing to check against, and the converted selectors come back as they are.
    """
    selectors = node_id_to_pytest_args(node_ids)
    root = suite_root(test_path)
    if root is None:
        return Reselection(selectors, [], None, None)
    located: list[str] = []
    missing: list[str] = []
    bases: set[str] = set()
    for node_id, selector in zip(node_ids, selectors, strict=True):
        found = _locate(selector, root)
        if found is None:
            missing.append(node_id)
        else:
            located.append(found[0])
            bases.add(found[1])
    # All ids come from one report, so they share one rootdir; if they somehow
    # don't, pinning any single one would be a guess.
    base = bases.pop() if len(bases) == 1 else None
    return Reselection(located, missing, _plausible_rootdir(base, root), root)


def _locate(selector: str, root: str) -> tuple[str, str] | None:
    """``(absolute selector, directory it was found from)``, or ``None``."""
    path_part, sep, rest = selector.partition("::")
    if not path_part or os.path.isabs(path_part):
        return None
    base = root
    while True:
        candidate = os.path.normpath(os.path.join(base, path_part))
        if _is_within(candidate, root) and os.path.exists(candidate):
            return candidate + sep + rest, base
        parent = os.path.dirname(base)
        if parent == base:  # reached the filesystem root
            return None
        base = parent


# Files whose directory pytest takes as the rootdir.
_CONFIGS = ("pytest.ini", "pyproject.toml", "tox.ini", "setup.cfg")


def _plausible_rootdir(base: str | None, root: str) -> str | None:
    """``base`` if pytest could have used it as the rootdir, else ``None``.

    pytest's rootdir is the closest ancestor holding a config file, else a
    directory derived from the targets themselves -- so the suite root and any
    ancestor with a config file are plausible, and nothing else is. A
    hand-edited failed_only Variable can otherwise name an id deep enough to
    resolve only from ``/``, and pinning *that* would hand pytest every
    conftest.py on the way down. Not pinning is always safe: pytest then picks
    the rootdir itself, as it did before.
    """
    if base is None or base == root:
        return base
    return (
        base if any(os.path.exists(os.path.join(base, f)) for f in _CONFIGS) else None
    )


def _is_within(path: str, directory: str) -> bool:
    """True when ``path`` really lives inside ``directory``.

    Symlinks are resolved on both sides: a link inside the suite pointing out of
    it must not make a file outside the tree look like one of its tests.
    """
    real_path = os.path.realpath(path)
    real_dir = os.path.realpath(directory)
    return real_path == real_dir or real_path.startswith(
        real_dir.rstrip(os.sep) + os.sep
    )
