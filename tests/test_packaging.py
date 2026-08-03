"""Packaging invariants that keep the built distribution correct."""

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

import re
from pathlib import Path

import airflow_pytest_operator

_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"
_SDIST_TABLE = "[tool.hatch.build.targets.sdist]"


def _sdist_include_patterns() -> list[str] | None:
    """The sdist ``include`` entries, read without a TOML parser.

    ``tomllib`` is 3.11+ and this project supports 3.10, where importing it
    fails at collection time and takes the whole module down. The assertion
    below is about the literal patterns anyway, so reading them literally keeps
    the check running on every interpreter in the matrix.

    ``None`` when there is nothing to read -- installed without sources, or the
    table has moved -- so the test skips instead of failing on its own scaffolding.
    """
    if not _PYPROJECT.is_file():  # pragma: no cover - installed without sources
        return None
    text = _PYPROJECT.read_text()
    if _SDIST_TABLE not in text:  # pragma: no cover - table renamed/removed
        return None
    table = text.split(_SDIST_TABLE, 1)[1].split("\n[", 1)[0]
    match = re.search(r"include\s*=\s*\[(.*?)\]", table, re.S)
    if match is None:  # pragma: no cover - include list removed
        return None
    return re.findall(r"""["']([^"']*)["']""", match.group(1))


def test_sdist_include_patterns_are_anchored_to_the_project_root():
    # These are gitignore-style globs, so an unanchored "src" matches a src/
    # directory at ANY depth. That is not theoretical: a nested checkout left by
    # a tool (.claude/worktrees/<branch>/src, a git worktree) was swept into the
    # sdist as a second, stale copy of the whole project -- 84 extra files and
    # nearly double the archive. An explicit include list also overrides
    # hatchling's VCS-ignore default, so "git ignores it" does not save us.
    include = _sdist_include_patterns()
    if include is None:
        return
    assert include, "no sdist include patterns found -- the check reads nothing"
    unanchored = [p for p in include if not p.startswith("/")]
    assert not unanchored, (
        f"sdist include patterns must start with '/': {unanchored} would also "
        "match a directory of the same name nested anywhere in the tree"
    )


def test_py_typed_marker_ships():
    # PEP 561: without this marker downstream type-checkers (mypy/pyright) treat
    # the whole package as untyped, so the public hints -- including the RunSummary
    # TypedDict -- are invisible to users. It lives inside the package dir, so
    # hatchling ships it in the wheel automatically.
    marker = Path(airflow_pytest_operator.__file__).parent / "py.typed"
    assert marker.is_file(), "py.typed marker is missing -- downstream typing breaks"
