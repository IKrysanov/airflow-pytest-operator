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

"""Convert dotted-form node IDs back into pytest CLI selectors.

The package's parsers (:class:`JUnitResultParser`,
:class:`JSONResultParser`) emit ``failed_node_ids`` in a *dotted*
JUnit-style form: ``"tests.test_x::test_y"`` regardless of which parser
produced the result. That cross-parser parity is convenient -- downstream
Airflow tasks reading XCom never have to care about the parser -- but
it's not the form pytest accepts as a positional CLI selector. For that
pytest needs the slash form ``"tests/test_x.py::test_y"``.

This module provides the conversion one direction (dotted -> slash) so a
follow-up "retry only failed" workflow can pull the IDs from XCom and
feed them back into a fresh pytest invocation.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence


def node_id_to_pytest_args(
    node_ids: Iterable[str],
    *,
    class_prefix: str | Sequence[str] = "Test",
) -> list[str]:
    """Convert dotted-form node IDs to pytest CLI positional selectors.

    :param node_ids: iterable of dotted-form node identifiers, typically
        pulled from ``TestRunResult.failed_node_ids`` in XCom.
    :param class_prefix: one prefix string, or a sequence of prefixes,
        identifying segments that should be treated as class names rather
        than module-path components. Default ``"Test"`` matches pytest's
        own default for the ``python_classes`` config option. Pass
        ``""`` to disable class detection entirely (every segment goes
        into the module path) for suites where test classes are not used.
    :returns: list of slash-form pytest selectors. Same length as the
        input. Inputs that already look slash-form (contain ``/`` or end
        with ``.py`` in the dotted-path portion) are returned unchanged
        -- the function is idempotent. Malformed inputs (no ``::``
        separator, empty dotted-path, all-class-looking segments) are
        returned unchanged so no information is lost.
    """
    if isinstance(class_prefix, str):
        prefixes: tuple[str, ...] = (class_prefix,)
    else:
        prefixes = tuple(class_prefix)
    return [_convert_one(nid, prefixes) for nid in node_ids]


def _convert_one(node_id: str, prefixes: tuple[str, ...]) -> str:
    if "::" not in node_id:
        return node_id

    dotted_path, _, name = node_id.partition("::")
    if not dotted_path:
        # "::test_x" -- malformed JUnit. Nothing to convert.
        return node_id

    if "/" in dotted_path or dotted_path.endswith(".py"):
        return node_id

    segments = [s for s in dotted_path.split(".") if s]
    if not segments:
        return node_id

    file_parts: list[str] = []
    class_parts: list[str] = []
    for seg in segments:
        if class_parts:
            class_parts.append(seg)
        elif _looks_like_class(seg, prefixes):
            class_parts.append(seg)
        else:
            file_parts.append(seg)

    if not file_parts:
        return node_id

    slash_path = "/".join(file_parts) + ".py"
    if class_parts:
        return f"{slash_path}::{'::'.join(class_parts)}::{name}"
    return f"{slash_path}::{name}"


def _looks_like_class(segment: str, prefixes: tuple[str, ...]) -> bool:
    """True iff ``segment`` matches any configured class prefix.

    Empty prefixes are ignored: ``class_prefix=""`` collapses to a
    ``prefixes`` of ``("",)``; ``str.startswith("")`` is always True,
    which would make every segment look like a class -- the *opposite*
    of what an empty prefix should mean. The ``p and ...`` guard skips
    empty entries, so ``class_prefix=""`` correctly disables detection
    entirely.
    """
    return any(p and segment.startswith(p) for p in prefixes)
