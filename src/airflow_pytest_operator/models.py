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

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

# A live sink for child output lines. Called once per line as it is drained,
# with the trailing newline stripped, as ``sink(line, stream)`` where ``stream``
# is ``"stdout"`` or ``"stderr"``. Used to stream pytest output to the task log
# in real time instead of one blob at the end.
OutputSink: TypeAlias = Callable[[str, str], None]


@dataclass(frozen=True)
class CaseResult:
    """A single test case outcome parsed from a report."""

    name: str
    classname: str
    time: float
    outcome: str  # "passed" | "failed" | "error" | "skipped"
    message: str | None = None

    @property
    def node_id(self) -> str:
        """Reconstruct a node id in the package's dotted, cross-parser form.

        Neither report format hands us a ready-to-run pytest node id: the
        JUnit XML carries only ``classname`` (a dotted module/class path such
        as ``tests.test_x`` or ``tests.test_x.TestThings``) plus ``name``,
        and the JSON parser deliberately splits its native slash-form nodeid
        into the *same* ``classname``/``name`` shape (see
        ``JSONResultParser._split_nodeid``) so both parsers emit identical IDs.
        We therefore reconstruct ``"{classname}::{name}"`` -- e.g.
        ``tests.test_x::test_y`` -- which is the canonical dotted form used by
        ``failed_node_ids`` and XCom.

        Note this is **not** a string pytest accepts as a CLI selector (that
        needs the slash form ``tests/test_x.py::test_y``); convert it with
        :func:`airflow_pytest_operator.node_id_to_pytest_args` for a
        "retry only failed" workflow. Parametrized cases keep their id
        (``name`` is e.g. ``test_y[1]``), so they survive the round trip.
        When ``classname`` is empty the bare ``name`` is returned.
        """
        if self.classname:
            return f"{self.classname}::{self.name}"
        return self.name


@dataclass(frozen=True)
class ReportRequest:
    """What a parser needs pytest to produce.

    A parser declares two things:
      * ``pytest_args``  -- CLI tokens to splice into the pytest invocation
        so it emits a report the parser can consume (e.g. ``--junitxml=...``
        for JUnit, ``--json-report --json-report-file=...`` for JSON);
      * ``report_path``  -- the path on disk where that report will land.
        ``None`` means no report file is expected (the runner will return
        ``RunArtifacts.report_path=None`` accordingly).

    The runner is handed this request by the operator before launching
    pytest. It splices the args verbatim and, on completion, returns the
    declared ``report_path`` in :class:`RunArtifacts` (or ``None`` if the
    file is missing). The runner never interprets the args -- this is what
    keeps it format-agnostic and what makes "add a new format by adding a
    parser, not by editing the runner" actually true.
    """

    pytest_args: tuple[str, ...]
    report_path: str | None


@dataclass(frozen=True)
class RunArtifacts:
    """Everything a Runner produces: where to find outputs + the exit code.

    A Runner's job ends at producing files; interpreting them is the
    Parser's job. This separation is what lets us swap runners
    (subprocess, docker, k8s-pod) without touching parsing logic.

    ``report_path`` is whatever path the parser declared via its
    :class:`ReportRequest` -- the runner only checks the file exists and
    passes it through; it does not assume a format.
    """

    exit_code: int
    report_path: str | None
    stdout: str = ""
    stderr: str = ""
    working_dir: str | None = None


@dataclass(frozen=True)
class TestRunResult:
    """Aggregated result of one pytest run, mapped from a parsed report."""

    total: int
    passed: int
    failed: int
    skipped: int
    errors: int
    duration: float
    exit_code: int
    cases: tuple[CaseResult, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.cases, tuple):
            object.__setattr__(self, "cases", tuple(self.cases))

    @property
    def success(self) -> bool:
        return self.failed == 0 and self.errors == 0 and self.exit_code == 0

    @property
    def failed_node_ids(self) -> list[str]:
        return [c.node_id for c in self.cases if c.outcome in ("failed", "error")]

    def to_xcom(self) -> dict[str, Any]:
        """A compact, JSON-serializable dict suitable for XCom."""
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration": self.duration,
            "exit_code": self.exit_code,
            "success": self.success,
            "failed_node_ids": self.failed_node_ids,
        }
