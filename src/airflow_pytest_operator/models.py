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

from dataclasses import dataclass
from typing import Any


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
        """Reconstruct a pytest-style node id when possible."""
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
