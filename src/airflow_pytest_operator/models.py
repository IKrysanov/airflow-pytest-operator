"""Domain models for test runs.

These are plain, framework-agnostic dataclasses. Nothing here imports
Airflow, pytest, or subprocess — keeping the domain layer dependency-free
makes it trivial to unit-test parsers and the operator in isolation (DIP).
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

from dataclasses import asdict, dataclass, field
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
class RunArtifacts:
    """Everything a Runner produces: where to find outputs + the exit code.

    A Runner's job ends at producing files; interpreting them is the
    Parser's job. This separation is what lets us swap runners
    (subprocess, docker, k8s-pod) without touching parsing logic.
    """

    exit_code: int
    junit_xml_path: str | None
    stdout: str = ""
    stderr: str = ""
    working_dir: str | None = None


@dataclass(frozen=True)
class TestRunResult:
    """Structured, serializable result of a pytest run."""

    __test__ = False  # not a pytest test class despite the name

    total: int
    passed: int
    failed: int
    skipped: int
    errors: int
    duration: float
    exit_code: int
    cases: list[CaseResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.failed == 0 and self.errors == 0

    @property
    def failed_node_ids(self) -> list[str]:
        return [c.node_id for c in self.cases if c.outcome in ("failed", "error")]

    def to_xcom(self) -> dict[str, Any]:
        """A compact, JSON-serializable dict suitable for XCom.

        We deliberately drop per-case ``message`` blobs from the summary
        pushed to XCom (they can be huge); full detail stays in logs.
        """
        data = asdict(self)
        data.pop("cases", None)
        data["success"] = self.success
        data["failed_node_ids"] = self.failed_node_ids
        return data
