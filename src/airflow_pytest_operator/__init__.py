"""Run pytest suites as Airflow tasks.

Public API:
    PytestOperator         — the operator to use in DAGs
    PytestRunner           — runner interface (extend for docker/k8s)
    SubprocessPytestRunner — default runner
    ResultParser           — parser interface
    JUnitResultParser      — default parser (JUnit XML)
    JSONResultParser       — parser for pytest-json-report output
    ReportRequest          — parser-declared pytest invocation spec
    TestRunResult          — structured result model
    node_id_to_pytest_args — convert dotted failed_node_ids back to
                             pytest CLI selectors (for retry-failed-only
                             workflows)
    LastFailedStore        — structural (Protocol) interface for a custom
                             ``failed_only`` cross-retry store
    VariableLastFailedStore — Airflow-Variable store backing the single-operator
                             ``test_retry_strategy="failed_only"`` retry mode
    last_failed_var_key    — derive the Variable key a task instance uses for
                             its failed set (for inspection / cleanup)
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

from typing import TYPE_CHECKING

from .exceptions import (
    AirflowPytestError,
    ReportParseError,
    TestExecutionError,
    TestsFailedError,
)
from .models import CaseResult, ReportRequest, RunArtifacts, TestRunResult
from .provider_info import __version__ as __version__
from .provider_info import get_provider_info as get_provider_info
from .reporters import JSONResultParser, JUnitResultParser, ResultParser
from .runners import PytestRunner, SubprocessPytestRunner
from .stores import LastFailedStore, VariableLastFailedStore, last_failed_var_key
from .utils import node_id_to_pytest_args

if TYPE_CHECKING:
    # PytestOperator is exposed lazily via __getattr__ (see below) so that
    # importing this package does not eagerly import Airflow. Re-declaring it
    # here under TYPE_CHECKING lets mypy and IDEs resolve the name without
    # triggering the runtime import.
    from .operators import PytestOperator as PytestOperator


__all__ = [
    "PytestOperator",
    "PytestRunner",
    "SubprocessPytestRunner",
    "ResultParser",
    "JUnitResultParser",
    "JSONResultParser",
    "ReportRequest",
    "TestRunResult",
    "RunArtifacts",
    "CaseResult",
    "AirflowPytestError",
    "TestExecutionError",
    "ReportParseError",
    "TestsFailedError",
    "get_provider_info",
    "node_id_to_pytest_args",
    "LastFailedStore",
    "VariableLastFailedStore",
    "last_failed_var_key",
]


def __getattr__(name: str) -> object:
    # Lazy import: PytestOperator pulls in the Airflow compat shim which
    # imports BaseOperator. Deferring this to first access means that
    # Airflow's provider-discovery (which imports this module at startup
    # to call get_provider_info) does NOT immediately trigger the Airflow
    # import chain. This prevents a broken/mismatched SDK from crashing
    # the entire Airflow worker process on startup.
    if name == "PytestOperator":
        from .operators import PytestOperator

        return PytestOperator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
