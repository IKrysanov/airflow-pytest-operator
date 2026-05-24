"""Run pytest suites as Airflow tasks.

Public API:
    PytestOperator         — the operator to use in DAGs
    PytestRunner           — runner interface (extend for docker/k8s)
    SubprocessPytestRunner — default runner
    ResultParser           — parser interface
    JUnitResultParser      — default parser
    TestRunResult          — structured result model
"""

# Copyright 2026 Ilya Krysanov
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

from typing import Any

from .exceptions import (
    AirflowPytestError,
    ReportParseError,
    TestExecutionError,
    TestsFailedError,
)
from .models import CaseResult, RunArtifacts, TestRunResult
from .operators import PytestOperator
from .reporters import JUnitResultParser, ResultParser
from .runners import PytestRunner, SubprocessPytestRunner

__version__ = "0.2.0"


def get_provider_info() -> dict[str, Any]:
    """Metadata for Airflow's provider-discovery mechanism.

    Lets Airflow's CLI/UI list this package as a provider. Optional —
    operators work via plain imports regardless — but it makes the
    package a well-behaved citizen if published.
    """
    return {
        "package-name": "airflow-pytest-operator",
        "name": "Pytest Operator",
        "description": "Run pytest suites as Airflow tasks.",
        "versions": [__version__],
    }


__all__ = [
    "PytestOperator",
    "PytestRunner",
    "SubprocessPytestRunner",
    "ResultParser",
    "JUnitResultParser",
    "TestRunResult",
    "RunArtifacts",
    "CaseResult",
    "AirflowPytestError",
    "TestExecutionError",
    "ReportParseError",
    "TestsFailedError",
]
