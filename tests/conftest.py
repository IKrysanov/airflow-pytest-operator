"""Test bootstrap.

If Airflow is not installed, we register a *minimal* stub of
``airflow.sdk.BaseOperator`` so the operator can be imported and its
orchestration tested in isolation. When Airflow *is* installed, this
stub is skipped and the real class is used. This keeps the unit-test
suite fast and dependency-light while remaining faithful to the real
operator contract (it only relies on ``self.log`` and ``task_id``).
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

from __future__ import annotations

import importlib.util
import logging
import sys
import types


def _airflow_available() -> bool:
    return importlib.util.find_spec("airflow") is not None


if not _airflow_available():
    # Build a fake `airflow.sdk` module exposing a BaseOperator stub.
    airflow_mod = types.ModuleType("airflow")
    sdk_mod = types.ModuleType("airflow.sdk")

    class _StubBaseOperator:
        def __init__(self, *, task_id: str, do_xcom_push: bool = True, **kwargs):
            self.task_id = task_id
            # Mirror the real BaseOperator: do_xcom_push defaults to True
            # and is stored as an attribute that subclasses may override.
            self.do_xcom_push = do_xcom_push
            self.log = logging.getLogger(f"stub.operator.{task_id}")

    sdk_mod.BaseOperator = _StubBaseOperator
    airflow_mod.sdk = sdk_mod

    sys.modules.setdefault("airflow", airflow_mod)
    sys.modules.setdefault("airflow.sdk", sdk_mod)
