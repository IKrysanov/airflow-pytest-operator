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

from pathlib import Path

import airflow_pytest_operator


def test_py_typed_marker_ships():
    # PEP 561: without this marker downstream type-checkers (mypy/pyright) treat
    # the whole package as untyped, so the public hints -- including the RunSummary
    # TypedDict -- are invisible to users. It lives inside the package dir, so
    # hatchling ships it in the wheel automatically.
    marker = Path(airflow_pytest_operator.__file__).parent / "py.typed"
    assert marker.is_file(), "py.typed marker is missing -- downstream typing breaks"
