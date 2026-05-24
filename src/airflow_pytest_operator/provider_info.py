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

"""Airflow provider-discovery metadata.

This module is deliberately import-light: it pulls in *nothing* from the
rest of the package (no operator, no compat shim, no Airflow imports).

Airflow's provider manager imports the ``get_provider_info`` entry point
very early during startup, before Airflow itself is fully initialized. If
that entry point lived in ``__init__.py`` -- which eagerly imports the
operator and therefore ``airflow.sdk.bases.operator`` -- the early import
triggers a circular import on newer Airflow (3.2.x), surfacing as
``partially initialized module ... has no attribute 'get_provider_info'``.
Keeping the entry point here, with no heavy imports, breaks that cycle.
"""

from __future__ import annotations

from typing import Any

# Kept in sync with pyproject.toml / __init__.__version__. Defined locally so
# this module never has to import the package root.
__version__ = "0.2.1"


def get_provider_info() -> dict[str, Any]:
    """Metadata for Airflow's provider-discovery mechanism."""
    return {
        "package-name": "airflow-pytest-operator",
        "name": "Pytest Operator",
        "description": "Run pytest suites as Airflow tasks.",
        "versions": [__version__],
    }
