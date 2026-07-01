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

from importlib.metadata import PackageNotFoundError, version
from typing import Any

#: Distribution name as published on PyPI / declared in pyproject.toml.
_DIST_NAME = "airflow-pytest-operator"


def _resolve_version() -> str:
    """Read the version from installed package metadata.

    ``pyproject.toml`` is the single source of truth; ``pip`` bakes that
    value into the distribution metadata at install time, and we read it
    back here so the version is never duplicated in source. Uses only the
    stdlib (``importlib.metadata``) -- no Airflow, keeping this module
    import-light for provider discovery.

    Falls back to a sentinel when the package is not installed (e.g. when
    running straight from a source checkout via ``PYTHONPATH=src``), so
    importing the package never fails just because metadata is absent.
    """
    try:
        return version(_DIST_NAME)
    except PackageNotFoundError:  # running from an uninstalled source tree
        return "0.0.0+unknown"


__version__ = _resolve_version()


def get_provider_info() -> dict[str, Any]:
    """Metadata for Airflow's provider-discovery mechanism."""
    return {
        "package-name": _DIST_NAME,
        "name": "Pytest Operator",
        "description": "Run pytest suites as Airflow tasks.",
        "versions": [__version__],
    }
