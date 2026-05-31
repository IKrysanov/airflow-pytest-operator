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

"""Provider-discovery and lazy-import behavior.

Regression coverage for the Airflow 3.2.x startup crash: provider
discovery imports this package (to call get_provider_info) *during*
Airflow's own config initialization, before the Task SDK is ready. If the
package eagerly imported the operator -> compat -> airflow.sdk at that
point, the whole worker would crash. These tests pin the two properties
that prevent it: a heavy-import-free entry point, and a lazy operator.
"""

from __future__ import annotations

import subprocess
import sys


def test_provider_info_is_import_light():
    # The provider_info module must not pull in the operator/compat/Airflow.
    # We import it in a fresh interpreter and assert none of the heavy
    # modules ended up loaded as a side effect.
    code = (
        "import airflow_pytest_operator.provider_info as p;"
        "import sys;"
        "info = p.get_provider_info();"
        "assert info['package-name'] == 'airflow-pytest-operator', info;"
        "heavy = ["
        "  'airflow_pytest_operator.operators.pytest_operator',"
        "  'airflow_pytest_operator.compat.airflow',"
        "];"
        "loaded = [m for m in heavy if m in sys.modules];"
        "assert not loaded, f'provider_info pulled in heavy modules: {loaded}';"
        "print('OK')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_importing_package_does_not_import_operator():
    # Importing the top-level package (as Airflow's discovery does) must not
    # eagerly import the operator -- that would trigger the Airflow import
    # chain at startup and crash on a not-yet-ready SDK (Airflow 3.2.x).
    code = (
        "import airflow_pytest_operator;"
        "import sys;"
        "mod = 'airflow_pytest_operator.operators.pytest_operator';"
        "assert mod not in sys.modules, 'operator was eagerly imported';"
        "assert hasattr(airflow_pytest_operator, 'get_provider_info');"
        "print('OK')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_version_is_resolved_from_metadata():
    # __version__ comes from installed package metadata (single source of
    # truth = pyproject.toml), not a hardcoded string.
    import airflow_pytest_operator

    v = airflow_pytest_operator.__version__
    assert isinstance(v, str) and v, "version must be a non-empty string"
    # provider_info exposes the same value.
    from airflow_pytest_operator.provider_info import (
        __version__ as pi_version,
    )
    from airflow_pytest_operator.provider_info import get_provider_info

    assert pi_version == v
    assert get_provider_info()["versions"] == [v]


def test_version_fallback_when_not_installed(monkeypatch):
    # If metadata is missing (running from an uninstalled source tree), the
    # resolver must not raise -- it returns a sentinel instead.
    import importlib.metadata as md

    import airflow_pytest_operator

    def _raise(_name):
        raise md.PackageNotFoundError

    monkeypatch.setattr(airflow_pytest_operator.provider_info, "version", _raise)
    assert airflow_pytest_operator.provider_info._resolve_version() == "0.0.0+unknown"


def test_lazy_operator_attribute_resolves():
    # Accessing PytestOperator triggers the lazy import and returns the class.
    import airflow_pytest_operator

    op_cls = airflow_pytest_operator.PytestOperator
    assert op_cls.__name__ == "PytestOperator"


def test_unknown_attribute_raises_attribute_error():
    import airflow_pytest_operator

    try:
        _ = airflow_pytest_operator.DefinitelyNotARealName
    except AttributeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected AttributeError for unknown attribute")
