"""Tests for the Airflow compatibility shim.

The shim resolves ``BaseOperator`` across Airflow 2.x/3.x import locations
and provides ``get_airflow_version`` and an ``apply_defaults`` passthrough.
We drive each resolution branch by injecting fake modules into
``sys.modules`` so the tests are deterministic regardless of which Airflow
(if any) is installed in the environment. Helper functions are exercised
directly rather than relying on the module's import-time resolution.
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

import sys
import types

import pytest

from airflow_pytest_operator.compat import airflow as compat


def _fake_module(name: str, **attrs: object) -> types.ModuleType:
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def test_get_airflow_version_parses_version(monkeypatch):
    # Inject a fake airflow.version and confirm we parse the numeric
    # (major, minor, patch) tuple. The parser keeps *all* digits within a
    # dotted chunk, so a clean "3.0.6" yields (3, 0, 6).
    compat.get_airflow_version.cache_clear()
    ver_mod = _fake_module("airflow.version", version="3.0.6")
    monkeypatch.setitem(sys.modules, "airflow.version", ver_mod)
    try:
        assert compat.get_airflow_version() == (3, 0, 6)
    finally:
        compat.get_airflow_version.cache_clear()


def test_get_airflow_version_strips_nonnumeric_suffix(monkeypatch):
    # A pre-release like "2.10.3.dev0" is truncated to three dotted chunks
    # and digits are extracted from each, so we still get a clean tuple.
    compat.get_airflow_version.cache_clear()
    ver_mod = _fake_module("airflow.version", version="2.10.3.dev0")
    monkeypatch.setitem(sys.modules, "airflow.version", ver_mod)
    try:
        assert compat.get_airflow_version() == (2, 10, 3)
    finally:
        compat.get_airflow_version.cache_clear()


def test_get_airflow_version_handles_nonnumeric_chunks(monkeypatch):
    compat.get_airflow_version.cache_clear()
    ver_mod = _fake_module("airflow.version", version="2.x.0")
    monkeypatch.setitem(sys.modules, "airflow.version", ver_mod)
    try:
        # Non-numeric chunk -> 0 in that slot, rest parsed normally.
        assert compat.get_airflow_version() == (2, 0, 0)
    finally:
        compat.get_airflow_version.cache_clear()


def test_get_airflow_version_returns_zero_when_unavailable(monkeypatch):
    # If airflow.version cannot be imported, fall back to (0,).
    compat.get_airflow_version.cache_clear()
    monkeypatch.setitem(sys.modules, "airflow.version", None)
    try:
        assert compat.get_airflow_version() == (0,)
    finally:
        compat.get_airflow_version.cache_clear()


def test_import_base_operator_prefers_sdk_bases_operator(monkeypatch):
    # Step 1: canonical Airflow 3 location wins when present.
    class _Base:
        pass

    bases_op = _fake_module("airflow.sdk.bases.operator", BaseOperator=_Base)
    monkeypatch.setitem(sys.modules, "airflow.sdk.bases.operator", bases_op)
    assert compat._import_base_operator() is _Base


def test_import_base_operator_falls_back_to_sdk_top_level(monkeypatch):
    # Step 2: when the canonical path is unimportable, the top-level
    # airflow.sdk re-export is used.
    class _Base:
        pass

    # Make step 1 fail explicitly.
    monkeypatch.setitem(sys.modules, "airflow.sdk.bases.operator", None)
    sdk = _fake_module("airflow.sdk", BaseOperator=_Base)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    assert compat._import_base_operator() is _Base


def test_import_base_operator_falls_back_to_models_baseoperator(monkeypatch):
    # Step 3: Airflow 2.x location, used only when both SDK paths fail.
    class _Base:
        pass

    monkeypatch.setitem(sys.modules, "airflow.sdk.bases.operator", None)
    monkeypatch.setitem(sys.modules, "airflow.sdk", None)
    models_mod = _fake_module("airflow.models.baseoperator", BaseOperator=_Base)
    monkeypatch.setitem(sys.modules, "airflow.models.baseoperator", models_mod)
    assert compat._import_base_operator() is _Base


def test_import_base_operator_raises_diagnostic_when_all_fail(monkeypatch):
    # When every known path is unimportable, a single diagnostic ImportError
    # is raised that lists each attempted path -- not a confusing internal
    # traceback from Airflow's deprecation shim.
    monkeypatch.setitem(sys.modules, "airflow.sdk.bases.operator", None)
    monkeypatch.setitem(sys.modules, "airflow.sdk", None)
    monkeypatch.setitem(sys.modules, "airflow.models.baseoperator", None)
    with pytest.raises(ImportError) as excinfo:
        compat._import_base_operator()
    msg = str(excinfo.value)
    assert "Attempted paths" in msg
    assert "airflow.sdk.bases.operator" in msg
    assert "airflow.models.baseoperator" in msg


def test_apply_defaults_uses_airflow_decorator_when_present(monkeypatch):
    # Step where airflow.utils.decorators.apply_defaults exists: we return it.
    sentinel = object()
    decorators = _fake_module("airflow.utils.decorators", apply_defaults=sentinel)
    monkeypatch.setitem(sys.modules, "airflow.utils.decorators", decorators)
    assert compat._import_apply_defaults() is sentinel


def test_apply_defaults_passthrough_when_absent(monkeypatch):
    # When apply_defaults is unavailable (Airflow 3.x), we return a
    # passthrough decorator that leaves the function unchanged.
    monkeypatch.setitem(sys.modules, "airflow.utils.decorators", None)
    passthrough = compat._import_apply_defaults()

    def my_func():
        return 42

    wrapped = passthrough(my_func)
    assert wrapped is my_func
    assert wrapped() == 42
