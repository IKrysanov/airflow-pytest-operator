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

"""Unit tests for the opt-in Allure wiring in conftest.py.

The hook itself is exercised end-to-end whenever the suite runs with
``ALLURE_DIR`` set; here we pin the pure decision helper's branches.
"""

from __future__ import annotations

import pytest

from conftest import _allure_dir_to_apply


def test_no_env_value_leaves_allure_untouched():
    assert _allure_dir_to_apply(None, None, True) is None
    assert _allure_dir_to_apply("", None, True) is None  # blank == unset


def test_env_value_with_plugin_returns_dir():
    assert _allure_dir_to_apply("allure-results", None, True) == "allure-results"


def test_explicit_cli_alluredir_wins_over_env():
    # A --alluredir already on the CLI (current_option set) is not overridden.
    assert _allure_dir_to_apply("from-env", "from-cli", True) is None


def test_env_value_without_plugin_raises_usage_error():
    with pytest.raises(pytest.UsageError, match="allure-pytest is not installed"):
        _allure_dir_to_apply("allure-results", None, False)


def test_missing_plugin_is_ignored_when_env_unset():
    # No opt-in -> no error even if the plugin is absent.
    assert _allure_dir_to_apply(None, None, False) is None
