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


"""verbose=True runtime diagnostics (command, cwd, masked env-diff, report dir).
Shared fakes in _run_helpers."""

from __future__ import annotations

import logging
import os

from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_mask_env_value_masks_credentials_and_passes_plain():
    from airflow_pytest_operator.runners.subprocess_runner import _mask_env_value

    # Credential-looking keys -> masked.
    assert _mask_env_value("MY_PASSWORD", "hunter2") == "***"
    assert _mask_env_value("API_TOKEN", "abc") == "***"
    assert _mask_env_value("DB_SECRET", "s") == "***"
    assert _mask_env_value("AIRFLOW__CORE__FERNET_KEY", "k") == "***"
    # The key whose NAME is innocuous but VALUE holds a connection URI.
    assert _mask_env_value("AIRFLOW_CONN_PG", "postgres://u:pw@h/db") == "***"
    assert _mask_env_value("DATABASE_URL", "postgres://u:pw@h/db") == "***"
    # Plain keys pass through unchanged.
    assert _mask_env_value("MY_FLAG", "42") == "42"
    assert _mask_env_value("LANG", "en_US.UTF-8") == "en_US.UTF-8"


def test_verbose_diagnostics_log_command_cwd_and_mask_secret(caplog):
    runner = SubprocessPytestRunner(verbose=True)
    run_env = dict(os.environ)
    run_env["AIRFLOW_CONN_PG"] = "postgres://user:supersecret@host/db"  # added, secret
    run_env["MY_FLAG"] = "42"  # added, plain
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner._log_runtime_diagnostics(
            ["python", "-m", "pytest", "tests/"],
            "/work/dir",
            run_env,
            "/tmp/report",
        )
    text = "\n".join(r.getMessage() for r in caplog.records)
    print(text)
    assert "command: python -m pytest tests/" in text
    assert "cwd: /work/dir" in text
    assert "cleanup='always'" in text
    assert "MY_FLAG=42" in text  # plain value shown
    assert "AIRFLOW_CONN_PG=***" in text  # masked
    # Security regression: the secret must never reach the log in clear text.
    assert "supersecret" not in text


def test_verbose_run_logs_runtime_diagnostics(tmp_path, caplog):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(verbose=True)
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        _run(runner, path, env={"MY_FLAG": "42"})
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "runtime diagnostics -- command:" in text
    assert "MY_FLAG=42" in text  # the added var shows up in the env-diff


def test_verbose_false_emits_no_runtime_diagnostics(tmp_path, caplog):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner()  # verbose defaults to False
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        _run(runner, path)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "runtime diagnostics" not in text


def test_verbose_diagnostics_reports_overridden_key(caplog):
    # An entry that changes a value already present in os.environ shows up under
    # "overridden", not "added".
    runner = SubprocessPytestRunner(verbose=True)
    existing = next(iter(os.environ))  # any inherited key
    run_env = dict(os.environ)
    run_env[existing] = os.environ[existing] + "_CHANGED"
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner._log_runtime_diagnostics(["python"], None, run_env, None)
    text = "\n".join(r.getMessage() for r in caplog.records)
    print(text)
    assert "cwd: <inherited from worker>" in text
    assert f"env overridden vs os.environ (1): {existing}=" in text


def test_verbose_diagnostics_no_env_overrides(caplog):
    # run_env=None means the child inherits os.environ unchanged.
    runner = SubprocessPytestRunner(verbose=True)
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner._log_runtime_diagnostics(["python", "-m", "pytest"], "/x", None, None)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "env: inherits os.environ unchanged" in text
    assert "report_dir: <none>" in text
