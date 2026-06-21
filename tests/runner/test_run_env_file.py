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


"""env_file: the runner reads a .env and merges it (os.environ < env_file < env),
with AIRFLOW* keys guarded. Shared fakes in _run_helpers."""

from __future__ import annotations

import logging
import os
import sys

import pytest
from _run_helpers import (
    _run,
    _suite,
    _write_env,
    requires_dotenv,
)

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.runners import SubprocessPytestRunner


@requires_dotenv
def test_resolve_run_env_merges_env_file(tmp_path):
    env_path = _write_env(tmp_path, "FOO=bar\nBAZ=qux\n")
    run_env = SubprocessPytestRunner()._resolve_run_env(None, env_path, False)
    assert run_env is not None
    assert run_env["FOO"] == "bar"
    assert run_env["BAZ"] == "qux"


@requires_dotenv
def test_resolve_run_env_explicit_env_wins_over_file(tmp_path):
    # env and env_file together: for a shared key the explicit env wins.
    env_path = _write_env(tmp_path, "FOO=fromfile\nONLYFILE=1\n")
    run_env = SubprocessPytestRunner()._resolve_run_env(
        {"FOO": "fromenv"}, env_path, False
    )
    assert run_env["FOO"] == "fromenv"  # explicit env beats the file
    assert run_env["ONLYFILE"] == "1"  # file-only key still applied


@requires_dotenv
def test_resolve_run_env_skips_airflow_keys_by_default(tmp_path):
    env_path = _write_env(
        tmp_path, "AIRFLOW__CORE__APO_GUARD=evil\nAIRFLOW_HOME=/evil\nMYVAR=ok\n"
    )
    run_env = SubprocessPytestRunner()._resolve_run_env(None, env_path, False)
    # A .env must never clobber the worker's Airflow wiring in the child.
    assert "AIRFLOW__CORE__APO_GUARD" not in run_env
    assert run_env.get("AIRFLOW_HOME") != "/evil"
    assert run_env["MYVAR"] == "ok"


@requires_dotenv
def test_resolve_run_env_overrides_true_passes_airflow_keys(tmp_path):
    env_path = _write_env(tmp_path, "AIRFLOW__CORE__APO_GUARD=set\nMYVAR=ok\n")
    run_env = SubprocessPytestRunner()._resolve_run_env(None, env_path, True)
    assert run_env["AIRFLOW__CORE__APO_GUARD"] == "set"  # guard lifted
    assert run_env["MYVAR"] == "ok"


@requires_dotenv
def test_resolve_run_env_drops_keys_without_value(tmp_path):
    # ``BARE`` (no ``=``) parses to None -> dropped; ``EMPTY=`` -> kept as "".
    env_path = _write_env(tmp_path, "BARE\nEMPTY=\nSET=x\n")
    run_env = SubprocessPytestRunner()._resolve_run_env(None, env_path, False)
    assert "BARE" not in run_env
    assert run_env["EMPTY"] == ""
    assert run_env["SET"] == "x"


def test_resolve_run_env_none_without_env_or_file():
    # No env_file and no env -> None, so Popen inherits os.environ directly.
    assert SubprocessPytestRunner()._resolve_run_env(None, None, False) is None
    assert SubprocessPytestRunner()._resolve_run_env({}, None, False) is None


def test_resolve_run_env_missing_file_raises(tmp_path):
    runner = SubprocessPytestRunner()
    with pytest.raises(TestExecutionError, match="env_file not found"):
        runner._resolve_run_env(None, str(tmp_path / "nope.env"), False)


def test_resolve_run_env_requires_python_dotenv(tmp_path, monkeypatch):

    env_path = _write_env(tmp_path, "FOO=bar\n")
    monkeypatch.setitem(sys.modules, "dotenv", None)  # `from dotenv import ...` fails
    with pytest.raises(TestExecutionError, match="python-dotenv"):
        SubprocessPytestRunner()._resolve_run_env(None, env_path, False)


@requires_dotenv
def test_env_file_reaches_real_subprocess(tmp_path):
    # End-to-end: env_file passed to run() lands in the child's os.environ.
    env_path = _write_env(tmp_path, "MY_FLAG=from_env_file\n")
    path = _suite(
        tmp_path,
        """
        import os
        def test_env(): assert os.environ["MY_FLAG"] == "from_env_file"
    """,
    )
    artifacts = _run(SubprocessPytestRunner(), path, env_file=env_path)
    print(f"exit_code={artifacts.exit_code}")
    assert artifacts.exit_code == 0


def test_env_file_missing_raises_from_run(tmp_path):
    # A bad env_file surfaces as TestExecutionError from run() itself.
    path = _suite(tmp_path, "def test_ok(): assert True")
    with pytest.raises(TestExecutionError, match="env_file not found"):
        _run(SubprocessPytestRunner(), path, env_file=str(tmp_path / "nope.env"))


def test_resolve_run_env_blank_env_file_treated_as_unset():
    # A whitespace-only env_file (a templating artefact) is treated as no file,
    # not a missing-file error -- mirroring how blank test targets/args are dropped.
    assert SubprocessPytestRunner()._resolve_run_env(None, "   ", False) is None
    run_env = SubprocessPytestRunner()._resolve_run_env({"A": "1"}, "  ", False)
    assert run_env is not None and run_env["A"] == "1"


@requires_dotenv
def test_resolve_run_env_parses_realistic_dotenv(tmp_path):
    # Comments, blank lines, `export `, and a quoted value with spaces.
    env_path = _write_env(
        tmp_path,
        '# a comment\n\nexport TOKEN_PATH=/secrets/t\nGREETING="hello world"\n',
    )
    run_env = SubprocessPytestRunner()._resolve_run_env(None, env_path, False)
    assert run_env["TOKEN_PATH"] == "/secrets/t"
    assert run_env["GREETING"] == "hello world"


@requires_dotenv
def test_resolve_run_env_airflow_guard_is_a_prefix_match(tmp_path):
    # The guard is a prefix match on "AIRFLOW": a key that merely CONTAINS it
    # (but doesn't start with it) is an ordinary var and is applied.
    env_path = _write_env(tmp_path, "MY_AIRFLOW_TUNE=1\nDATABASE_URL=x\n")
    run_env = SubprocessPytestRunner()._resolve_run_env(None, env_path, False)
    assert run_env["MY_AIRFLOW_TUNE"] == "1"  # not guarded (doesn't start with AIRFLOW)
    assert run_env["DATABASE_URL"] == "x"


@requires_dotenv
def test_env_and_env_file_combine_in_real_subprocess(tmp_path):
    # End-to-end precedence: env wins over env_file for a shared key; a file-only
    # key is still applied.
    env_path = _write_env(tmp_path, "SHARED=from_file\nFILE_ONLY=f\n")
    path = _suite(
        tmp_path,
        """
        import os
        def test_precedence():
            assert os.environ["SHARED"] == "from_env"
            assert os.environ["FILE_ONLY"] == "f"
    """,
    )
    artifacts = _run(
        SubprocessPytestRunner(),
        path,
        env={"SHARED": "from_env"},
        env_file=env_path,
    )
    print(f"exit_code={artifacts.exit_code}")
    assert artifacts.exit_code == 0


@requires_dotenv
def test_airflow_guard_in_real_subprocess(tmp_path, monkeypatch):
    # The .env tries to set an AIRFLOW key; the child must NOT see the file value.
    monkeypatch.delenv("AIRFLOW__CORE__APO_E2E", raising=False)
    env_path = _write_env(tmp_path, "AIRFLOW__CORE__APO_E2E=evil\nOK_VAR=ok\n")
    path = _suite(
        tmp_path,
        """
        import os
        def test_guard():
            assert os.environ.get("AIRFLOW__CORE__APO_E2E") is None
            assert os.environ["OK_VAR"] == "ok"
    """,
    )
    artifacts = _run(SubprocessPytestRunner(), path, env_file=env_path)
    assert artifacts.exit_code == 0


@pytest.mark.parametrize(
    "key",
    [
        "DB_PASSWORD",
        "API_TOKEN",
        "MY_SECRET",
        "FERNET_KEY",
        "X_CREDENTIAL",
        "USER_PASSWD",
        "DB_PWD",
        "SSH_PASSPHRASE",
        "OAUTH_AUTH",
        "TLS_PRIVATE",
        "DATABASE_URL",
        "SERVICE_URI",
        "PG_DSN",
        "AIRFLOW_CONN_PG",
    ],
)
def test_mask_env_value_covers_credential_patterns(key):
    from airflow_pytest_operator.runners.subprocess_runner import _mask_env_value

    assert _mask_env_value(key, "topsecret") == "***"


@pytest.mark.parametrize("key", ["MY_FLAG", "LANG", "PATH", "HOME", "TZ"])
def test_mask_env_value_passes_plain_keys(key):
    from airflow_pytest_operator.runners.subprocess_runner import _mask_env_value

    assert _mask_env_value(key, "plainvalue") == "plainvalue"


@requires_dotenv
def test_verbose_env_diff_includes_masked_env_file(tmp_path, caplog):
    # verbose surfaces env_file contributions in the diff, with secrets masked.
    env_path = _write_env(tmp_path, "PLAIN_VAR=visible\nAPI_TOKEN=shhh\n")
    path = _suite(tmp_path, "def test_ok(): assert True")
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        _run(SubprocessPytestRunner(verbose=True), path, env_file=env_path)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "PLAIN_VAR=visible" in text  # env_file plain var shown
    assert "API_TOKEN=***" in text  # env_file secret masked
    assert "shhh" not in text  # secret never logged


def test_verbose_masks_overridden_secret_value(caplog, monkeypatch):
    # Masking must cover the "overridden" branch too, not just "added": a secret
    # key already present in os.environ but given a new value must still be
    # masked in the diff.
    monkeypatch.setenv("MY_API_TOKEN", "old")  # present in os.environ...
    run_env = dict(os.environ)
    run_env["MY_API_TOKEN"] = "newsecret"  # ...with a changed value in the run
    runner = SubprocessPytestRunner(verbose=True)
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        runner._log_runtime_diagnostics(["python"], None, run_env, None)
    text = "\n".join(r.getMessage() for r in caplog.records)
    print(text)
    assert "overridden" in text
    assert "MY_API_TOKEN=***" in text  # overridden secret masked
    assert "newsecret" not in text  # the new value never reaches the log


def test_verbose_logs_pytest_args_verbatim_in_command(tmp_path, caplog):
    # The command (including pytest_args) is logged verbatim and NOT masked --
    # this locks in the documented contract that secrets must not be passed as
    # CLI flags (only env/env_file values are masked).
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(verbose=True)
    with caplog.at_level(
        logging.INFO, logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        _run(runner, path, pytest_args=["-k", "ok", "--token=clear-in-args"])
    text = "\n".join(r.getMessage() for r in caplog.records)
    # The flag appears in the command line as written -- not masked.
    assert "--token=clear-in-args" in text
