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


"""env validation: keys/values must be strings, rejected at init with a clear
TypeError (not deep in os.fsencode); plus env_file/env_file_overrides
forwarding. Shared fakes in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    SequenceParser,
    _ctx,
    _RecordingCustomRunner,
    _res,
    _result,
)

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_env_bool_value_raises_type_error():
    # The reported case: env={"FLEX": True} otherwise blows up inside subprocess
    # with "expected str, bytes or os.PathLike object, not bool". Reject up front.
    with pytest.raises(TypeError, match=r"env\['FLEX'\] must be a str"):
        PytestOperator(task_id="t", test_path="tests/", env={"FLEX": True})


def test_env_non_str_value_raises_type_error():
    with pytest.raises(TypeError, match="env"):
        PytestOperator(task_id="t", test_path="tests/", env={"PORT": 8080})


def test_env_non_str_key_raises_type_error():
    with pytest.raises(TypeError, match="env keys must be str"):
        PytestOperator(task_id="t", test_path="tests/", env={1: "x"})


def test_env_non_dict_raises_type_error():
    with pytest.raises(TypeError, match="env must be a dict"):
        PytestOperator(task_id="t", test_path="tests/", env=["A=1"])


def test_env_valid_str_mapping_is_accepted():
    op = PytestOperator(task_id="t", test_path="tests/", env={"A": "1", "B": "two"})
    print(f"[env:valid] env={op.env!r}")
    assert op.env == {"A": "1", "B": "two"}


def test_env_none_defaults_to_empty_dict():
    op = PytestOperator(task_id="t", test_path="tests/")
    assert op.env == {}


def test_env_file_and_overrides_forwarded_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env={"A": "1"},  # explicit env and a file together is fine
        env_file="/cfg/test.env",
        env_file_overrides=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    call = runner.calls[0]
    print(
        f"[env_file:forward] env={call['env']} env_file={call['env_file']!r} "
        f"overrides={call['env_file_overrides']}"
    )
    # The operator forwards all three verbatim; precedence/merge is the runner's
    # job (os.environ < env_file < env), tested in test_subprocess_runner.py.
    assert call["env"] == {"A": "1"}
    assert call["env_file"] == "/cfg/test.env"
    assert call["env_file_overrides"] is True


def test_env_file_defaults_forwarded_as_none_and_false():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    call = runner.calls[0]
    assert call["env_file"] is None
    assert call["env_file_overrides"] is False


def test_env_file_is_a_template_field():
    # Templated so the path can depend on the environment/run.
    assert "env_file" in PytestOperator.template_fields


def test_env_file_forwarded_in_dry_run():
    # dry_run still runs pytest (--collect-only), so env_file must be forwarded.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env_file="/cfg/.env",
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    assert runner.calls[0]["env_file"] == "/cfg/.env"
    assert runner.calls[0]["pytest_args"][-1] == "--collect-only"


def test_env_file_forwarded_on_every_rerun():
    # rerun_failed re-invokes run(); env_file must travel to each rerun too.
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=1),
            _res(["tests.test_x::test_a"], passed=0),
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env_file="/cfg/.env",
        rerun_failed=1,
        runner=runner,
        parser=parser,
        fail_on_test_failure=False,
    )
    op.execute(_ctx())
    print(f"[env_file:reruns] calls={len(runner.calls)}")
    assert len(runner.calls) == 2  # full run + 1 rerun
    assert all(c["env_file"] == "/cfg/.env" for c in runner.calls)


def test_custom_runner_receives_env_file_through_operator():
    runner = _RecordingCustomRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        env_file="/cfg/.env",
        env_file_overrides=True,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    assert runner.calls[0] == {"env_file": "/cfg/.env", "env_file_overrides": True}
