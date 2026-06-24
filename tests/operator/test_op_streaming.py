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

"""stream_output: live per-line pytest output instead of one end-of-run blob.
Shared fakes in _op_helpers."""

from __future__ import annotations

from unittest import mock

from _op_helpers import (
    FakeParser,
    FakeRunner,
    SequenceParser,
    _ctx,
    _res,
    _result,
)

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_stream_output_default_is_true():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[stream:default] stream_output={op.stream_output!r}")
    assert op.stream_output is True


def test_streaming_passes_sink_to_runner():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    print(f"[stream:wire] on_output={runner.calls[0]['on_output']!r}")
    assert runner.calls[0]["on_output"] is not None


def test_no_streaming_passes_no_sink():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        stream_output=False,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    assert runner.calls[0]["on_output"] is None


def test_streamed_lines_route_stdout_info_stderr_warning():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    runner.stream_lines = [("out-line", "stdout"), ("err-line", "stderr")]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with (
        mock.patch.object(op.log, "info") as info,
        mock.patch.object(op.log, "warning") as warning,
    ):
        op.execute(_ctx())
    info_msgs = " ".join(str(c) for c in info.call_args_list)
    warn_msgs = " ".join(str(c) for c in warning.call_args_list)
    print(
        f"[stream:route] info={'out-line' in info_msgs} warn={'err-line' in warn_msgs}"
    )
    assert "out-line" in info_msgs
    assert "err-line" in warn_msgs


def test_streaming_skips_the_end_of_run_blob():
    # With streaming on, lines are logged live -> the end-of-run blob would be a
    # duplicate, so it must NOT be logged (here stream_lines is empty, so the
    # blob's stdout text must not appear at all).
    runner = FakeRunner(
        RunArtifacts(
            exit_code=0,
            report_path="/x.xml",
            stdout="blob-stdout-text",
            stderr="blob-stderr-text",
        )
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    with (
        mock.patch.object(op.log, "info") as info,
        mock.patch.object(op.log, "warning") as warning,
    ):
        op.execute(_ctx())
    logged = " ".join(str(c) for c in info.call_args_list + warning.call_args_list)
    print(f"[stream:noblob] {logged[:200]!r}")
    assert "pytest stdout:" not in logged
    assert "blob-stdout-text" not in logged


def test_emit_pytest_line_routes_levels():
    op = PytestOperator(task_id="t", test_path="tests/")
    with (
        mock.patch.object(op.log, "info") as info,
        mock.patch.object(op.log, "warning") as warning,
    ):
        op._emit_pytest_line("hello", "stdout")
        op._emit_pytest_line("oops", "stderr")
    assert any("hello" in str(c) for c in info.call_args_list)
    assert any("oops" in str(c) for c in warning.call_args_list)


def test_streaming_applied_to_every_rerun_round():
    # The sink is wired for the first full run AND each in-process rerun.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    print(f"[stream:reruns] calls={len(runner.calls)}")
    assert len(runner.calls) == 2
    assert all(call["on_output"] is not None for call in runner.calls)
