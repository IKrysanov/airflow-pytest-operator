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

"""Live output streaming: the on_output sink, the -u flag, and cap sharing.
Shared fakes in _run_helpers."""

from __future__ import annotations

from _run_helpers import _run, _suite

from airflow_pytest_operator.runners import SubprocessPytestRunner

_LOGGER = "airflow_pytest_operator.runners.subprocess_runner"


def test_run_streams_lines_to_sink(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_a(): assert True
        def test_b(): assert True
    """,
    )
    lines: list[tuple[str, str]] = []
    art = _run(
        SubprocessPytestRunner(),
        path,
        pytest_args=["-v"],  # one newline-terminated line per test -> streams
        on_output=lambda line, stream: lines.append((stream, line)),
    )
    print(f"[stream] {len(lines)} live lines; first={lines[0] if lines else None!r}")
    assert lines, "expected lines streamed through the sink"
    assert all(stream in ("stdout", "stderr") for stream, _ in lines)
    # Lines have no trailing newline, blank spacer lines are skipped, and the
    # full output is still captured.
    assert all(not line.endswith("\n") for _, line in lines)
    assert all(line for _, line in lines), "blank lines should not be streamed"
    assert art.stdout
    streamed_stdout = "\n".join(line for stream, line in lines if stream == "stdout")
    assert "test_a" in streamed_stdout or "test_a" in art.stdout


def test_run_without_sink_does_not_stream_but_still_captures(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    art = _run(SubprocessPytestRunner(), path)  # no on_output
    print(f"[stream:none] exit={art.exit_code} captured_len={len(art.stdout)}")
    assert art.exit_code == 0
    assert art.stdout  # captured exactly as before -- back-compat


def test_run_inserts_dash_u_when_streaming(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    with caplog.at_level("INFO", logger=_LOGGER):
        _run(
            SubprocessPytestRunner(verbose=True),
            path,
            on_output=lambda line, stream: None,
        )
    cmds = [r.getMessage() for r in caplog.records if "command:" in r.getMessage()]
    print(f"[stream:-u] {cmds}")
    assert any(" -u -m pytest" in m for m in cmds)


def test_run_omits_dash_u_without_streaming(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    with caplog.at_level("INFO", logger=_LOGGER):
        _run(SubprocessPytestRunner(verbose=True), path)  # no on_output
    cmds = [r.getMessage() for r in caplog.records if "command:" in r.getMessage()]
    assert cmds and all(" -u -m pytest" not in m for m in cmds)
    assert any("-m pytest" in m for m in cmds)


def test_streaming_respects_the_output_cap(tmp_path):
    # A tiny cap bounds the captured blob AND the streamed text identically:
    # the live emit shares the same byte budget as accumulation.
    path = _suite(
        tmp_path,
        """
        def test_a():
            print("X" * 5000)
            assert True
    """,
    )
    streamed: list[str] = []
    art = _run(
        SubprocessPytestRunner(max_output_bytes=200),
        path,
        pytest_args=["-s"],  # don't let pytest capture the print
        on_output=lambda line, stream: streamed.append(line),
    )
    total = sum(len(line) for line in streamed)
    print(f"[stream:cap] streamed_chars={total} captured={len(art.stdout)}")
    # The cap was hit (captured blob carries the truncation marker) ...
    assert "truncated at 200" in art.stdout
    # ... and the live-streamed text honours the same 200-char budget, and the
    # truncation marker is only on the captured blob, never streamed.
    assert total <= 200
    assert all("truncated" not in line for line in streamed)


def test_bad_sink_does_not_break_draining(tmp_path):
    # A sink that raises must not wedge the drainer (which would block the child
    # on a full pipe) -- the full output is still captured.
    path = _suite(
        tmp_path,
        """
        def test_a(): assert True
        def test_b(): assert True
    """,
    )

    def boom(line: str, stream: str) -> None:
        raise RuntimeError("sink boom")

    art = _run(SubprocessPytestRunner(), path, pytest_args=["-v"], on_output=boom)
    print(f"[stream:badsink] exit={art.exit_code} captured_len={len(art.stdout)}")
    assert art.exit_code == 0
    assert art.stdout  # draining continued despite the raising sink
