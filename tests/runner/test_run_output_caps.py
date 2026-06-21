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


"""Output capping/truncation of drained stdout/stderr. Shared fakes in
_run_helpers."""

from __future__ import annotations

import pytest
from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.runners import SubprocessPytestRunner


@pytest.mark.parametrize("bad", [0, -1, -1024])
def test_runner_rejects_non_positive_max_output_bytes(bad):
    with pytest.raises(ValueError, match="max_output_bytes"):
        SubprocessPytestRunner(max_output_bytes=bad)


def test_runner_truncates_stdout_when_cap_exceeded(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        def test_noisy():
            # ~200 KiB of stdout, an order of magnitude above the cap.
            for i in range(2000):
                print('x' * 100)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(
        f"[truncate] exit={artifacts.exit_code} "
        f"stdout_len={len(artifacts.stdout)} cap={cap}"
    )
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert "stdout truncated at" in artifacts.stdout
    assert len(artifacts.stdout.encode("utf-8")) <= cap + 1024


def test_runner_truncates_stderr_when_cap_exceeded(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        import sys
        def test_noisy_stderr():
            for i in range(2000):
                print('e' * 100, file=sys.stderr)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert "stderr truncated at" in artifacts.stderr
    assert len(artifacts.stderr.encode("utf-8")) <= cap + 1024


def test_runner_caps_single_oversized_chunk_at_exact_limit(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        def test_one_huge_line():
            # A single ~1 MiB line -> one readline() chunk, ~250x the cap. With
            # the old pre-append check this whole chunk would be captured.
            print('y' * 1000000)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    assert artifacts.exit_code == 0
    assert "stdout truncated at" in artifacts.stdout

    marker = "\n...(stdout truncated"
    body = artifacts.stdout[: artifacts.stdout.index(marker)]
    print(f"[overshoot] body_len={len(body)} cap={cap}")
    # The captured body (everything before the marker) is clamped to exactly
    # the cap -- not cap + one giant chunk.
    assert len(body) == cap


def test_truncation_marker_reports_characters_unit(tmp_path):
    cap = 2048
    path = _suite(
        tmp_path,
        """
        def test_noisy():
            for _ in range(1000):
                print('z' * 100)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=cap)
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(f"[marker] tail={artifacts.stdout[-120:]!r}")
    assert f"stdout truncated at {cap} characters" in artifacts.stdout
    # The old "~N chars" phrasing is gone.
    assert "chars;" not in artifacts.stdout


def test_runner_does_not_truncate_when_cap_disabled(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_quiet():
            print('hello-from-child')
            assert True
        """,
    )
    runner = SubprocessPytestRunner(max_output_bytes=None)
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(f"[no-cap] stdout_len={len(artifacts.stdout)}")
    assert artifacts.exit_code == 0
    assert "hello-from-child" in artifacts.stdout
    assert "truncated" not in artifacts.stdout
    assert "truncated" not in artifacts.stderr


def test_drainer_size_counting_is_fast_on_long_suite_output():
    import time

    # Realistic chunk: ~80 chars of pytest output, plain ASCII. We use a
    # mix of plain ASCII (the common pytest case) and a small percentage
    # of non-ASCII (test names occasionally contain Cyrillic / emoji) so
    # the benchmark reflects a realistic mix, not a strawman.
    ascii_line = "tests/test_module_007.py::TestClass::test_method[param=42] PASSED\n"
    unicode_line = "tests/test_кириллица.py::test_тест_💥 PASSED\n"
    chunks = ([ascii_line] * 100 + [unicode_line]) * 100  # ~10_100 lines

    # Old path (what we replaced): allocate a bytes object per line and
    # take its length. We inline-reproduce it so the comparison is
    # against the actual previous implementation, not a memory of it.
    def old_count(chunks):
        total = 0
        for c in chunks:
            total += len(c.encode("utf-8", errors="replace"))
        return total

    def new_count(chunks):
        total = 0
        for c in chunks:
            total += len(c)
        return total

    # Warm-up
    old_count(chunks)
    new_count(chunks)

    iters = 50

    t0 = time.perf_counter()
    for _ in range(iters):
        old_total_bytes = old_count(chunks)
    old_elapsed = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(iters):
        new_total_chars = new_count(chunks)
    new_elapsed = time.perf_counter() - t0

    speedup = old_elapsed / max(new_elapsed, 1e-9)

    under_ratio = old_total_bytes / max(new_total_chars, 1)

    print(
        f"[drainer_perf] chunks={len(chunks)} iters={iters} "
        f"old_encode={old_elapsed * 1000:.1f}ms "
        f"new_len={new_elapsed * 1000:.1f}ms "
        f"speedup={speedup:.1f}x "
        f"under_count_ratio={under_ratio:.3f}x"
    )

    assert new_elapsed < old_elapsed, (
        f"len(chunk) ({new_elapsed * 1000:.1f}ms) is not faster than "
        f"chunk.encode().len() ({old_elapsed * 1000:.1f}ms) -- something "
        "regressed the optimisation."
    )

    assert 1.0 <= under_ratio < 1.10, (
        f"under_count_ratio={under_ratio:.3f} -- the realistic mix should "
        "stay well under 1.10 for ASCII-dominant pytest output."
    )
