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


"""Shared fakes and helpers for the split test modules."""

from __future__ import annotations

from airflow_pytest_operator.models import RunArtifacts, TestRunResult


class FakeRunner:
    """Records how it was called and returns canned artifacts."""

    def __init__(self, artifacts: RunArtifacts):
        self._artifacts = artifacts
        self.calls = []
        self.cancelled = 0
        self.cleanup_calls = []

    def run(
        self,
        test_path,
        *,
        pytest_args=None,
        env=None,
        env_file=None,
        env_file_overrides=False,
        report_request,
    ):
        spec = report_request("/fake/report/dir")
        self.calls.append(
            {
                "test_path": test_path,
                "pytest_args": pytest_args,
                "env": env,
                "env_file": env_file,
                "env_file_overrides": env_file_overrides,
                "report_request": report_request,
                "spec": spec,
            }
        )
        return self._artifacts

    def cancel(self):
        self.cancelled += 1

    def cleanup(self, *, success=True):
        self.cleanup_calls.append(success)


class FakeParser:
    """Returns a canned result regardless of input."""

    def __init__(self, result: TestRunResult):
        self._result = result
        self.parsed_paths = []
        self.report_request_calls = []

    def report_request(self, report_dir):
        from airflow_pytest_operator.models import ReportRequest

        self.report_request_calls.append(report_dir)
        return ReportRequest(
            pytest_args=("--fake-report",),
            report_path=f"{report_dir}/fake.report",
        )

    def parse(self, report_path, *, exit_code=0):
        self.parsed_paths.append((report_path, exit_code))
        return self._result


class FakeTI:
    def __init__(
        self, try_number=1, dag_id=None, task_id=None, run_id=None, max_tries=None
    ):
        self.pushed = {}
        # Airflow exposes the attempt number here: 1 on the first run, 2+ on
        # retries. With max_tries it tells the operator whether this is the
        # final attempt (try_number > max_tries) -- which gates whether the
        # failed_only Variable is written forward for a next retry.
        self.try_number = try_number
        self.max_tries = max_tries
        # (dag_id, task_id, run_id) derive the failed_only Variable key. Default
        # None -> no derivable key, so tests that don't care are unaffected.
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _result(*, failed=0, errors=0, passed=1):
    total = passed + failed + errors
    return TestRunResult(
        total=total,
        passed=passed,
        failed=failed,
        skipped=0,
        errors=errors,
        duration=0.1,
        exit_code=0 if not (failed or errors) else 1,
    )


def _ctx(try_number=1, *, dag_id=None, task_id=None, run_id=None, max_tries=None):
    return {
        "ti": FakeTI(
            try_number=try_number,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            max_tries=max_tries,
        )
    }


class FakeStore:
    """In-memory stand-in for VariableLastFailedStore.

    Records every read/write/delete so tests can assert the cross-retry
    bookkeeping without touching a real Airflow Variable.
    """

    def __init__(self, initial=None):
        self.data = dict(initial or {})
        self.reads = []
        self.writes = []
        self.deletes = []

    def read(self, key):
        self.reads.append(key)
        return list(self.data.get(key, []))

    def write(self, key, node_ids):
        self.writes.append((key, list(node_ids)))
        self.data[key] = list(node_ids)

    def delete(self, key):
        self.deletes.append(key)
        self.data.pop(key, None)


def _key(dag_id="d", task_id="t", run_id="r"):
    """The Variable key the operator derives for these ids (real derivation)."""
    from airflow_pytest_operator.stores import last_failed_var_key

    return last_failed_var_key(_ctx(dag_id=dag_id, task_id=task_id, run_id=run_id))


class SequenceParser:
    """Returns canned results in sequence -- one per parse() call.

    Lets a test script several pytest rounds (first full run, then reruns):
    parse() returns results[0], results[1], ... and clamps to the last one.
    """

    def __init__(self, results):
        self._results = list(results)
        self._i = 0
        self.parsed_paths = []
        self.report_request_calls = []

    def report_request(self, report_dir):
        from airflow_pytest_operator.models import ReportRequest

        self.report_request_calls.append(report_dir)
        return ReportRequest(
            pytest_args=("--fake-report",),
            report_path=f"{report_dir}/fake.report",
        )

    def parse(self, report_path, *, exit_code=0):
        self.parsed_paths.append((report_path, exit_code))
        result = self._results[min(self._i, len(self._results) - 1)]
        self._i += 1
        return result


def _res(failed_ids=(), *, passed=0):
    """Build a TestRunResult whose failed_node_ids == list(failed_ids)."""
    from airflow_pytest_operator.models import CaseResult

    cases = [
        CaseResult(
            name=fid.partition("::")[2],
            classname=fid.partition("::")[0],
            time=0.0,
            outcome="failed",
        )
        for fid in failed_ids
    ]
    failed = len(cases)
    return TestRunResult(
        total=passed + failed,
        passed=passed,
        failed=failed,
        skipped=0,
        errors=0,
        duration=0.1,
        exit_code=0 if failed == 0 else 1,
        cases=tuple(cases),
    )


class _ExplodingCleanupRunner(FakeRunner):
    """A runner whose cleanup() violates the best-effort contract."""

    def cleanup(self, *, success=True):
        self.cleanup_calls.append(success)
        raise RuntimeError("cleanup boom")


class _ExplodingStore:
    """A store that raises on every method (satisfies the protocol shape)."""

    def read(self, key):
        raise RuntimeError("read boom")

    def write(self, key, node_ids):
        raise RuntimeError("write boom")

    def delete(self, key):
        raise RuntimeError("delete boom")


class _DeleteExplodingStore(FakeStore):
    """Reads/writes normally but raises on delete (consume-on-read path)."""

    def delete(self, key):
        raise RuntimeError("delete boom")


class _RecordingCustomRunner:
    """A minimal *custom* runner accepting the new run() kwargs (interface check).

    Duck-typed (no PytestRunner base) to prove the operator only relies on the
    structural contract, and that env_file/env_file_overrides reach a custom
    runner unchanged.
    """

    def __init__(self, artifacts):
        self._artifacts = artifacts
        self.calls = []

    def run(
        self,
        test_path,
        *,
        pytest_args=None,
        env=None,
        env_file=None,
        env_file_overrides=False,
        report_request,
    ):
        report_request("/fake/dir")
        self.calls.append(
            {"env_file": env_file, "env_file_overrides": env_file_overrides}
        )
        return self._artifacts

    def cleanup(self, *, success=True):
        pass
