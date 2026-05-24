# airflow-pytest-operator

Run a `pytest` suite as an Airflow task. The operator executes your tests in a child process, parses the JUnit report into a structured result, pushes a summary to XCom, and fails the task when tests fail (configurable).

Works on **Airflow 2.x and 3.x** — all version-specific imports are isolated in a single compatibility module, so one wheel supports both.

[![PyPI version](https://img.shields.io/pypi/v/airflow-pytest-operator.svg)](https://pypi.org/project/airflow-pytest-operator/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-pytest-operator.svg)](https://pypi.org/project/airflow-pytest-operator/)
[![CI](https://github.com/IKrysanov/airflow-pytest-operator/actions/workflows/ci.yml/badge.svg)](https://github.com/IKrysanov/airflow-pytest-operator/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Why a child process

Tests run via `{sys.executable} -m pytest`, i.e. in the **same virtualenv / interpreter as the Airflow worker** (same dependencies), but in a **child process**. This keeps pytest's global-state mutations (`sys.modules`, plugins, cwd, logging) out of the long-lived worker while still satisfying "same environment" semantics. A crashing or segfaulting test can't take the worker down, and the child can be killed cleanly on timeout or task termination.

## Install

```bash
pip install airflow-pytest-operator
# recommended: hardened XML parsing for untrusted reports
pip install "airflow-pytest-operator[secure-xml]"
```

Airflow itself is **not** a hard dependency — the package installs into your existing Airflow environment. Pin a compatible Airflow via an extra if you want resolution help: `airflow-pytest-operator[airflow2]` or `[airflow3]`.

### Installing in Docker / constrained environments

In an Airflow Docker image, install the package **with Airflow's official constraint file** so dependency resolution matches your Airflow version exactly. Make sure the build args are actually set — an empty `AIRFLOW_VERSION`/`PYTHON_VERSION` produces an invalid constraint URL and the build fails:

```dockerfile
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir "airflow-pytest-operator" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

The package itself pins nothing (`dependencies = []`), so any resolution conflict comes from your wider environment; the constraint file is the standard way to keep it reproducible.

## Usage

```python
import pendulum
from airflow import DAG
from airflow_pytest_operator import PytestOperator

with DAG(
    dag_id="run_tests",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,
) as dag:
    smoke = PytestOperator(
        task_id="smoke_tests",
        test_path="/opt/airflow/tests",      # next to your dags/ folder
        pytest_args=["-k", "smoke", "-x"],   # any pytest CLI args
        env={"ENV": "staging"},              # extra env for the run
        fail_on_test_failure=True,           # task fails if any test fails
    )
```

The summary pushed to XCom (standard `return_value` key) looks like:

```python
{
    "total": 12, "passed": 11, "failed": 1, "skipped": 0, "errors": 0,
    "duration": 3.4, "exit_code": 1, "success": False,
    "failed_node_ids": ["tests/test_api.py::test_timeout"],
}
```

## Constructor options

| Option | Default | Description |
|---|---|---|
| `test_path` | — | File or directory passed to pytest. Templated. |
| `pytest_args` | `[]` | Extra pytest CLI args, e.g. `["-k", "smoke", "-x"]`. Templated. |
| `env` | `{}` | Extra environment variables for the run. Templated. |
| `fail_on_test_failure` | `True` | Fail the task on any test failure/error. If `False`, the task always succeeds and the outcome is only reflected in XCom. |
| `do_xcom_push` | `True` | Airflow's standard flag. When on, the summary dict is pushed to XCom under the `return_value` key. Set `False` to disable all XCom output. Read it downstream with `xcom_pull(task_ids="<task>")`. |
| `runner` | `SubprocessPytestRunner()` | Injectable execution strategy (see *Extending*). |
| `parser` | `JUnitResultParser()` | Injectable report parser (see *Extending*). |

The default `SubprocessPytestRunner` additionally accepts `python_executable`, `timeout`, `report_dir`, `cwd`, `grace_period`, and `cleanup` — see below.

## pytest config, plugins, and Allure

The operator runs real `python -m pytest`, so pytest discovers its own configuration (`pytest.ini`, `pyproject.toml`, `tox.ini`, `setup.cfg`) and `rootdir` exactly as on the command line. **Plugins and their options are picked up from your test folder's config automatically** — Allure, `pytest-xdist`, `pytest-cov`, markers, `addopts`, and so on. The operator only adds `--junitxml` (for its own parsing); everything else is yours.

To make relative paths in `addopts` (e.g. `--alluredir=allure-results`) resolve next to your tests rather than the worker's working directory, the runner sets its working directory to the test folder by default: a directory `test_path` becomes the cwd, a file's parent becomes the cwd. Pass an explicit `cwd=` to override. The JUnit report path stays absolute, so this never misplaces it.

```python
# pytest.ini next to your tests, with allure-pytest installed on the worker:
#   [pytest]
#   addopts = --alluredir=allure-results
# -> results land in <tests>/allure-results, as expected.
```

On distributed executors, make sure the plugins you reference (e.g. `allure-pytest`) are installed in the worker/pod environment, and write Allure output to persistent storage (volume/S3) rather than an ephemeral pod filesystem.

## Report cleanup

When `report_dir` is not given, the runner creates a temporary directory per run for the JUnit report. It is cleaned up according to the `cleanup` policy on `SubprocessPytestRunner`:

| `cleanup` | Behaviour |
|---|---|
| `"always"` (default) | Remove the temp dir after every run, including on test failure and on task kill/timeout. |
| `"on_success"` | Keep the temp dir when the run failed (for post-mortem); remove it on success. |
| `"never"` | Never remove it (e.g. you upload it as a CI artifact). |

A **user-supplied** `report_dir` is never removed — it's your data. Cleanup also runs from `on_kill`, so killed tasks don't leak temp directories.

## Cancellation and timeouts

When Airflow kills the task (execution timeout, manual clear/mark-failed, worker shutdown), the operator's `on_kill` delegates to the runner, which terminates the **entire pytest process tree** — not just the direct child. This matters because pytest spawns its own children (e.g. `xdist` workers). Termination is graceful by default: `SIGTERM`, wait `grace_period` seconds (default 10), then `SIGKILL`. Set `timeout=` on the runner to bound the run itself.

> **Platform note:** process-group termination is fully supported on **Linux and macOS**. On Windows the package runs and cancels the direct process, but reliable whole-tree termination is not guaranteed; Airflow workers are Linux in virtually all deployments.

## Where do the tests live?

The operator runs whatever path exists **on the worker** at execute time, so it works with any executor (Local, Celery, Kubernetes, custom) — the runner spawns pytest wherever the task already runs. The practical constraint is *availability*: with `LocalExecutor` the tests sit next to `dags/`; with Celery/Kubernetes, make sure the test folder is synced to workers the same way DAGs are (git-sync, baked image, shared volume), or point `test_path` at wherever they land. If the path is missing, the task fails with a clear `TestExecutionError`.

## Extending it

The operator depends on two narrow abstractions and accepts them via constructor injection — no operator subclassing required. Provide your own to change *how* tests run or *how* results are parsed.

### Custom runner

```python
from airflow_pytest_operator import PytestOperator, PytestRunner, RunArtifacts

class DockerPytestRunner(PytestRunner):
    def run(self, test_path, *, pytest_args=None, env=None) -> RunArtifacts:
        # run pytest inside a container, write a JUnit xml, then:
        return RunArtifacts(exit_code=..., junit_xml_path="/path/junit.xml")

    # optional: override cancel() / cleanup() if you own resources
    # (the base class provides safe no-op defaults)

PytestOperator(task_id="t", test_path="tests/", runner=DockerPytestRunner())
```

### Custom parser

```python
from airflow_pytest_operator import PytestOperator, ResultParser, TestRunResult

class JSONResultParser(ResultParser):
    def parse(self, report_path, *, exit_code=0) -> TestRunResult:
        ...  # read pytest-json-report output, return a TestRunResult

PytestOperator(task_id="t", test_path="tests/", parser=JSONResultParser())
```

## Architecture

| Concern | Type | Responsibility |
|---|---|---|
| `PytestOperator` | operator | orchestrate runner→parser, Airflow integration, fail/cleanup policy |
| `PytestRunner` / `SubprocessPytestRunner` | runner | execute pytest, produce `RunArtifacts`, own cancel/cleanup |
| `ResultParser` / `JUnitResultParser` | parser | turn a report file into `TestRunResult` |
| `compat.airflow` | shim | the only place that imports Airflow |
| `models` | domain | framework-free dataclasses |

## Development

The library's own tests run **without Airflow** by stubbing `BaseOperator` — itself a demonstration of the dependency-inversion design.

```bash
pip install -e ".[dev]"
ruff check src tests
mypy
pytest
```

## License

Apache-2.0. See [LICENSE](LICENSE).
