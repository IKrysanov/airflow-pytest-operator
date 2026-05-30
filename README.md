# airflow-pytest-operator

Run a `pytest` suite as an Airflow task. The operator executes your tests in a child process, parses the JUnit report into a structured result, pushes a summary to XCom, and fails the task when tests fail (configurable).

Works on **Airflow 2.x and 3.x** — all version-specific imports are isolated in a single compatibility module, so one wheel supports both.

[![PyPI version](https://img.shields.io/pypi/v/airflow-pytest-operator.svg)](https://pypi.org/project/airflow-pytest-operator/)
[![Airflow](https://img.shields.io/badge/Airflow-2.10%20%7C%203.0%20%7C%203.2-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-pytest-operator.svg)](https://pypi.org/project/airflow-pytest-operator/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

<details open>
<summary>Quality &amp; build status</summary>

[![CI](https://github.com/IKrysanov/airflow-pytest-operator/actions/workflows/ci.yml/badge.svg)](https://github.com/IKrysanov/airflow-pytest-operator/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/IKrysanov/airflow-pytest-operator/branch/main/graph/badge.svg)](https://codecov.io/gh/IKrysanov/airflow-pytest-operator)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
</details>

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

## Verifying the release
 
Each PyPI release is published from GitHub Actions via PyPI's
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) and ships
with a [PEP 740](https://peps.python.org/pep-0740/) **Sigstore attestation**
that cryptographically binds every wheel and sdist to a specific commit and
workflow in this repository. PyPI verifies the attestation at upload time
and shows the source repository in the release's *Verified details*. You can
also verify it yourself before installing, which protects against tampering
between PyPI and your machine.
 
PyPI verifies the attestation at upload time and surfaces the link back to
this repository in the release's *Verified details*, so the common case
(`pip install airflow-pytest-operator`) already gives you that assurance
through PyPI. For deeper verification before installing — for example in a
security-sensitive or air-gapped environment — there are two independent
paths, both rooted in the same Sigstore public-good instance.
 
### Path 1 — verify the PyPI artifact (PEP 740)
 
Each PyPI release carries a PEP 740 attestation that ties the wheel and
sdist to the exact `release.yml` run that produced them. The
[`pypi-attestations`](https://pypi.org/project/pypi-attestations/) CLI
fetches the artifact and its provenance directly from PyPI:
 
```bash
pip install pypi-attestations
 
pypi-attestations verify pypi \
    --repository https://github.com/IKrysanov/airflow-pytest-operator \
    pypi:airflow_pytest_operator-X.Y.Z-py3-none-any.whl
 
pypi-attestations verify pypi \
    --repository https://github.com/IKrysanov/airflow-pytest-operator \
    pypi:airflow_pytest_operator-X.Y.Z.tar.gz
```
 
Replace `X.Y.Z` with the version you are installing.
 
> `pypi-attestations` is an experimentation-grade CLI per its own
> documentation; PyPI's upload-time check is the primary trust path.
> Future `pip` releases are expected to expose attestation verification
> natively.
 
### Path 2 — verify the GitHub Release artifact (Sigstore bundle)
 
Starting with version 0.3.1, each GitHub Release also ships the built
distributions plus an `.intoto.jsonl` Sigstore bundle that covers the same
bytes published to PyPI (both are produced from a single `build` job —
there is no parallel rebuild). This enables offline verification using the
[GitHub CLI](https://cli.github.com/) (`gh` 2.49+):
 
```bash
# Download the release assets (wheel, sdist, and the bundle):
gh release download vX.Y.Z \
    --repo IKrysanov/airflow-pytest-operator \
    --pattern "*.whl" --pattern "*.tar.gz" --pattern "*.intoto.jsonl"
 
# Online: verify against the GitHub attestations API
# (simplest; requires network access to api.github.com):
gh attestation verify airflow_pytest_operator-X.Y.Z-py3-none-any.whl \
    --repo IKrysanov/airflow-pytest-operator
 
# Offline: verify against the downloaded bundle
# (no API access required; the bundle is self-contained):
gh attestation verify airflow_pytest_operator-X.Y.Z-py3-none-any.whl \
    --repo IKrysanov/airflow-pytest-operator \
    --bundle airflow-pytest-operator-X.Y.Z.intoto.jsonl
```
 
A successful verification prints the workflow and run that produced the
artifact:
 
```
✓ Verification succeeded!
  Workflow: …/release.yml@refs/tags/vX.Y.Z
```
 
A failure means the file did not come from this workflow — **do not
install**.
 
Both paths confirm the same guarantee: the artifact came from this
GitHub repository, was produced by `release.yml` (the only configured
Trusted Publisher), and has not been modified since publication.

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

## Passing values from upstream tasks into your tests

A common pattern: an upstream task (say a `DataIngester`) creates rows in a
table and you want the test run to adapt — parametrise over the freshly
created IDs, target a specific table, etc. — **without editing the test
file** each time. The channel for this is the templated `env` field.

The important rule is **where** the Jinja template goes. Template the
**values inside the dict**, not the whole `env` as one string:

```python
# WRONG — templates the whole env as a single string. Jinja renders a dict
# to its *string repr* ("{'A': 'B'}"), so your test gets garbage.
env="{{ ti.xcom_pull(task_ids='ingest', key='cfg') }}"

# RIGHT — each value is templated independently and stays a clean string,
# which is exactly what an environment variable must be.
env={
    "TEST_IDS": "{{ ti.xcom_pull(task_ids='ingest', key='entity_ids') }}",
    "TARGET_TABLE": "{{ ti.xcom_pull(task_ids='ingest', key='target_table') }}",
}
```

Because environment variables are always strings, the upstream task should
push **already-serialised strings** to XCom (a CSV like `"101,102,103"` or a
JSON string), and the test parses them back. Full flow:

```python
# 1. Upstream task serialises what it produced into XCom as strings.
def ingest(**context):
    created_ids = [101, 102, 103]            # whatever the ingester made
    ti = context["ti"]
    ti.xcom_push(key="entity_ids", value=",".join(map(str, created_ids)))
    ti.xcom_push(key="target_table", value="fact_orders")

ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)

# 2. PytestOperator forwards those values via per-value templated env.
run_tests = PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    env={
        "TEST_IDS": "{{ ti.xcom_pull(task_ids='ingest', key='entity_ids') }}",
        "TARGET_TABLE": "{{ ti.xcom_pull(task_ids='ingest', key='target_table') }}",
    },
)

ingest_task >> run_tests
```

```python
# 3. The test reads the env var and parametrises over it. The function is
#    evaluated at collection time, by which point the operator has already
#    exported the variable, so the parametrisation sees the upstream values.
import os
import pytest

def _entity_ids():
    return [int(x) for x in os.environ.get("TEST_IDS", "").split(",") if x]

@pytest.mark.parametrize("entity_id", _entity_ids())
def test_entity_was_created(entity_id):
    table = os.environ["TARGET_TABLE"]
    # assert the row with entity_id exists in `table`
    ...
```

If you genuinely need a structured (non-string) object to survive
templating — e.g. you want `env` itself to come out as a dict from a single
XCom value — set `render_template_as_native_obj=True` on the DAG. Note that
this switches Jinja to native rendering for **every** templated field of
**every** task in that DAG, which can surprise other operators, so prefer
the per-value string approach above unless you specifically need native
objects.

## Constructor options

`PytestOperator` accepts the parameters below **plus every parameter that
[`BaseOperator`](https://airflow.apache.org/docs/apache-airflow/2.3.4/_api/airflow/models/baseoperator/index.html)
accepts** — `task_id`, `retries`, `execution_timeout`, `on_failure_callback`,
`trigger_rule`, `pool`, and so on. Airflow 3 users: `BaseOperator` moved to
`airflow.sdk`; the canonical reference is the
[Task SDK API docs](https://airflow.apache.org/docs/task-sdk/stable/api.html).

The parameters specific to `PytestOperator` are:

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
pytest --cov
```

## License

Apache-2.0. See [LICENSE](LICENSE).
