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
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/IKrysanov/airflow-pytest-operator/badge)](https://scorecard.dev/viewer/?uri=github.com/IKrysanov/airflow-pytest-operator)
</details>

## Table of Contents

- [Why a child process](#why-a-child-process)
- [Install](#install)
  - [Quick install](#quick-install)
  - [Installing in Docker / constrained environments](#installing-in-docker--constrained-environments)
- [Verifying the release](#verifying-the-release)
  - [Path 1 — verify the PyPI artifact (PEP 740)](#path-1--verify-the-pypi-artifact-pep-740)
  - [Path 2 — verify the GitHub Release artifact (Sigstore bundle)](#path-2--verify-the-github-release-artifact-sigstore-bundle)
- [Usage](#usage)
- [Passing values from upstream tasks into your tests](#passing-values-from-upstream-tasks-into-your-tests)
- [Constructor options](#constructor-options)
- [pytest config, plugins, and Allure](#pytest-config-plugins-and-allure)
- [Report location & cleanup](#report-location--cleanup)
- [Cancellation and timeouts](#cancellation-and-timeouts)
- [Dry-run mode](#dry-run-mode)
- [Retry strategy (failed-only reruns)](#retry-strategy-failed-only-reruns)
- [Where do the tests live?](#where-do-the-tests-live)
- [Built-in parsers](#built-in-parsers)
- [Extending it](#extending-it)
  - [Custom parser](#custom-parser)
  - [Custom runner](#custom-runner)
- [Architecture](#architecture)
- [Development](#development)
- [Changelog](#changelog)
- [License](#license)

## Why a child process

Tests run via `{sys.executable} -m pytest`, i.e. in the **same virtualenv / interpreter as the Airflow worker** (same dependencies), but in a **child process**. This keeps pytest's global-state mutations (`sys.modules`, plugins, cwd, logging) out of the long-lived worker while still satisfying "same environment" semantics. A crashing or segfaulting test can't take the worker down, and the child can be killed cleanly on timeout or task termination.

## Install

### Quick install

```bash
pip install airflow-pytest-operator
```
 
**Worker extras** — install alongside the operator on every Airflow worker
that will run test tasks:
 
```bash
# pytest only (operator requires pytest on the worker)
pip install "airflow-pytest-operator[pytest]"
 
# pytest + allure-pytest (for --alluredir report generation)
# Note: to *view* Allure reports you also need the Allure CLI (Java); see README below.
pip install "airflow-pytest-operator[pytest-allure]"
 
# pytest + pytest-json-report (for the built-in JSONResultParser)
pip install "airflow-pytest-operator[json-report]"
 
# hardened XML parsing for untrusted JUnit reports (recommended for production)
pip install "airflow-pytest-operator[secure-xml]"
 
# combine extras as needed
pip install "airflow-pytest-operator[pytest,secure-xml,json-report]"
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
    "failed_node_ids": ["tests.test_api::test_timeout"],
}
```

`failed_node_ids` uses a dotted, parser-independent form. To feed them back
into a pytest run (a "retry only failed" task), convert them to CLI selectors
with `node_id_to_pytest_args`:

```python
from airflow_pytest_operator import node_id_to_pytest_args

def build_retry_args(**ctx):
    prev = ctx["ti"].xcom_pull(task_ids="run_tests") or {}
    # -> ["tests/test_api.py::test_timeout"]
    return node_id_to_pytest_args(prev.get("failed_node_ids") or [])
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
| `test_path` | — | Target(s) passed to pytest: a file, directory, or node-id selector — or a sequence of them. Templated. |
| `pytest_args` | `[]` | Extra pytest CLI args, e.g. `["-k", "smoke", "-x"]`. Templated. |
| `env` | `{}` | Extra environment variables for the run. Templated. |
| `fail_on_test_failure` | `True` | Fail the task on any test failure/error. If `False`, the task always succeeds and the outcome is only reflected in XCom. |
| `dry_run` | `False` | Run pytest in `--collect-only` mode: import the test modules and walk the collection tree, but **do not execute test bodies**. Useful as a pre-flight task in a DAG; see [Dry-run mode](#dry-run-mode) below. |
| `test_retry_strategy` | `"all"` | How Airflow task **retries** re-run the suite. `"all"` re-runs everything; `"failed_only"` carries the previous attempt's failed node-ids in an Airflow Variable and re-runs **only those** on the next retry (deleted when no further retry will read it). See [Retry strategy](#retry-strategy-failed-only-reruns) below. |
| `store` | `VariableLastFailedStore()` | Backing store for the `failed_only` cross-retry set. Inject a fake (tests) or another backend with `read`/`write`/`delete`. Unused unless `test_retry_strategy="failed_only"`. |
| `rerun_failed` | `0` | **In-process** re-runs of only the failed tests, within one task. `N>0` runs the full suite then re-runs the still-failing tests up to `N` more times — no cache, no Airflow retry, robust on any executor. See [Retry strategy](#retry-strategy-failed-only-reruns). |
| `do_xcom_push` | `True` | Airflow's standard flag. When on, the summary dict is pushed to XCom under the `return_value` key. Set `False` to disable all XCom output. Read it downstream with `xcom_pull(task_ids="<task>")`. |
| `runner` | `SubprocessPytestRunner()` | Injectable execution strategy (see *Extending*). |
| `parser` | `JUnitResultParser()` | Injectable report parser (see *Extending*). |

The default `SubprocessPytestRunner` additionally accepts `python_executable`, `timeout`, `cwd`, `grace_period`, and `cleanup` — see below. The **report location is set on the parser** (`JUnitResultParser(report_dir=...)`), not the runner — see [Report location & cleanup](#report-location--cleanup).

## pytest config, plugins, and Allure

The operator runs real `python -m pytest`, so pytest discovers its own configuration (`pytest.ini`, `pyproject.toml`, `tox.ini`, `setup.cfg`) and `rootdir` exactly as on the command line. **Plugins and their options are picked up from your test folder's config automatically** — Allure, `pytest-xdist`, `pytest-cov`, markers, `addopts`, and so on. The operator only adds `--junitxml` (for its own parsing); everything else is yours.

To make relative paths in `addopts` (e.g. `--alluredir=allure-results`) resolve next to your tests rather than the worker's working directory, the runner sets its working directory to the test folder by default: a directory target becomes the cwd, a file's parent becomes the cwd, and with multiple targets the closest shared parent is used. Node-id selectors (`path::test`) disable this (the inherited cwd is kept). Pass an explicit `cwd=` to override. Report paths stay absolute, so this never misplaces them.

```python
# pytest.ini next to your tests, with allure-pytest installed on the worker:
#   [pytest]
#   addopts = --alluredir=allure-results
# -> results land in <tests>/allure-results, as expected.
```

On distributed executors, make sure the plugins you reference (e.g. `allure-pytest`) are installed in the worker/pod environment, and write Allure output to persistent storage (volume/S3) rather than an ephemeral pod filesystem.

## Report location & cleanup

The **parser** owns where the report lands — set `report_dir` on it:

```python
from airflow_pytest_operator import JUnitResultParser, PytestOperator

PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    parser=JUnitResultParser(report_dir="/opt/airflow/artifacts"),  # your folder, kept
)
```

When `report_dir` is **not** set, the runner writes the report to a temporary
directory per run and cleans it up according to the `cleanup` policy on
`SubprocessPytestRunner`:

| `cleanup` | Behaviour |
|---|---|
| `"always"` (default) | Remove the temp dir after every run, including on test failure and on task kill/timeout. |
| `"on_success"` | Keep the temp dir when the run failed (for post-mortem); remove it on success. |
| `"never"` | Never remove it (e.g. you upload it as a CI artifact). |

A **parser-supplied** `report_dir` is your data and is never removed, regardless of policy. Cleanup also runs from `on_kill`, so killed tasks don't leak temp directories.

## Cancellation and timeouts

When Airflow kills the task (execution timeout, manual clear/mark-failed, worker shutdown), the operator's `on_kill` delegates to the runner, which terminates the **entire pytest process tree** — not just the direct child. This matters because pytest spawns its own children (e.g. `xdist` workers). Termination is graceful by default: `SIGTERM`, wait `grace_period` seconds (default 10), then `SIGKILL`. Set `timeout=` on the runner to bound the run itself. When that limit trips, the `TestExecutionError` carries the captured `stdout` / `stderr` as attributes, so you can inspect what the run printed before it hung.

> **Platform note:** process-group termination is fully supported on **Linux and macOS**. On Windows the package runs and cancels the direct process, but reliable whole-tree termination is not guaranteed; Airflow workers are Linux in virtually all deployments.

## Dry-run mode

`dry_run=True` runs pytest with `--collect-only`. Test bodies are not executed, but pytest still imports the test modules, walks the collection tree, and runs `conftest.py`. This is exactly what you want for a **pre-flight validation** task at the start of a DAG: it catches stale paths, missing deps on the worker, broken imports, `SyntaxError`s, and renamed fixtures in seconds rather than minutes.

```python
from airflow_pytest_operator import PytestOperator

# Pre-flight: validate that tests collect cleanly on this worker.
validate = PytestOperator(
    task_id="validate_tests",
    test_path="tests/",
    dry_run=True,
)

# Real run, gated on the pre-flight succeeding.
run = PytestOperator(
    task_id="run_tests",
    test_path="tests/",
)

validate >> run
```

A collection error (broken import, `SyntaxError`, missing fixture used by a parametrize) fails the dry-run task just like a test failure would, so downstream tasks are skipped and the real run never starts.

**What dry-run does and doesn't do:**

| Step | Real run | `dry_run=True` |
|---|---|---|
| Import test modules | yes | **yes** |
| Run module-level code | yes | **yes** |
| Run `conftest.py` | yes | **yes** |
| Collect tests (walk the tree) | yes | yes |
| Run collection-time fixtures | yes | yes |
| **Run test bodies** | yes | **no** |
| Run session/module/function teardown | yes | only for collection-time setup, if any |

So `dry_run` is not a no-op — module-level side effects happen. It's "collect only", which is meaningfully cheaper than a real run but still imports your code.

**Result interpretation:**

- With `parser=JSONResultParser()`, `TestRunResult.total` reports the number of collected tests (parsed from `summary.collected`).
- With the default `JUnitResultParser`, the XML pytest emits in `--collect-only` mode contains no `<testcase>` entries (`<testsuite tests="0">`), so `total` is `0`. The task still passes/fails correctly based on exit code; only the collected-count is unavailable. Use the JSON parser if you need the count for branching downstream.

**Interaction with user-supplied flags:** if you already passed `--collect-only` (or its aliases `--collectonly`, `--co`) in `pytest_args`, the operator won't add another one. The dedup is targeted only at the collect-only family — other repeated args (`-v -v`, multiple `-o KEY=VAL`, multiple `--ignore=...`) are preserved as-is.

## Retry strategy (failed-only reruns)

By default the **entire** pytest suite runs again whenever it is re-run. For a large suite where only a couple of tests failed, that wastes time. There are three ways to re-run only the failures — start with the first.

### Recommended: `rerun_failed` (built-in, any executor)

Set `rerun_failed=N` to make the operator **re-run only the failed tests, in-process, up to N times**, before reporting the final outcome — all within a single task:

```python
from airflow_pytest_operator import PytestOperator

run = PytestOperator(
    task_id="run_tests",
    test_path="tests/",
    rerun_failed=2,   # run all once, then re-run only the failures up to 2 more times
)
```

How it works:

- The operator runs the full suite once, then re-runs **only** the failed tests (converted via `node_id_to_pytest_args`), repeating up to `rerun_failed` times and stopping early as soon as none fail.
- It is **robust on any executor**: the set of failures is carried in memory across rounds within one `execute()`, so it needs **no metadata store, no Airflow Variable, no XCom between attempts, and no `try_number`** — none of the moving parts of the retry-driven approaches below.
- The task fails only if tests **still** fail after all rounds. The XCom summary keeps the first full run's counts and adds `rerun_rounds`, `recovered_node_ids`, and `still_failing_node_ids`. Ignored in `dry_run` mode.

This is the recommended way to absorb flaky failures and save compute. Use it alone, or combine it with Airflow's `retries=` — the in-process reruns happen first; if the task still fails, Airflow re-runs it from scratch on a fresh worker.

### Single-operator: `test_retry_strategy="failed_only"` (Airflow retries)

Set `test_retry_strategy="failed_only"` to make Airflow retries re-run **only the tests that failed on the previous attempt** — in a single task, driven purely by Airflow's own `retries=`:

```python
from airflow_pytest_operator import PytestOperator

run = PytestOperator(
    task_id="run_tests",
    test_path="tests/",
    retries=2,                          # Airflow's standard retry count
    test_retry_strategy="failed_only",  # retries re-run only what failed
)
```

How it works:

- On the **first attempt of a fresh run** the store is empty, so the full suite runs; if it fails (and a retry remains) the failing node-ids are saved. Narrowing is driven purely by what's stored, **not** by `try_number` — so a *reused* `run_id` (a cleared/restarted run after a partial crash) may carry a prior set and narrow earlier.
- **Each retry** (`try_number > 1`) reads that saved set, converts it back to pytest selectors (via `node_id_to_pytest_args`), and passes them as the targets in place of `test_path` — so pytest collects and runs **only** the previously-failed tests. Your `pytest_args` are never mutated.

**Why an Airflow Variable, not XCom.** The failed set is carried between attempts in an **Airflow Variable** keyed by `(dag_id, task_id, run_id)`. A task **cannot** read its own XCom from a previous attempt — Airflow clears a task instance's XCom at the start of every retry — and writing a *different* task's XCom from inside a task is not portable to Airflow 3 (workers have no direct metadata-DB access). A Variable, by contrast, is readable **and** writable from within a task on both Airflow 2.x and 3.x, survives the task's own retries, and can be deleted when done. This unifies the old worker-local `--lf` cache trick and the two-task `run-all → run-failed` XCom pattern into one operator.

**Crash-safe cleanup (no orphaned Variables).** The Variable is **consumed on read**: a retry deletes it the instant it has read the failures and built its targets — *before* running a single test — so a worker killed mid-run (OOM/SIGKILL/pod eviction) can't leave one behind. A fresh copy is written at the **end** of an attempt **only when a further retry will read it** (the attempt failed *and* it is not the final one). On success, and on the final attempt, nothing is written, so the terminal attempt can never orphan a Variable even if it dies right before finishing. There is deliberately **no teardown-time delete a crash could skip** — the Variable exists only in the narrow gap between a failed non-final attempt and the retry that consumes it.

**Safe by construction.** If the Variable backend is unavailable, or the Airflow ids can't be derived from the context, the retry simply runs the **full suite** rather than failing — you never silently skip tests. Ignored in `dry_run` mode (there is no "last failed" to narrow to). The default `test_retry_strategy="all"` keeps the original behaviour (full suite on every retry).

> **Tip.** Want a different backing store (e.g. a custom KV store, or to scope the key differently)? Inject one: `PytestOperator(..., store=MyStore())`. Any object implementing the `LastFailedStore` protocol — `read(key) -> list[str]`, `write(key, ids)`, `delete(key)` — works (structural typing, so it type-checks under mypy without subclassing); the default is `VariableLastFailedStore`.

For absorbing flaky failures **without** an Airflow retry at all, prefer `rerun_failed` above (in-process, no metadata writes). For splitting the work across two explicit tasks, see the pattern below.

### Robust: run-all → run-failed (any executor)

For a result that does **not** depend on the worker cache, split the work across two tasks and carry the failed test ids through **XCom** (Airflow's metadata DB). Because the second task reads a *different* task's XCom, Airflow never clears it (it only clears a task's own XCom on that task's own retry) — so this works on any executor and survives a worker/pod dying between tasks:

```python
from airflow.decorators import task
from airflow_pytest_operator import PytestOperator, node_id_to_pytest_args
from airflow_pytest_operator.runners import SubprocessPytestRunner


def _failed(summary: dict | None) -> list[str]:
    # Read failed_node_ids out of the XCom summary and convert them to pytest
    # selectors; yields [] when nothing failed (so the short-circuit stays clean).
    return node_id_to_pytest_args((summary or {}).get("failed_node_ids") or [])


with DAG(...) as dag:
    # 1) Run everything; don't fail the task, so the summary (with
    #    failed_node_ids) is pushed to XCom for the next task.
    run_all = PytestOperator(
        task_id="run_all",
        test_path="/opt/airflow/tests",
        fail_on_test_failure=False,
    )

    # 2) Skip the rerun when nothing failed.
    @task.short_circuit
    def has_failures(summary: dict) -> bool:
        return bool(_failed(summary))

    # 3) Turn the XCom summary into pytest selectors for the failed tests.
    @task
    def to_selectors(summary: dict) -> list[str]:
        return _failed(summary)

    summary = run_all.output
    selectors = to_selectors(summary)

    # 4) Re-run ONLY the failed tests. cwd is the pytest rootdir (where
    #    pytest.ini / pyproject lives) so the relative selectors resolve.
    #    Give this task its own retries for several rounds if you like — they
    #    stay reliable because the ids live in run_all's XCom, not a cache.
    run_failed = PytestOperator(
        task_id="run_failed",
        test_path=selectors,
        fail_on_test_failure=True,
        retries=2,
        runner=SubprocessPytestRunner(cwd="/opt/airflow"),
    )

    run_all >> has_failures(summary) >> selectors >> run_failed
```

The `_failed` helper just reads `failed_node_ids` out of the XCom summary and runs them through `node_id_to_pytest_args`, returning `[]` when nothing failed. The snippet above is the complete pattern.

## Where do the tests live?

The operator runs whatever path exists **on the worker** at execute time, so it works with any executor (Local, Celery, Kubernetes, custom) — the runner spawns pytest wherever the task already runs. The practical constraint is *availability*: with `LocalExecutor` the tests sit next to `dags/`; with Celery/Kubernetes, make sure the test folder is synced to workers the same way DAGs are (git-sync, baked image, shared volume), or point `test_path` at wherever they land. If the path is missing, the task fails with a clear `TestExecutionError`.

## Built-in parsers

| Parser | Report format | Install requirement on the worker |
|---|---|---|
| `JUnitResultParser` (default) | JUnit XML (`--junitxml`) | nothing extra; pytest ships with it |
| `JSONResultParser` | pytest-json-report JSON | `pip install airflow-pytest-operator[json-report]` |

Both implement the same `ResultParser` interface and produce the same `TestRunResult`, so callers downstream of XCom don't care which one ran. Swap them via the `parser=` argument:

```python
from airflow_pytest_operator import JSONResultParser, PytestOperator

PytestOperator(
    task_id="t",
    test_path="tests/",
    parser=JSONResultParser(),
)
```

## Extending it

The operator depends on two narrow abstractions and accepts them via constructor injection — no operator subclassing required. Provide your own to change *how* tests run or *how* results are parsed.

The runner is **format-agnostic**: it does not know whether the report is JUnit XML, JSON, or anything else. Parsers declare the pytest CLI flags they need (and the path the report will land at) via `report_request(report_dir)`; the runner offers a temp `report_dir` as a fallback, but the parser owns the location and may use its own. Adding a new format means writing a new parser, not editing the runner.

### Custom parser

A parser implements two methods: `report_request` declares what pytest must produce, `parse` interprets the resulting file. The base `ResultParser` accepts `report_dir` — honor it (falling back to the runner's), and return an **absolute** path so it survives the runner's cwd handling.

```python
import os
from airflow_pytest_operator import (
    PytestOperator, ReportRequest, ResultParser, TestRunResult,
)

class TAPResultParser(ResultParser):
    """Example: read TAP (Test Anything Protocol) output via pytest-tap."""

    REPORT_FILENAME = "results.tap"

    def report_request(self, report_dir: str) -> ReportRequest:
        out_dir = os.path.abspath(self.report_dir or report_dir)
        path = os.path.join(out_dir, self.REPORT_FILENAME)
        return ReportRequest(
            pytest_args=("--tap-files", f"--tap-outdir={out_dir}"),
            report_path=path,
        )

    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        ...  # read TAP, return a TestRunResult

# location set on the parser; runner uses a temp dir when omitted
PytestOperator(task_id="t", test_path="tests/", parser=TAPResultParser(report_dir="/artifacts"))
```

### Custom runner

```python
from airflow_pytest_operator import PytestOperator, PytestRunner, RunArtifacts

class DockerPytestRunner(PytestRunner):
    def run(
        self,
        test_path,
        *,
        pytest_args=None,
        env=None,
        report_request,            # required: parser-supplied callback
    ) -> RunArtifacts:
        report_dir = "/some/prepared/dir/in/the/container"
        spec = report_request(report_dir)
        # spawn pytest in a container with: [..., *spec.pytest_args, ...]
        # then collect the file from spec.report_path
        return RunArtifacts(exit_code=..., report_path=spec.report_path)

    # optional: override cancel() / cleanup() if you own resources
    # (the base class provides safe no-op defaults)

PytestOperator(task_id="t", test_path="tests/", runner=DockerPytestRunner())
```

## Architecture

| Concern | Type | Responsibility |
|---|---|---|
| `PytestOperator` | operator | orchestrate runner→parser, Airflow integration, fail/cleanup policy |
| `PytestRunner` / `SubprocessPytestRunner` | runner | execute pytest (format-agnostic), produce `RunArtifacts`, own cancel/cleanup |
| `ResultParser` / `JUnitResultParser` / `JSONResultParser` | parser | declare pytest's report args via `report_request`, then `parse` the resulting file into `TestRunResult` |
| `ReportRequest` | domain | what a parser asks pytest to produce (CLI flags + output path) |
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

## Changelog

See [CHANGELOG](CHANGELOG.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
