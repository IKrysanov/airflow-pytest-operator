# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-05-24

### Fixed
- Resolve a circular import on Airflow 3.2.x that surfaced as
  ``partially initialized module 'airflow_pytest_operator' has no attribute
  'get_provider_info'`` (often seen as a `BaseOperator` ImportError). The
  provider-discovery entry point now lives in a dedicated import-light
  module (`airflow_pytest_operator.provider_info`) that pulls in nothing
  from the package, so Airflow's early provider scan no longer triggers the
  operator/compat imports mid-initialization.

## [0.2.0] - 2026-05-24

### Fixed
- Resolve `BaseOperator` from `airflow.sdk.bases.operator` on Airflow 3,
  eliminating the `DeprecatedImportWarning` that came from the legacy
  `airflow.models.baseoperator` path. Import resolution now prefers the
  canonical Task SDK location and only falls back to the Airflow 2 path on
  Airflow 2.
- Reject concurrent `run()` calls on a single `SubprocessPytestRunner`
  instance (fail-fast with `TestExecutionError`) to prevent a race on the
  per-run temporary directory and process handle. Separate runner instances
  remain fully independent, and the same instance can be reused for
  sequential runs (e.g. task retries).
- `cancel()` no longer holds the internal lock during its graceful
  termination wait (up to `grace_period` seconds). The lock is now held only
  to snapshot the process handle, so `on_kill` no longer serializes the
  run's teardown or `cleanup()`.
- Wrap report-directory preparation so a user-supplied `report_dir` pointing
  at a file (or an unwritable location) raises `TestExecutionError` per the
  runner contract, instead of leaking a bare `OSError`.

### Changed
- **Breaking:** the test summary is now exposed only via the standard XCom
  `return_value` key. The previously-duplicated custom `pytest_result` key
  has been removed. Downstream tasks that read `xcom_pull(key="pytest_result")`
  must switch to `xcom_pull(task_ids="<task>")` (the default `return_value`).
- **Breaking:** removed the custom `push_result` parameter. XCom output is
  now controlled solely by Airflow's standard `do_xcom_push` (default `True`);
  pass `do_xcom_push=False` to disable. Replace `push_result=False` with
  `do_xcom_push=False`.

### Added
- Documentation on installing the package in Docker/constrained environments
  using Airflow's official constraint files.

## [0.1.0] - 2026-05-23

Initial release.

### Added
- `PytestOperator` — runs a pytest suite as an Airflow task, parses the JUnit
  report into a structured result, optionally pushes a summary to XCom under the
  `pytest_result` key, and fails the task on test failure (configurable via
  `fail_on_test_failure`).
- `PytestRunner` abstraction with a default `SubprocessPytestRunner` that runs
  `python -m pytest` in a child process using the worker's own interpreter and
  virtualenv.
- `ResultParser` abstraction with a default `JUnitResultParser` (uses
  `defusedxml` when available via the `secure-xml` extra).
- Airflow 2.x / 3.x compatibility through a single `compat.airflow` shim.
- Graceful cancellation: `on_kill` terminates the whole pytest process tree
  (`SIGTERM` → `grace_period` → `SIGKILL`), covering `xdist` workers and nested
  processes.
- Automatic working-directory resolution so relative paths in pytest `addopts`
  (e.g. Allure's `--alluredir`) resolve next to the tests.
- Temporary report-directory cleanup with `cleanup` policy
  (`"always"` | `"on_success"` | `"never"`); user-supplied `report_dir` is never
  removed; cleanup also runs on task kill.
- `run` timeout support and clear `TestExecutionError` when pytest produces no
  report (collection error, crash, wrong path).
- `push_result=False` suppresses all XCom output, including Airflow's automatic
  return-value push.
- Packaged as an Airflow provider (`get_provider_info` entry point), Apache-2.0
  licensed.

[Unreleased]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/IKrysanov/airflow-pytest-operator/releases/tag/v0.1.0
