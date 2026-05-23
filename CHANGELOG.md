# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/IKrysanov/airflow-pytest-operator/releases/tag/v0.1.0
