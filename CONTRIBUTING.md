# Contributing

Thanks for your interest in improving **airflow-pytest-operator**! This guide covers how to set up a dev environment, the checks your change must pass, and how to submit it.

## Getting started

The package targets Python 3.9+ and supports Airflow 2.x and 3.x. You do **not** need Airflow installed to develop or run the test suite — the suite stubs `BaseOperator` when Airflow is absent.

```bash
git clone https://github.com/IKrysanov/airflow-pytest-operator
cd airflow-pytest-operator
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Quality gates

Every change must pass all three checks. They run in CI and you should run them locally first:

```bash
ruff check src tests     # lint + import order
mypy                     # strict static type checking
pytest                   # full test suite
```

`ruff format src tests` will apply formatting. The configuration for all three tools lives in `pyproject.toml`.

## Design principles

This project follows SOLID deliberately; please keep new code consistent with it.

- **The operator stays thin.** It orchestrates a runner and a parser and integrates with Airflow — nothing more. No subprocess logic, no XML parsing.
- **Extend, don't modify.** New execution strategies are new `PytestRunner` subclasses; new report formats are new `ResultParser` subclasses. Avoid adding branches to existing classes for new behaviour.
- **`compat/airflow.py` is the only module that imports Airflow.** Supporting a new Airflow version should be a change confined to that file.
- **Domain models stay framework-free.** `models.py` must not import Airflow, pytest, or subprocess.
- **Teardown must never raise.** `on_kill`, `cancel`, and `cleanup` run during termination; swallow and log their errors.

## Tests

- Add tests for any behaviour change; bug fixes should come with a regression test.
- Prefer testing the operator with injected fakes (see `tests/test_operator.py`) over spinning up Airflow.
- Runner tests use real child processes (see `tests/test_subprocess_runner.py`); keep them fast and deterministic.
- New public exceptions should follow the `...Error` naming convention.

## Commit messages and PRs

- Use clear, imperative commit subjects (e.g. "Add Docker runner", "Fix temp dir leak on kill").
- Keep PRs focused on one concern; smaller PRs review faster.
- Describe the motivation and any platform-specific considerations (especially around process handling and Airflow version differences).
- Update `README.md` and `CHANGELOG.md` when behaviour or the public API changes.

## Developer Certificate of Origin (DCO)

By contributing, you certify that you wrote the code or otherwise have the right to submit it under the project's Apache-2.0 license (see the [DCO](https://developercertificate.org/)). Sign off your commits:

```bash
git commit -s -m "Your message"
```

This adds a `Signed-off-by` line and is required for any future inclusion in the Apache Airflow ecosystem.

## License

By contributing, you agree that your contributions are licensed under the Apache License 2.0.
