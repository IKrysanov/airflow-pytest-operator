# Contributing

## Table of Contents

- [Getting started](#getting-started)
- [Quality gates](#quality-gates)
- [Design principles](#design-principles)
- [License headers on new files](#license-headers-on-new-files)
- [Tests](#tests)
- [Branching and pull requests](#branching-and-pull-requests)
- [Commit messages and PRs](#commit-messages-and-prs)
- [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco)
- [Reviewing and merging (for maintainers)](#reviewing-and-merging-for-maintainers)
- [License](#license)

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

## License headers on new files

Every source file carries the project's Apache-2.0 header. The copyright
line is **deliberately impersonal** — it names the project's contributors
collectively, not any individual. When you add a new file, copy this header
verbatim; do **not** put your own name in it. Your authorship is recorded by
your commit's `Signed-off-by` line and the Git history, which is exactly what
makes per-file name-juggling unnecessary.

```python
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
```

In Python files this goes **below** the module docstring (so the docstring
stays the first statement), as in the existing modules. You keep copyright
over your own contributions — see `NOTICE`; the collective header simply
spares everyone from editing names file by file.

## Tests

- Add tests for any behaviour change; bug fixes should come with a regression test.
- Prefer testing the operator with injected fakes (see `tests/test_operator.py`) over spinning up Airflow.
- Runner tests use real child processes (see `tests/test_subprocess_runner.py`); keep them fast and deterministic.
- New public exceptions should follow the `...Error` naming convention.

## Branching and pull requests

This project uses **GitHub Flow**: `main` is the only long-lived branch
and is always in a releasable state. Releases are tags (`vX.Y.Z`) on
specific `main` commits; there is no `develop` branch.

Concretely, to contribute a change:

1. **Fork** the repository on GitHub.
2. **Create a topic branch from `main`** in your fork. Name it after the
   change, e.g. `feat/docker-runner`, `fix/temp-dir-leak`, `docs/readme`.
   Don't work on your fork's `main` directly — keep it clean so you can
   rebase easily.
3. **Open a pull request into `IKrysanov/airflow-pytest-operator:main`**.
   Draft PRs are welcome for early feedback.
4. **Rebase, don't merge `main`** if `main` moves while your PR is open:
   `git fetch upstream && git rebase upstream/main`, then
   `git push --force-with-lease`. This keeps history linear and CI clean.
5. Keep the PR **focused on one concern** — smaller PRs review faster and
   are easier to revert if needed.

Direct pushes to `main` are reserved for maintainers and for release tags.
Unreleased work accumulates under the `[Unreleased]` section of
`CHANGELOG.md` until a release is cut.

## Commit messages and PRs

- Use clear, imperative commit subjects (e.g. "Add Docker runner", "Fix temp dir leak on kill").
- Keep PRs focused on one concern; smaller PRs review faster.
- Describe the motivation and any platform-specific considerations (especially around process handling and Airflow version differences).
- Update `README.md` and `CHANGELOG.md` when behaviour or the public API changes.

## Developer Certificate of Origin (DCO)

This project tracks contribution provenance with the
[DCO](https://developercertificate.org/) rather than a CLA. By signing off a
commit you certify that you wrote the code, or otherwise have the right to
submit it under the project's Apache-2.0 license. Sign off every commit:

```bash
git commit -s -m "Your message"
```

This appends a `Signed-off-by: Your Name <you@example.com>` line using your
configured `git` identity. If you forgot to sign off, fix it before pushing:

```bash
git commit --amend -s --no-edit          # last commit
git rebase --signoff main                # a whole branch of commits
```

The sign-off is checked automatically on every pull request by the project's
`DCO` workflow (`.github/workflows/dco.yml`), which blocks merge until all
commits are signed. This keeps a clear, lightweight record that each
contribution was submitted deliberately and lawfully — which protects both
you and the project.

## Reviewing and merging (for maintainers)

A pull request is ready to approve when:

1. **Targets `main`** — this project uses GitHub Flow; PRs to any other base
   branch should be retargeted before review.
2. **CI is green** — lint, type-check, the unit matrix, and all three
   integration jobs (Airflow 2.10.3, 3.0.6, 3.2.1) pass.
3. **DCO check passes** — every commit is signed off. If not, ask the
   contributor to amend/rebase with `--signoff`; do not merge unsigned
   commits.
4. **Coverage holds** — the `fail_under` gate is satisfied and Codecov does
   not report a meaningful drop. New behaviour comes with new tests.
5. **License header present** on any new file, using the collective header
   above (no individual names).
6. **Design principles respected** — especially: the operator stays thin,
   new behaviour arrives as new `PytestRunner`/`ResultParser` subclasses
   rather than branches in existing classes, and only `compat/airflow.py`
   imports Airflow.
7. **Docs updated** — `README.md` and `CHANGELOG.md` reflect any
   behaviour or public-API change.

Use **rebase-merge** when the PR is already a clean series of well-scoped
commits (each signed off, each green if you imagine CI on it); use
**squash-merge** when the PR is a single concern spread across many small
fixup commits. Either way, the resulting commit on `main` must keep a
`Signed-off-by` trailer. Tag a release only from a green `main`.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, and that you hold copyright over your own contributions
as one of the project's contributors (see [NOTICE](NOTICE)).
