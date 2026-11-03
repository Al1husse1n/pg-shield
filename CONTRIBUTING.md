# Contributing to PGShield

This guide covers setting up your development environment, executing the test suite, and submitting pull requests.

## Prerequisites

- **Python 3.13+**: Required runtime environment.
- **Poetry**: Dependency and virtual environment manager (`poetry >= 2.0`).
- **Docker & Docker Compose** (optional): For running integration tests via the compose workflow.

## Installation

Clone the repository and install dependencies inside the `Main/` directory:

```bash
cd Main
poetry install
```

## Running Unit Tests

Unit tests execute without an external database connection:

```bash
cd Main
pytest -q
```

## Running Integration Tests Locally

Integration tests require a reachable PostgreSQL 16+ instance. Set `PGSHIELD_TEST_DSN` and run pytest with `-m integration`:

```bash
# On Linux/macOS (typical default port 5432):
export PGSHIELD_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

# On Windows dev hosts (if running on port 5433):
# $env:PGSHIELD_TEST_DSN="postgresql://postgres@127.0.0.1:5433/postgres"

cd Main
pytest -q -m integration
```

Note: on the current Windows dev host, PostgreSQL runs on port 5433; on Linux/Mac dev hosts, 5432 is typical.

## Running Integration Tests via Docker Compose

To test against an isolated PostgreSQL container via Docker Compose:

```bash
docker compose up -d --wait
cd Main
export PGSHIELD_TEST_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
pytest -q -m integration
cd ..
docker compose down
```

## Continuous Integration (CI) Architecture

GitHub Actions runs five distinct jobs on every push and pull request:

- **lint**: Enforces code style, import sorting, and line length via `ruff check .` and formatting.
- **type-check**: Enforces strict static type checking with `mypy src/pgshield`.
- **unit**: Runs all unit tests (`pytest -q`) in an isolated Python 3.13 environment without database dependencies.
- **integration**: Runs integration tests (`pytest -q -m integration`) against a native GitHub Actions PostgreSQL 16 service container.
- **compose**: Verifies `docker-compose.yml` startup, validates healthchecks, runs integration tests against the compose cluster, and tears down the stack.

## Building the Package Locally

To build distribution packages (source archive and wheel) locally:

```bash
cd Main
pip install build twine
python -m build
twine check dist/*
```

This creates `dist/pgshield-<version>.tar.gz` (sdist) and `dist/pgshield-<version>-py3-none-any.whl` (wheel), and verifies metadata with `twine check`.

## Publishing Releases

PGShield releases are automated through GitHub Actions:
- **TestPyPI**: Pushes with version tags (`v*`) or manual triggers via `workflow_dispatch` invoke `.github/workflows/publish.yml` to build the distribution and publish artifacts to [TestPyPI](https://test.pypi.org/project/pgshield/) via Trusted Publishing.

## Definition of Done for a PR

Before submitting a Pull Request, verify:
1. Lint and formatting checks pass (`ruff check .`).
2. Type checking passes (`mypy src/pgshield`).
3. Unit tests pass locally (`pytest -q`).
4. Integration tests pass locally or via Docker Compose (`pytest -q -m integration`).
5. All GitHub Actions CI checks are green.
6. The PR contains no unrelated changes, temporary artifacts, or formatting churn.

## Branch Protection

The main branch should be protected via GitHub branch protection rules requiring the lint, type-check, unit, integration, and compose CI checks to pass before merge. This is a repository settings change, not a workflow file change; configure it in GitHub -> Settings -> Branches after the first successful CI run.

