# Contributing

## Prerequisites

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/) >= 0.5
- Node.js >= 22 (for commitlint only)
- yarn (for commitlint only)
- Docker (for integration tests)

## Getting Started

```bash
# Clone the repository
git clone git@github.com:newstack-cloud/celerity-python-sdk.git
cd celerity-python-sdk

# Install Python dependencies
uv sync --extra dev

# Install commitlint (Node.js dev tooling)
yarn install

# Set up git hooks for conventional commits
git config core.hooksPath .githooks

# Run unit tests
./scripts/run-tests.sh unit

# Run integration tests
./scripts/run-tests.sh integration

# Run all tests (unit + integration)
./scripts/run-tests.sh all

# Run linting and type checking
uv run ruff check src/ tests/
uv run mypy src/
```

## Development

```bash
# Format code
uv run ruff format src/ tests/

# Lint with auto-fix
uv run ruff check --fix src/ tests/

# Type check
uv run mypy src/

# Run a specific test file
uv run pytest tests/test_types.py

# Run tests matching a keyword
uv run pytest -k "test_metadata"
```

## Managing Dependencies

```bash
# Add a runtime dependency
uv add httpx

# Add a dev dependency
uv add --dev pytest-mock

# Add a dependency to an extra group
# (edit pyproject.toml [project.optional-dependencies] directly)

# Sync after editing pyproject.toml
uv sync --extra dev
```

## Conventional Commits

This project uses [conventional commits](https://www.conventionalcommits.org/) enforced by commitlint.

Format: `type(scope): description`

**Types**: `feat`, `fix`, `build`, `revert`, `wip`, `chore`, `ci`, `docs`, `style`, `refactor`, `perf`, `test`, `instr`, `deps`

**Scopes**: `types`, `common`, `metadata`, `decorators`, `di`, `bootstrap`, `handlers`, `layers`, `errors`, `functions`, `testing`, `cli`, `serverless`, `resources`, `telemetry`, `ci`, `repo`, `deps`

Examples:
```
feat(handlers): add HTTP handler pipeline
fix(di): resolve circular dependency detection
chore: update dependencies
docs(resources): add datastore usage examples
test(bootstrap): add module graph integration tests
```

## Project Structure

```
src/celerity/          SDK source code (src layout)
├── types/             Shared data types and ABCs
├── common/            Shared utilities
├── metadata/          Metadata storage primitives
├── decorators/        Handler and parameter decorators
├── di/                Dependency injection container
├── bootstrap/         Application bootstrap and module graph
├── handlers/          Handler registry, scanners, pipelines
├── layers/            Middleware layer pipeline
├── errors/            HTTP exceptions
├── functions/         Function handler factories
├── testing/           Test utilities (TestApp, mocks)
├── cli/               Handler manifest extraction CLI
├── serverless/        Cloud adapters (AWS, GCP, Azure)
├── resources/         Cloud-agnostic resource clients
└── telemetry/         Logging and tracing
tests/                 Test suite
tests/integration/     Integration tests (require Docker services)
scripts/               Build and test scripts
docs/design/           Phase-by-phase design documents
```

## Testing

Use `scripts/run-tests.sh` to run tests. Coverage and JUnit XML reports are always generated.

```bash
# Unit tests only (no Docker needed)
./scripts/run-tests.sh unit

# Integration tests only (starts/stops Docker services)
./scripts/run-tests.sh integration

# All tests — unit + integration (starts/stops Docker services)
./scripts/run-tests.sh all

# Pass additional pytest args
./scripts/run-tests.sh unit -k "test_metadata"
```

All Docker-dependent commands (`integration`, `all`) handle the full Docker Compose lifecycle automatically — starting services, running tests, and tearing down — so you never need to manage Docker manually.

Reports generated:
- `coverage.xml` — Cobertura coverage report (consumed by SonarCloud)
- `test-results.xml` — JUnit test results

To manage Docker services manually for iterative development:

```bash
# Start services in the background
docker compose up -d --wait

# Run integration tests directly (services already running)
uv run pytest tests/integration/

# Stop services
docker compose down -v
```

### Docker Services

The `docker-compose.yml` provides:

- **Valkey** (port 6399) — Redis-compatible cache, queue, and topic testing
- **LocalStack** (port 4566) — AWS service emulation (S3, SQS, SNS, DynamoDB, SSM, Secrets Manager)
- **PostgreSQL** (port 5499) — SQL database testing

## Code Quality

All checks must pass before merging:

- **Formatting**: `uv run ruff format --check src/ tests/`
- **Linting**: `uv run ruff check src/ tests/`
- **Type checking**: `uv run mypy src/`
- **Tests**: `./scripts/run-tests.sh all`

The pre-commit hook runs format and lint checks automatically on staged Python files.
