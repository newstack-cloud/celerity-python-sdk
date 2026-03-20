#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<EOF
Usage: $(basename "$0") [unit|integration|all] [pytest-args...]

Commands:
  unit          Run unit tests only (no Docker needed)
  integration   Run integration tests only (starts/stops Docker services)
  all           Run unit + integration tests (starts/stops Docker services)

Coverage and JUnit XML reports are always generated in the project root:
  coverage.xml      Cobertura coverage report (for SonarCloud)
  test-results.xml  JUnit test results

Any additional arguments are passed to pytest.

Examples:
  ./scripts/run-tests.sh unit
  ./scripts/run-tests.sh unit -k "test_metadata"
  ./scripts/run-tests.sh integration
  ./scripts/run-tests.sh all
EOF
  exit 1
}

## Load test environment variables (dummy AWS credentials etc.)
if [[ -f "$REPO_ROOT/.env.test" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env.test"
  set +a
fi

MODE="${1:-unit}"
shift 2>/dev/null || true

COVERAGE_ARGS=(
  --cov=celerity
  --cov-report=xml:coverage.xml
  --cov-report=term
  --junitxml=test-results.xml
)

cleanup() {
  echo "Stopping services..."
  docker compose down -v || true
}

run_unit() {
  echo "Running unit tests..."
  uv run pytest tests/ --ignore=tests/integration "${COVERAGE_ARGS[@]}" "$@"
}

run_integration() {
  echo "Starting services..."
  docker compose up -d --wait

  trap cleanup EXIT

  echo "Running integration tests..."
  uv run pytest tests/integration "${COVERAGE_ARGS[@]}" "$@"
}

run_all() {
  echo "Starting services..."
  docker compose up -d --wait

  trap cleanup EXIT

  echo "Running all tests..."
  uv run pytest tests/ "${COVERAGE_ARGS[@]}" "$@"
}

case "$MODE" in
  unit)
    run_unit "$@"
    ;;
  integration)
    run_integration "$@"
    ;;
  all)
    run_all "$@"
    ;;
  *)
    usage
    ;;
esac
