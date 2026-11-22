# Celerity Python SDK

The `celerity-sdk` package for building Celerity applications in Python. Celerity's Python
runtime image starts applications through it, the Celerity CLI runs its `celerity-extract` tool
and generates AWS Lambda entry files that import its adapter, and it uses the runtime binding
`celerity-runtime-sdk` from celerity's `libs/runtime/sdk/python`.

## Workspace

This repo is one of NewStack's. NewStack's maintainers develop it alongside the others in a
shared workspace, using the NewStack Claude Code plugin for how work is planned, reviewed,
committed and coordinated across repos. The plugin isn't public, and nothing here depends on it:
this file and the repo's own docs cover what a contribution needs.

## Layout

One package in `src/celerity/` (src layout); [CONTRIBUTING.md](CONTRIBUTING.md) lists its
subpackages. Tests are flat in `tests/` (`test_*.py`), with Docker-backed tests in
`tests/integration/`. Optional dependencies are extras in `pyproject.toml` (`runtime`, `aws`,
`cache`, `resources-sql`, `telemetry`, `gcp`, `azure`, `test`, and an empty `cli` that the
Celerity CLI tells users to install); `dev` pulls in all but `gcp` and `azure`. A development
dependency goes in the `dev` extra: `uv add --dev` writes a dependency group that
`--extra dev` never installs.

`docs/design/` isn't in the repo: maintainers keep working notes there, untracked and not
gitignored, so stage files by path rather than with `git add -A`.

## Checks

Commands are in [CONTRIBUTING.md](CONTRIBUTING.md).

- **Tools:** Python 3.13 (`.python-version`), uv, docker for integration tests, and Node with
  Yarn classic for commitlint. Run `uv sync --extra dev` first; plain `uv sync` installs no
  test or lint tools. `uv.lock` is gitignored: each checkout keeps its own, while CI and new
  worktrees resolve the latest versions, so an upstream release can break CI with no change
  here.
- **Tests:** `./scripts/run-tests.sh unit|integration|all` (default `unit`), extra arguments
  passed to pytest. It loads `.env.test` (dummy AWS credentials, `AWS_ENDPOINT_URL` set to
  LocalStack) and always writes `coverage.xml` and `test-results.xml`.
  - `integration` and `all` start `docker-compose.yml` (Valkey on 6399, LocalStack on 4566,
    Postgres on 5499) and stop it with `docker compose down -v` on exit. The tests reach only
    these containers.
  - Run one stack at a time. The compose project name is fixed
    (`celerity-python-sdk-integration`), so a run in another worktree shares the stack and its
    teardown removes it, and the Node SDK's stack binds the same three ports. If startup fails,
    the containers it started are left running.
- **Lint and types:** `uv run ruff format --check src/ tests/`, `uv run ruff check src/ tests/`,
  `uv run mypy src/` (strict).
- **Gates:**
  - With `git config core.hooksPath .githooks`, pre-commit runs ruff format and lint over the
    working tree (not mypy or tests) when Python files are staged. commit-msg runs commitlint
    once `yarn install` has run in that checkout, so each worktree needs its own; without it, a
    regex checks the type and the scope's syntax but not the allowed scopes.
  - CI (`ci.yml`) runs `pip-audit`, ruff format and lint, mypy, `run-tests.sh all` and
    SonarCloud on pull requests and pushes to `main`. `codeql.yml` runs CodeQL on the same
    triggers and weekly, and `security.yml` runs actionlint on pull requests.

## Compatibility

- **Package API:** pre-1.0. release-please here doesn't set `bump-minor-pre-major`, so a
  breaking change releases 1.0.0; don't mark one as breaking without agreeing the release
  first. Resource contracts are kept at parity with the Node and Go SDKs; a contract change
  says what each other SDK needs.
- **Runtime image:** celerity's Python runtime image tracks this package on major.minor, and
  the Celerity CLI picks the image from the SDK version a project declares
  (`apps/runtime/python/RELEASING.md` in celerity). Each minor release here needs a runtime
  release at the same minor, and breaking changes stay out of `fix:` releases.
- **Entry points other repos import:** celerity's `apps/runtime/python` calls
  `celerity.bootstrap.runtime_orchestrator.start_runtime`, and the Lambda entry file the
  Celerity CLI generates calls `celerity.serverless.aws.create_lambda_handler()`. Renaming or
  moving either, or changing its signature, needs a matching celerity change first. Lambda
  deployments install the package with no extras, so `import celerity.serverless.aws` (and
  `celerity/__init__.py` with it) must not import an optional dependency at module level.
- **Handler manifest and `celerity-extract`:** the Celerity CLI runs
  `celerity-extract --module <path> --project-root <dir>` from the project's `.venv/bin` or
  `venv/bin` and parses all of stdout as the handler-manifest.v1 JSON the Node SDK's extractor
  also writes, so anything else printed to stdout, at import time included, breaks extraction;
  warnings go to stderr. Additive only. Nothing here validates the output: a new field goes in
  the schema in the Node SDK (`packages/cli/schemas/`) and in the Go structs in celerity's
  `apps/cli/internal/preprocess/manifest.go`, or the CLI drops it.
- **Runtime binding:** `celerity-runtime-sdk` (the `runtime` extra) is published from celerity;
  a change there reaches this repo by raising the minimum version in `pyproject.toml`.
- **Persisted state:** none.

## Conventions

- Commits: conventional, with the types in `commitlint.config.js`. Its scopes are allowed but
  optional, and history is almost entirely unscoped; `src/celerity/config` has no scope.
- Releases: release-please opens one release pull request; merging it publishes to PyPI. The
  release workflow can also be run by hand to publish. Don't edit `CHANGELOG.md`, the version
  in `pyproject.toml` or `.release-please-manifest.json` by hand.
- Dependency updates: Dependabot (`.github/dependabot.yml`), for pip and GitHub Actions.
- Async-first: handlers and DI resolution are async.
- Dataclasses for internal types, Pydantic for user-facing validation. ABCs for behavioural
  contracts (`CelerityLayer`, `ServiceContainer`, resource interfaces); `Protocol` only for
  structural ones (`Schema`).
- Decorators are snake_case and store metadata in a `__celerity_metadata__` dict attribute.
  PascalCase is for classes only, including parameter types (`Body`, `Query`, `Param`).
- Google-style docstrings on public APIs. No wildcard imports.
- New code and comments don't mention the Node SDK: this SDK documents itself, and parity
  notes belong in the design notes. `layers/validate.py` predates this.

## Safety

- Don't publish to PyPI, create tags or releases, or merge release pull requests.

## Known mistakes

<!-- One line per mistake an agent has made here, phrased as the rule that prevents it. -->
