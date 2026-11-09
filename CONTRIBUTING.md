# Contributing to StatePact

Thanks for taking the time to contribute. StatePact is correctness-first: a
change that makes an isolation guarantee weaker, even if it is faster, is a
regression.

## Development setup

Requirements: Python 3.12+, [uv](https://docs.astral.sh/uv/), and optionally
Docker for the PostgreSQL and Redis services.

```bash
git clone https://github.com/mithun-27/StatePact
cd StatePact
uv sync --extra dev
```

Run the fast suite:

```bash
uv run pytest -q                 # unit, property, concurrency, failure injection
uv run ruff check src tests demo benchmarks
uv run mypy src
```

Run the integration suite against real services:

```bash
docker compose up -d postgres redis

export STATEPACT_TEST_POSTGRES_URL='postgresql://statepact:statepact@localhost:5432/statepact'
export STATEPACT_TEST_REDIS_URL='redis://localhost:6379/0'

uv run pytest tests/integration -q
```

On PowerShell use `$env:NAME='value'` instead of `export`.

The integration tests run the same behavioural contract
(`tests/integration/backend_suite.py`) against PostgreSQL and Redis. If you add
a backend, make it pass that suite before adding backend-specific tests.

## What to work on

* Bugs and design questions: open an issue first for anything larger than a
  small fix, so the approach can be agreed before code is written.
* Good first issues are labelled `good first issue`.
* Security problems go to the address in [SECURITY.md](SECURITY.md), not the
  public issue tracker.

## Code style

* Ruff enforces linting and formatting (`uv run ruff format --check ...`).
* mypy runs in strict mode over `src/`. New public functions need annotations.
* Keep the public API small. Internals live under `statepact.core`,
  `statepact.concurrency`, `statepact.coordination` and
  `statepact.storage`.
* Framework adapters must not import their framework at module load. Keep the
  adapter surface thin and test it without the framework installed.
* Prefer explicit errors over silent fallbacks. Every failure path should answer:
  did it commit, is retry safe, and which resource/key failed.
* Add a docstring to every public class and function; the docs build reads them.

## Tests that matter

Beyond line coverage, these invariants are the contract:

* `none` loses updates; `optimistic`/`serializable` do not.
* `snapshot` permits write skew; `optimistic`/`serializable` reject one side.
* `pessimistic` serialises writers and surfaces deadlock/timeout as retryable
  conflicts.
* A replayed idempotency key returns the stored result and does not reapply writes.
* A stale fencing token is rejected with `LeaseLostError` (`retryable=False`).
* An unknown commit failure is recorded as `FAILED`, never assumed committed.

Property tests use Hypothesis; keep them deterministic with a fixed seed when
reporting a failure. Concurrency tests should use `statepact.testing.run_concurrently`.

## Commit messages

```
<type>(<scope>): <summary>

Why the change is needed, and any trade-off.

Refs: #123
```

`type` is one of `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`,
`ci`, `chore`. Keep the summary imperative and under ~72 characters.

## Pull request checklist

- [ ] `uv run pytest -q` passes.
- [ ] Integration tests pass if the change touches a backend.
- [ ] `uv run ruff check src tests demo benchmarks` is clean.
- [ ] `uv run mypy src` is clean.
- [ ] New behaviour has tests, including a failure-path test.
- [ ] Public API changes are documented in `docs/api.md` and `CHANGELOG.md`.
- [ ] README/docs claims match the implementation (no aspirational features).
- [ ] Benchmark invariants still hold if the change touches the commit path.

## Documentation and benchmarks

```bash
uv sync --extra docs
uv run mkdocs build --strict
```

To re-run the benchmark (takes a few minutes and needs Docker services):

```bash
uv run python benchmarks/run.py \
  --backends memory,postgres,redis --agents 40 --ops 10 --trials 20
```

The runner writes a timestamped JSON artifact and `latest.json` under
`benchmarks/results/`. Do not edit those files by hand; re-run the runner. When
you cite numbers in docs, cite the artifact filename.

## Releasing

Release steps are maintainer-only and require PyPI Trusted Publishing. The full,
reproducible procedure — version bump, gate commands, tagging, publishing and
post-release verification — lives in [RELEASING.md](RELEASING.md).

Two rules matter most:

1. Never yank a published release. Old versions stay installable; ship the next
   patch instead.
2. Tags are immutable. Cut `vX.Y.Z` only from a commit whose full gate passed.

## License

By contributing you agree that your contribution is licensed under
Apache-2.0, as described in [LICENSE](LICENSE).
