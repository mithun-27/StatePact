# Releasing StatePact

This is the release runbook. It is deliberately mechanical: every release should
be reproducible from a tag and verifiable from public artifacts.

## Versioning policy

* StatePact follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
* Patch releases (`0.1.x`) may fix bugs, correct documentation and harden CI, but
  must **not** remove or change public API behaviour. Upgrading from the previous
  patch must need no code changes.
* New features and breaking changes wait for a minor release.
* **Never yank a release.** PyPI versions are immutable and old versions must stay
  installable. If a release is wrong, ship the next patch.

## 1. Prepare the branch

```bash
git switch main
git pull --ff-only
git switch -c release/X.Y.Z
```

Bump the single source of truth:

```bash
# src/statepact/__about__.py
__version__ = "X.Y.Z"
```

Update in the same commit:

* `CHANGELOG.md` — move items out of `[Unreleased]`, add a dated `[X.Y.Z]`
  section with an **Upgrade notes** subsection.
* `README.md` and `docs/` — any install or current-version text.
* `pyproject.toml` — project URLs and classifiers, if they changed.

## 2. Run the full gate

Use the project Python (3.12+) through `uv`, with all extras installed together:

```bash
uv sync --extra dev --extra docs --extra postgres --extra redis --extra otel --extra demo
uv run ruff check src tests demo benchmarks
uv run ruff format --check src tests demo benchmarks
uv run mypy src
uv run pytest -q
uv run mkdocs build --strict
uv build
uvx twine check dist/*
```

The integration tests need real services:

```bash
docker compose up -d postgres redis
$env:STATEPACT_TEST_POSTGRES_URL = "postgresql://statepact:statepact@localhost:5432/statepact"
$env:STATEPACT_TEST_REDIS_URL = "redis://localhost:6379/0"
uv run pytest -q tests/integration
```

Install the built wheel into a clean environment and exercise it:

```bash
uv venv --python 3.12 .venv-release
uv pip install --python .venv-release/bin/python dist/*.whl
.venv-release/bin/statepact --version
.venv-release/bin/statepact info
.venv-release/bin/statepact smoke
```

Assert that `statepact.__version__` matches `importlib.metadata.version("statepact")`,
that `statepact/py.typed` is in the wheel, and that the wheel contains no
`__pycache__` or `.pyc` files.

## 3. Merge and tag

```bash
git add -A
git commit -m "Release X.Y.Z"
git switch main
git merge --no-ff release/X.Y.Z
git push origin main
git tag -a vX.Y.Z -m "StatePact X.Y.Z"
git push origin vX.Y.Z
```

Tags are annotated and point at the exact commit that CI validated. Do not move or
re-point a published tag; cut a new patch instead.

To make an earlier release reproducible when its tag is missing, tag the release
commit retroactively:

```bash
git tag -a v0.1.0 63b9378 -m "StatePact 0.1.0"
git push origin v0.1.0
```

## 4. Publish to PyPI

The `Publish` workflow (`.github/workflows/publish.yml`) uses PyPI Trusted
Publishing — no API token is stored in the repository.

Either:

* publish a non-prerelease GitHub Release for the tag (recommended — it records
  release notes and triggers PyPI), or
* run **Actions → Publish → Run workflow** and choose `target: pypi`.

Publishing a GitHub Release marked as a prerelease sends it to TestPyPI instead.
Release notes should start with the upgrade impact, for example:

> Patch release. No API or behaviour changes; upgrading from 0.1.0 needs no code
> changes. 0.1.0 remains available and is not yanked.

## 5. Verify the public artifacts

```bash
curl -s https://pypi.org/pypi/statepact/json
```

Check that:

* the new version appears with both a wheel and an sdist;
* `yanked` is `false` for the new files **and** for the previous release;
* the rendered description matches the README (no stale "not yet on PyPI" text);
* `pip install statepact==X.Y.Z` and `pip install statepact==<previous>` both
  resolve.

Also confirm the docs site rebuilt:

```bash
curl -sI https://mithun-27.github.io/StatePact/
```

And that the demo Space still reports `RUNNING` in the Hugging Face UI.

## 6. After the release

* Close the release milestone and open the next `[Unreleased]` section.
* Delete the release branch once merged.
* Rotate any credentials that were used during the release.
* If the release changed engine code, rerun the benchmarks and commit the new
  artifact under `benchmarks/results/`. Documentation-only patches keep the
  existing artifact and numbers.
