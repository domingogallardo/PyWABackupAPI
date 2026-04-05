# Repository Guidelines

## Project Structure & Module Organization
The Python package metadata lives in `pyproject.toml`; library code is under `src/pywabackupapi`, with the CLI in `src/pywabackupapi/cli.py` and the public package surface re-exported from `src/pywabackupapi/__init__.py`. Tests live in `tests`, including the public synthetic suite, CLI coverage, and the slower full-fixture comparisons that reference data from the Swift repository instead of copying it locally.

## Build, Test, and Development Commands
- `python3.11 -m pytest` — run the full Python test suite.
- `python3.11 -m pytest tests/test_public.py` — run the fast synthetic public tests while iterating.
- `python3.11 -m pytest tests/test_full_fixture.py` — run the slower full-fixture regression checks against the shared Swift fixture.
- `python3.11 -m pywabackupapi --help` — inspect the installed CLI entry points.

## Coding Style & Naming Conventions
Follow normal Python conventions: `snake_case` for functions, methods, and variables, `PascalCase` for classes, and clear dataclass-based models for public payloads. Prefer small helpers over deeply nested logic, keep side effects close to the CLI or filesystem layers, and preserve the current standard-library-first approach unless a dependency is clearly justified.

## Testing Guidelines
Tests use `pytest`. Keep public behavioural coverage in `tests/test_public.py`, CLI expectations in `tests/test_cli.py`, and slower full-fixture parity checks in `tests/test_full_fixture.py`. When fixture expectations change, update both the Python assertions and the corresponding Swift-side tests so both ports continue to describe the same behaviour.

## Commit & Pull Request Guidelines
Use focused commits with imperative summaries. Before pushing, run the narrowest useful test set for the change and expand to the full suite when the behaviour is broad or touches fixture-driven logic. Call out any cross-repo parity work in the commit or PR description when the Python port is following a Swift change.

## Cross-Repo Synchronization
`/Users/domingo/Programacion/PyWABackupAPI` and `/Users/domingo/Programacion/SwiftWABackupAPI` must evolve in parallel. Treat the Swift repository as the canonical source for behaviour, but do not allow the Python port to drift behind it.

Any bug fix, feature, public API change, CLI change, JSON contract change, or behaviour change introduced in `SwiftWABackupAPI` must be ported here in the same workstream, together with the corresponding tests and documentation updates when applicable. Do not intentionally leave the Python repo behind unless the user explicitly approves a temporary divergence.
