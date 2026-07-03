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
Tests use `pytest`. Keep public behavioural coverage in `tests/test_public.py`, CLI expectations in `tests/test_cli.py`, and slower full-fixture parity checks in `tests/test_full_fixture.py`. This repository is now frozen as legacy code, so do not update expectations to chase SwiftWABackupAPI 3.0.0 or later unless the user explicitly asks for a one-off Python update.

## Commit & Pull Request Guidelines
Use focused commits with imperative summaries. Before pushing, run the narrowest useful test set for the change and expand to the full suite when the behaviour is broad or touches fixture-driven logic. Call out explicitly when a change is documentation-only or a one-off legacy maintenance update.

## Legacy Port Status
`/Users/domingo/Programacion/PyWABackupAPI` is frozen as legacy code. The
maintained implementation is `/Users/domingo/Programacion/SwiftWABackupAPI`,
starting with SwiftWABackupAPI 3.0.0 and its extracted-backup workflow.

Do not port Swift bug fixes, features, public API changes, CLI changes, JSON
contract changes, or behaviour changes here unless the user explicitly requests
a one-off Python update. The Python API remains at its previous
direct-iPhone-backup workflow.
