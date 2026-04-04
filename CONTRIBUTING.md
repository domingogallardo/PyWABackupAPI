# Contributing

## Development Setup

Clone this repository next to `SwiftWABackupAPI` if you want to run the full compatibility suite:

```text
Programacion/
  PyWABackupAPI/
  SwiftWABackupAPI/
```

Install the package in editable mode:

```bash
python3.11 -m pip install --user -e .
```

## Test Commands

Run the full Python suite:

```bash
PYTHONPATH=src python3.11 -m pytest
```

Run only the fast suite:

```bash
PYTHONPATH=src python3.11 -m pytest tests/test_public.py tests/test_json_contract.py tests/test_cli.py
```

Run only the slow fixture-backed suite:

```bash
PYTHONPATH=src python3.11 -m pytest tests/test_full_fixture.py
```

## Notes

- The slow suite references the large local backup fixture from `../SwiftWABackupAPI/Tests/Data`; it does not copy that fixture into this repository.
- The Swift oracle under `tests/swift_oracle/` depends on the sibling Swift package.
- Avoid committing generated caches, exported chats, or copied media.
