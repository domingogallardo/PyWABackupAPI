# Testing

`PyWABackupAPI` has two testing tiers.

## Fast Tests

These are self-contained and safe to run anywhere:

- `tests/test_public.py`
- `tests/test_json_contract.py`
- `tests/test_cli.py`

They build temporary synthetic backups at runtime and do not depend on private chat data.

Run them with:

```bash
PYTHONPATH=src python3.11 -m pytest tests/test_public.py tests/test_json_contract.py tests/test_cli.py
```

## Slow Compatibility Tests

`tests/test_full_fixture.py` validates the Python port against:

- a real local backup fixture
- expected aggregate counts derived from the Swift repository
- a small Swift oracle executable that calls the original library

Requirements:

- the sibling repository `../SwiftWABackupAPI`
- the local fixture under `../SwiftWABackupAPI/Tests/Data`
- `swift` available in the shell

Run with:

```bash
PYTHONPATH=src python3.11 -m pytest tests/test_full_fixture.py
```

If the local fixture is not available, the slow suite skips automatically.

## Full Suite

Run everything:

```bash
PYTHONPATH=src python3.11 -m pytest
```

## What The Slow Suite Checks

- chat names match the Swift implementation
- chat ids and message counts match
- total message-type distribution matches expected counts
- aggregate contact counts match expected counts
- chat `44` payload matches the Swift export exactly
- structural invariants hold across the whole fixture
- WhatsApp Web-validated reaction cases still match expected output
