# PyWABackupAPI

> **Legacy status**
>
> `PyWABackupAPI` is frozen as legacy code and is no longer maintained in
> parallel with [`SwiftWABackupAPI`](https://github.com/domingogallardo/SwiftWABackupAPI).
> The maintained implementation is now SwiftWABackupAPI 3.0.0 and later.
>
> This repository remains available for historical reference and for users who
> still need the previous Python behaviour. It will not receive the Swift 3.0
> extracted-backup architecture unless a one-off update is explicitly requested.

`PyWABackupAPI` is the former Python port of [`SwiftWABackupAPI`](https://github.com/domingogallardo/SwiftWABackupAPI) for exploring WhatsApp data stored inside iPhone backups that contain WhatsApp data. It includes backup-discovery diagnostics for encrypted backups, while the chat and export APIs still operate on backups that are confirmed to be non-encrypted.

Its final maintained scope was validated against:

- fast synthetic public tests
- JSON contract snapshots
- slow fixture-backed regression tests against a real local backup
- a Swift oracle used to compare Python output with the original library

## Privacy Warning

This project is intended for legitimate backup, recovery, export, and personal analysis workflows.

Accessing or processing WhatsApp conversations without the explicit consent of the people involved can violate privacy laws, workplace policies, and WhatsApp terms of service. Make sure you have the legal and ethical right to inspect the data before using this package.

## What The Package Exposes

This describes the legacy Python API. The current Swift API has diverged in
version 3.0.0 and now works from an extracted WhatsApp backup directory.

The main entry point is `WABackup`:

- discover iPhone backups with `getBackups()`
- inspect backups with encryption diagnostics via `inspectBackups()`
- connect to `ChatStorage.sqlite` with `connectChatStorageDb(...)`
- list chats with `getChats(...)`
- export a full chat with `getChat(...)`

Returned models mirror the Swift library:

- `BackupDiscoveryInfo`
- `BackupDiscoveryStatus`
- `ChatInfo`
- `MessageInfo`
- `MessageAuthor`
- `ContactInfo`
- `Reaction`
- `ChatDumpPayload`

## Requirements

- Python `3.11+`
- macOS access to an iPhone backup directory
- read access to the backup folder
- a backup that contains WhatsApp data

For chat listing and export, the selected backup must be non-encrypted. Use
`inspectBackups()` if you need an explicit readiness check before calling
`connectChatStorageDb(...)`.

By default, WhatsApp backups are usually found under:

```text
~/Library/Application Support/MobileSync/Backup/
```

On many systems you will need to grant Full Disk Access to the terminal app you use.

## Installation

### Editable Local Install

```bash
python3.11 -m pip install --user -e /path/to/PyWABackupAPI
```

If you install with `--user`, the generated script may end up under:

```text
~/Library/Python/3.11/bin
```

Add it to your shell `PATH` if needed:

```bash
export PATH="$HOME/Library/Python/3.11/bin:$PATH"
```

### Importing From Code

```python
from pywabackupapi import WABackup
```

## CLI Quick Start

These commands describe the frozen Python CLI. For the maintained workflow,
use SwiftWABackupAPI 3.0.0, where `list-chats` and `export-chat` read from an
extracted WhatsApp backup path.

List backups:

```bash
python3.11 -m pywabackupapi list-backups \
  --backup-path "$HOME/Library/Application Support/MobileSync/Backup" \
  --json --pretty
```

`list-backups` now reports diagnostic status for each candidate backup, including
whether it is ready, encrypted, or otherwise unusable. In JSON mode, the new
`backups` array exposes `status`, `isEncrypted`, `isReady`, and `issue`.

List chats for a backup:

```bash
python3.11 -m pywabackupapi list-chats \
  --backup-path "$HOME/Library/Application Support/MobileSync/Backup" \
  --backup-id "00008101-..." \
  --json --pretty
```

Export one chat as JSON only:

```bash
python3.11 -m pywabackupapi export-chat \
  --backup-path "$HOME/Library/Application Support/MobileSync/Backup" \
  --backup-id "00008101-..." \
  --chat-id 44 \
  --output-json /tmp/chat-44.json \
  --pretty
```

Export a full chat bundle into a directory:

```bash
python3.11 -m pywabackupapi export-chat \
  --backup-path "$HOME/Library/Application Support/MobileSync/Backup" \
  --backup-id "00008101-..." \
  --chat-id 44 \
  --output-dir /tmp/chat-44 \
  --pretty
```

`--output-dir` creates the directory if needed, writes `chat-<id>.json` inside it, and copies exported media into that same directory. `--output-json` writes only the JSON file.

The CLI now surfaces backup diagnostics directly in `list-backups`, and
`list-chats` / `export-chat` only operate on backups that are explicitly ready
for chat access.

If `--backup-id` is omitted, `list-chats` and `export-chat` now use the first
ready backup they find. Encrypted backups and backups with unknown encryption
status are rejected by those commands.

If the package is installed, the same commands are available as:

```bash
pywabackupapi list-backups
pywabackupapi list-chats --backup-id "00008101-..."
pywabackupapi export-chat --chat-id 44 --output-json /tmp/chat-44.json
```

## Python Usage

This is legacy usage for the frozen Python port. New development should target
SwiftWABackupAPI.

Recommended discovery flow:

```python
from pathlib import Path

from pywabackupapi import BackupDiscoveryStatus, WABackup

wa = WABackup()
inspections = wa.inspectBackups()
backup = next(
    inspection.backup
    for inspection in inspections
    if inspection.status == BackupDiscoveryStatus.READY
)

wa.connectChatStorageDb(backup)

chats = wa.getChats()
payload = wa.getChat(chatId=chats[0].id, directoryToSaveMedia=Path("/tmp/wa-export"))

print(payload.chatInfo.name)
print(len(payload.messages))
print(len(payload.contacts))
```

Compatibility flow retained for existing callers:

```python
from pywabackupapi import WABackup

wa = WABackup()
backups = wa.getBackups()
backup = backups.validBackups[0]

wa.connectChatStorageDb(backup)
```

`getBackups()` is retained as the legacy discovery API. It keeps the historical
`validBackups` / `invalidBackups` split. `inspectBackups()` is the recommended
entry point when you need encryption-aware discovery or per-backup diagnostics.

Each `BackupDiscoveryInfo` includes:

- `status` to distinguish `ready`, `encrypted`, and structural failure cases
- `isEncrypted` when `Manifest.plist` exposes `IsEncrypted`
- `isReady` as the high-level boolean gate for chat APIs
- `backup` when the candidate can still be represented as an `IPhoneBackup`

`BackupDiscoveryInfo.backup` is intentionally excluded from JSON serialization.
It is the in-memory value you can pass to `connectChatStorageDb(...)` after
checking `status == BackupDiscoveryStatus.READY`.

## JSON Contract

The canonical JSON payloads used by the Python port are verified in
`tests/test_json_contract.py`.

The tracked contract currently covers:

- `BackupDiscoveryInfo`
- `Reaction`
- `MessageAuthor`
- `ChatInfo`
- `MessageInfo`
- `ContactInfo`
- `ChatDumpPayload`

## Testing

The test suite is retained for historical validation of this frozen port. It is
not expected to track SwiftWABackupAPI 3.0.0 or later.

Short version:

```bash
PYTHONPATH=src python3.11 -m pytest
```

There are two test tiers:

- fast public tests that build synthetic backups at runtime
- slow fixture-backed tests that reference a real local backup and compare against the Swift implementation

The slow suite is optional and skips automatically if the local fixture is not available.

More details live in [docs/TESTING.md](./docs/TESTING.md).

## Repository Layout

```text
src/pywabackupapi/     Python package
tests/                 pytest suite
tests/swift_oracle/    Small Swift executable used as a comparison oracle
docs/                  Extra project documentation
```

## Relationship To The Swift Project

This repository used to track the behavior of the sibling Swift project:

- source reference: [SwiftWABackupAPI](https://github.com/domingogallardo/SwiftWABackupAPI)
- slow tests expect the real local fixture under `../SwiftWABackupAPI/Tests/Data`
- the Swift oracle in `tests/swift_oracle/` imports the Swift package by local path

That means the full slow compatibility suite is easiest to run when both repositories live side by side in the same parent directory.

As of SwiftWABackupAPI 3.0.0, this Python port no longer tracks new Swift
features, CLI changes, JSON contract changes, or behaviour changes. The Python
API remains at its previous direct-iPhone-backup workflow.

## Current Scope

The frozen port covers the previous public behaviour:

- backup discovery
- chat listing
- chat export payloads
- media copying
- structured author resolution
- reactions
- reply resolution
- optional `ContactsV2.sqlite` and `LID.sqlite` enrichment

## License

MIT. See [LICENSE](./LICENSE).
