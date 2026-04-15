from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import WABackup
from .models import BackupDiscoveryInfo, BackupFetchResult
from .utils import canonical_json_dumps, to_jsonable


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pywabackupapi",
        description="Explore WhatsApp data stored inside an iPhone backup.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_backups = subparsers.add_parser("list-backups", help="Discover backups under a root folder.")
    list_backups.add_argument(
        "--backup-path",
        default="~/Library/Application Support/MobileSync/Backup/",
        help="Root directory that contains iPhone backups.",
    )
    list_backups.add_argument("--json", action="store_true", help="Emit JSON instead of a text table.")
    list_backups.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    list_chats = subparsers.add_parser("list-chats", help="List chats for a backup.")
    _add_backup_resolution_args(list_chats)
    list_chats.add_argument(
        "--photos-dir",
        type=Path,
        default=None,
        help="Optional directory where chat photos will be copied.",
    )
    list_chats.add_argument("--json", action="store_true", help="Emit JSON instead of a text table.")
    list_chats.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    export_chat = subparsers.add_parser("export-chat", help="Export a chat as JSON or as a full directory bundle.")
    _add_backup_resolution_args(export_chat)
    export_chat.add_argument("--chat-id", type=int, required=True, help="Chat id to export.")
    output_group = export_chat.add_mutually_exclusive_group()
    output_group.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional JSON output path. Exports only the JSON payload.",
    )
    output_group.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional export directory. Writes chat-<id>.json and copies media there.",
    )
    export_chat.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def _add_backup_resolution_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--backup-path",
        default="~/Library/Application Support/MobileSync/Backup/",
        help="Root directory that contains iPhone backups.",
    )
    parser.add_argument(
        "--backup-id",
        default=None,
        help="Backup identifier. When omitted, the first valid backup is used.",
    )


def _serialize_backup_fetch_result(
    result: BackupFetchResult,
    inspections: list[BackupDiscoveryInfo],
) -> dict[str, Any]:
    return {
        "backups": inspections,
        "validBackups": [
            {
                "identifier": backup.identifier,
                "path": backup.path,
                "creationDate": backup.creationDate,
                "isEncrypted": backup.isEncrypted,
            }
            for backup in result.validBackups
        ],
        "invalidBackups": [str(path) for path in result.invalidBackups],
    }


def _render_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return canonical_json_dumps(payload)
    return json.dumps(to_jsonable(payload), ensure_ascii=False, sort_keys=True)


def _resolve_connected_backup(args: argparse.Namespace) -> tuple[WABackup, Any]:
    wa_backup = WABackup(backupPath=args.backup_path)
    inspections = wa_backup.inspectBackups()

    if args.backup_id is None:
        inspection = next((item for item in inspections if item.isReady), None)
        if inspection is None:
            raise SystemExit("No ready backups found. Run 'list-backups' to inspect encryption status and backup diagnostics.")
    else:
        inspection = next((item for item in inspections if item.identifier == args.backup_id), None)
        if inspection is None:
            raise SystemExit(f"Backup '{args.backup_id}' not found.")

    backup = inspection.backup
    if not inspection.isReady or backup is None:
        issue = inspection.issue or f"Backup status is {inspection.status.value}."
        raise SystemExit(f"Backup '{inspection.identifier}' is not ready for chat access: {issue}")

    wa_backup.connectChatStorageDb(backup)
    return wa_backup, backup


def _print_or_write(text: str, output: Path | None = None) -> None:
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")
    else:
        print(text)


def _handle_list_backups(args: argparse.Namespace) -> int:
    wa_backup = WABackup(backupPath=args.backup_path)
    inspections = wa_backup.inspectBackups()
    result = wa_backup.getBackups()

    if args.json:
        print(_render_json(_serialize_backup_fetch_result(result, inspections), args.pretty))
        return 0

    if not inspections:
        print("No backups found.")
        return 0

    for inspection in inspections:
        print(_format_backup_inspection_line(inspection))
    return 0


def _format_backup_inspection_line(inspection: BackupDiscoveryInfo) -> str:
    creation_date = inspection.creationDate.isoformat() if inspection.creationDate is not None else "-"
    if inspection.isEncrypted is True:
        encryption_state = "ENCRYPTED"
    elif inspection.isEncrypted is False:
        encryption_state = "NOT_ENCRYPTED"
    else:
        encryption_state = "UNKNOWN"

    columns = [
        _backup_status_label(inspection),
        inspection.identifier,
        creation_date,
        encryption_state,
        inspection.path,
    ]
    if inspection.issue is not None:
        columns.append(inspection.issue)
    return "\t".join(columns)


def _backup_status_label(inspection: BackupDiscoveryInfo) -> str:
    labels = {
        "ready": "READY",
        "encrypted": "ENCRYPTED",
        "encryptionStatusUnavailable": "UNKNOWN_ENCRYPTION",
        "missingRequiredFile": "INVALID_MISSING_FILE",
        "malformedStatusPlist": "INVALID_STATUS_PLIST",
        "missingWhatsAppDatabase": "NO_WHATSAPP_DATABASE",
        "unreadableManifestDatabase": "UNREADABLE_MANIFEST_DB",
        "unreadableBackup": "UNREADABLE_BACKUP",
    }
    return labels.get(inspection.status.value, inspection.status.value.upper())


def _handle_list_chats(args: argparse.Namespace) -> int:
    wa_backup, backup = _resolve_connected_backup(args)
    chats = wa_backup.getChats(directoryToSavePhotos=args.photos_dir)

    if args.json:
        print(_render_json(chats, args.pretty))
        return 0

    print(f"Backup: {backup.identifier}")
    for chat in chats:
        print(
            f"{chat.id}\t{chat.chatType.value}\t{chat.numberMessages}\t"
            f"{chat.lastMessageDate.isoformat()}\t{chat.name}"
        )
    return 0


def _handle_export_chat(args: argparse.Namespace) -> int:
    export_directory = None
    if args.output_dir is not None:
        export_directory = _resolve_output_dir(args.output_dir)

    wa_backup, backup = _resolve_connected_backup(args)
    payload = wa_backup.getChat(chatId=args.chat_id, directoryToSaveMedia=export_directory)
    rendered = _render_json(payload, args.pretty)

    if export_directory is not None:
        output_json = export_directory / f"chat-{args.chat_id}.json"
        _print_or_write(rendered, output_json)
        print(f"Wrote chat {args.chat_id} from backup {backup.identifier} to {output_json}")
    elif args.output_json is None:
        print(rendered)
    else:
        output_json = _resolve_output_json_path(args.output_json)
        _print_or_write(rendered, output_json)
        print(f"Wrote chat {args.chat_id} from backup {backup.identifier} to {output_json}")
    return 0


def _resolve_output_json_path(path: Path) -> Path:
    if path.exists() and path.is_dir():
        raise ValueError(f"--output-json expects a file path, but '{path}' is a directory.")
    return path


def _resolve_output_dir(path: Path) -> Path:
    if path.exists() and not path.is_dir():
        raise ValueError(f"--output-dir expects a directory path, but '{path}' is a file.")
    path.mkdir(parents=True, exist_ok=True)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "list-backups":
            return _handle_list_backups(args)
        if args.command == "list-chats":
            return _handle_list_chats(args)
        if args.command == "export-chat":
            return _handle_export_chat(args)
    except KeyboardInterrupt:
        parser.exit(130, "Interrupted.\n")
    except Exception as error:
        parser.exit(1, f"{error}\n")

    parser.exit(2, "Unknown command.\n")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
