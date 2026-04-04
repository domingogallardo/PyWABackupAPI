from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import WABackup
from .models import BackupFetchResult
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

    export_chat = subparsers.add_parser("export-chat", help="Export a full chat payload as JSON.")
    _add_backup_resolution_args(export_chat)
    export_chat.add_argument("--chat-id", type=int, required=True, help="Chat id to export.")
    export_chat.add_argument(
        "--media-dir",
        type=Path,
        default=None,
        help="Optional directory where chat media and contact photos will be copied.",
    )
    export_chat.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path. Defaults to stdout.",
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


def _serialize_backup_fetch_result(result: BackupFetchResult) -> dict[str, Any]:
    return {
        "validBackups": [
            {
                "identifier": backup.identifier,
                "path": backup.path,
                "creationDate": backup.creationDate,
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
    backups = wa_backup.getBackups()

    if args.backup_id is None:
        if not backups.validBackups:
            raise SystemExit("No valid backups found.")
        backup = backups.validBackups[0]
    else:
        backup = next((item for item in backups.validBackups if item.identifier == args.backup_id), None)
        if backup is None:
            raise SystemExit(f"Backup '{args.backup_id}' not found.")

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
    result = wa_backup.getBackups()

    if args.json:
        print(_render_json(_serialize_backup_fetch_result(result), args.pretty))
        return 0

    if not result.validBackups and not result.invalidBackups:
        print("No backups found.")
        return 0

    for backup in result.validBackups:
        print(f"VALID\t{backup.identifier}\t{backup.creationDate.isoformat()}\t{backup.path}")
    for invalid_path in result.invalidBackups:
        print(f"INVALID\t{invalid_path}")
    return 0


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
    wa_backup, backup = _resolve_connected_backup(args)
    payload = wa_backup.getChat(chatId=args.chat_id, directoryToSaveMedia=args.media_dir)
    rendered = _render_json(payload, args.pretty)

    if args.output is None:
        print(rendered)
    else:
        _print_or_write(rendered, args.output)
        print(f"Wrote chat {args.chat_id} from backup {backup.identifier} to {args.output}")
    return 0


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
