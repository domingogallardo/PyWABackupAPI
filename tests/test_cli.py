from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from .support import make_sample_backup, make_temporary_backup, make_temporary_directory, remove_item_if_exists


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return run_cli_checked(*args, check=True)


def run_cli_checked(*args: str, check: bool) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{os.environ.get('PYTHONPATH', '')}:{os.path.abspath('src')}".strip(":")
    return subprocess.run(
        [sys.executable, "-m", "pywabackupapi", *args],
        cwd=os.path.abspath("."),
        env=env,
        check=check,
        text=True,
        capture_output=True,
    )


def test_cli_list_backups_json() -> None:
    fixture = make_sample_backup()
    try:
        completed = run_cli("list-backups", "--backup-path", str(fixture.rootURL), "--json")
        payload = json.loads(completed.stdout)

        assert len(payload["backups"]) == 1
        assert payload["backups"][0]["status"] == "ready"
        assert payload["backups"][0]["isEncrypted"] is False
        assert len(payload["validBackups"]) == 1
        assert payload["validBackups"][0]["identifier"] == fixture.backup.identifier
        assert payload["validBackups"][0]["isEncrypted"] is False
        assert payload["invalidBackups"] == []
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_cli_list_backups_json_reports_encrypted_backup() -> None:
    fixture = make_temporary_backup(name="encrypted-backup", is_encrypted=True, chat_storage_setup=lambda connection: None)
    try:
        completed = run_cli("list-backups", "--backup-path", str(fixture.rootURL), "--json")
        payload = json.loads(completed.stdout)

        assert len(payload["backups"]) == 1
        assert payload["backups"][0]["status"] == "encrypted"
        assert payload["backups"][0]["isEncrypted"] is True
        assert len(payload["validBackups"]) == 1
        assert payload["validBackups"][0]["isEncrypted"] is True
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_cli_list_chats_json() -> None:
    fixture = make_sample_backup()
    try:
        completed = run_cli("list-chats", "--backup-path", str(fixture.rootURL), "--json")
        payload = json.loads(completed.stdout)

        assert [item["id"] for item in payload] == [44, 593]
        assert payload[0]["name"] == "Alias Atlas"
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_cli_list_chats_rejects_encrypted_backup_by_id() -> None:
    fixture = make_temporary_backup(name="encrypted-backup", is_encrypted=True, chat_storage_setup=lambda connection: None)
    try:
        completed = run_cli_checked(
            "list-chats",
            "--backup-path",
            str(fixture.rootURL),
            "--backup-id",
            fixture.backup.identifier,
            "--json",
            check=False,
        )

        assert completed.returncode == 1
        assert "is not ready for chat access" in completed.stderr
        assert "Backup is encrypted." in completed.stderr
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_cli_export_chat_to_stdout() -> None:
    fixture = make_sample_backup()
    try:
        completed = run_cli("export-chat", "--backup-path", str(fixture.rootURL), "--chat-id", "44")
        payload = json.loads(completed.stdout)

        assert payload["chatInfo"]["id"] == 44
        assert len(payload["messages"]) == 3
        assert len(payload["contacts"]) == 2
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_cli_export_chat_to_output_json_only() -> None:
    fixture = make_sample_backup()
    output_dir = make_temporary_directory("PyWABackupAPI-cli-output")
    try:
        output_file = output_dir / "chat-44.json"

        completed = run_cli(
            "export-chat",
            "--backup-path",
            str(fixture.rootURL),
            "--chat-id",
            "44",
            "--output-json",
            str(output_file),
            "--pretty",
        )

        assert "Wrote chat 44" in completed.stdout
        payload = json.loads(output_file.read_text(encoding="utf-8"))
        assert payload["chatInfo"]["id"] == 44
        assert not (output_dir / "fea35851-6a2c-45a3-a784-003d25576b45.pdf").exists()
    finally:
        remove_item_if_exists(output_dir)
        remove_item_if_exists(fixture.rootURL)


def test_cli_export_chat_to_output_dir_bundle() -> None:
    fixture = make_sample_backup()
    output_root = make_temporary_directory("PyWABackupAPI-cli-output-dir")
    try:
        output_dir = output_root / "chat-export"

        completed = run_cli(
            "export-chat",
            "--backup-path",
            str(fixture.rootURL),
            "--chat-id",
            "44",
            "--output-dir",
            str(output_dir),
            "--pretty",
        )

        output_file = output_dir / "chat-44.json"
        assert "Wrote chat 44" in completed.stdout
        payload = json.loads(output_file.read_text(encoding="utf-8"))
        assert payload["chatInfo"]["id"] == 44
        assert (output_dir / "fea35851-6a2c-45a3-a784-003d25576b45.pdf").exists()
    finally:
        remove_item_if_exists(output_root)
        remove_item_if_exists(fixture.rootURL)


def test_cli_export_chat_rejects_both_output_modes() -> None:
    output_dir = Path("/tmp/chat-export")
    try:
        completed = run_cli_checked(
            "export-chat",
            "--chat-id",
            "44",
            "--output-json",
            "/tmp/chat.json",
            "--output-dir",
            str(output_dir),
            check=False,
        )

        assert completed.returncode == 2
        assert "not allowed with argument" in completed.stderr
    finally:
        remove_item_if_exists(output_dir)
