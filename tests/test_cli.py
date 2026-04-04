from __future__ import annotations

import json
import os
import subprocess
import sys

from .support import make_sample_backup, make_temporary_directory, remove_item_if_exists


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{os.environ.get('PYTHONPATH', '')}:{os.path.abspath('src')}".strip(":")
    return subprocess.run(
        [sys.executable, "-m", "pywabackupapi", *args],
        cwd=os.path.abspath("."),
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )


def test_cli_list_backups_json() -> None:
    fixture = make_sample_backup()
    try:
        completed = run_cli("list-backups", "--backup-path", str(fixture.rootURL), "--json")
        payload = json.loads(completed.stdout)

        assert len(payload["validBackups"]) == 1
        assert payload["validBackups"][0]["identifier"] == fixture.backup.identifier
        assert payload["invalidBackups"] == []
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


def test_cli_export_chat_to_file_and_media_dir() -> None:
    fixture = make_sample_backup()
    output_dir = make_temporary_directory("PyWABackupAPI-cli-output")
    try:
        output_file = output_dir / "chat-44.json"
        media_dir = output_dir / "media"
        media_dir.mkdir(parents=True, exist_ok=True)

        completed = run_cli(
            "export-chat",
            "--backup-path",
            str(fixture.rootURL),
            "--chat-id",
            "44",
            "--media-dir",
            str(media_dir),
            "--output",
            str(output_file),
            "--pretty",
        )

        assert "Wrote chat 44" in completed.stdout
        payload = json.loads(output_file.read_text(encoding="utf-8"))
        assert payload["chatInfo"]["id"] == 44
        assert (media_dir / "fea35851-6a2c-45a3-a784-003d25576b45.pdf").exists()
    finally:
        remove_item_if_exists(output_dir)
        remove_item_if_exists(fixture.rootURL)
