from __future__ import annotations

import json
import math
import re
import sqlite3
import unicodedata
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any


APPLE_REFERENCE_DATE = datetime(2001, 1, 1, tzinfo=UTC)
WHATSAPP_IGNORED_DISPLAY_CHARS = {
    "\u200E",
    "\u200F",
    "\u202A",
    "\u202B",
    "\u202C",
    "\u202D",
    "\u202E",
}


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def apple_reference_date_to_datetime(seconds: float | int | None) -> datetime:
    if seconds is None:
        return APPLE_REFERENCE_DATE
    return APPLE_REFERENCE_DATE + timedelta(seconds=float(seconds))


def datetime_to_apple_reference_date(value: datetime) -> float:
    return (ensure_utc(value) - APPLE_REFERENCE_DATE).total_seconds()


def iso8601_string(value: datetime) -> str:
    return ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_whatsapp_display_text(value: str) -> str:
    cleaned = "".join(ch for ch in value if ch not in WHATSAPP_IGNORED_DISPLAY_CHARS)
    collapsed = re.sub(r"\s+", " ", cleaned)
    return collapsed.strip()


def jid_user(value: str) -> str:
    return value.split("@", 1)[0]


def jid_domain(value: str) -> str:
    if "@" not in value:
        return ""
    return value.split("@", 1)[1].lower()


def is_group_jid(value: str) -> bool:
    return jid_domain(value) == "g.us"


def is_individual_jid(value: str) -> bool:
    return jid_domain(value) == "s.whatsapp.net"


def is_lid_jid(value: str) -> bool:
    return jid_domain(value) == "lid"


def extracted_phone(value: str) -> str:
    return jid_user(value)


def question_marks(count: int) -> str:
    if count <= 0:
        return ""
    return ", ".join("?" for _ in range(count))


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def check_table_schema(
    connection: sqlite3.Connection,
    table_name: str,
    expected_columns: set[str],
) -> None:
    if not table_exists(connection, table_name):
        raise ValueError(f"Table {table_name} does not exist")

    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    column_names = {str(row["name"]).upper() for row in rows}
    if not expected_columns.issubset(column_names):
        raise ValueError(f"Table {table_name} does not have all expected fields")


def row_value(row: sqlite3.Row, column: str, default: Any = None) -> Any:
    if column not in row.keys():
        return default
    value = row[column]
    return default if value is None else value


def row_datetime(
    row: sqlite3.Row,
    column: str,
    default: datetime | None = None,
) -> datetime:
    raw_value = row_value(row, column, None)
    if raw_value is None:
        return default or APPLE_REFERENCE_DATE
    if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
        if math.isnan(float(raw_value)):
            return default or APPLE_REFERENCE_DATE
        return apple_reference_date_to_datetime(raw_value)
    return default or APPLE_REFERENCE_DATE


def normalized_author_field(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = normalize_whatsapp_display_text(value)
    return normalized or None


def is_reaction_sender_jid(value: str) -> bool:
    return value.endswith("@s.whatsapp.net") or value.endswith("@lid")


def is_single_emoji(value: str) -> bool:
    if not value or value.isspace():
        return False

    filtered = []
    for char in value:
        codepoint = ord(char)
        if char in {"\u200d", "\ufe0f"}:
            continue
        if 0x1F3FB <= codepoint <= 0x1F3FF:
            continue
        filtered.append(char)

    if not filtered:
        return False

    for char in filtered:
        category = unicodedata.category(char)
        codepoint = ord(char)
        if category in {"So", "Sk"}:
            continue
        if 0x1F1E6 <= codepoint <= 0x1F1FF:
            continue
        return False

    return True


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return iso8601_string(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        result: dict[str, Any] = {}
        for field in fields(value):
            json_name = field.metadata.get("json_name", field.name)
            serialized = _jsonable(getattr(value, field.name))
            if serialized is not None:
                result[json_name] = serialized
        return result
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            serialized = _jsonable(item)
            if serialized is not None:
                result[key] = serialized
        return result
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def to_jsonable(value: Any) -> Any:
    return _jsonable(value)


def canonical_json_dumps(value: Any) -> str:
    payload = to_jsonable(value)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    return rendered.replace('": ', '" : ')
