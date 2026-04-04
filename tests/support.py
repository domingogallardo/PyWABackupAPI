from __future__ import annotations

import json
import os
import plistlib
import shutil
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import pytest

from pywabackupapi import (
    ChatDumpPayload,
    ChatInfo,
    ContactInfo,
    IPhoneBackup,
    MessageAuthor,
    MessageAuthorKind,
    MessageAuthorSource,
    Reaction,
    WABackup,
    canonical_json_dumps,
)
from pywabackupapi.api import MediaItem
from pywabackupapi.utils import datetime_to_apple_reference_date


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROGRAMACION_ROOT = PROJECT_ROOT.parent
SWIFT_REPO_ROOT = PROGRAMACION_ROOT / "SwiftWABackupAPI"
DATA_ROOT = SWIFT_REPO_ROOT / "Tests" / "Data"
FULL_FIXTURE_IDENTIFIER = "00008101-000478893600801E"
FULL_FIXTURE_ROOT = DATA_ROOT / FULL_FIXTURE_IDENTIFIER
SWIFT_ORACLE_ROOT = PROJECT_ROOT / "tests" / "swift_oracle"


@dataclass
class StoredFile:
    relativePath: str
    fileHash: str
    contents: bytes


@dataclass
class TemporaryBackupFixture:
    rootURL: Path
    backupURL: Path
    backup: IPhoneBackup


class MediaWriteDelegateSpy:
    def __init__(self) -> None:
        self.fileNames: list[str] = []

    def didWriteMediaFile(self, fileName: str) -> None:
        self.fileNames.append(fileName)


def make_temporary_directory(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"{prefix}-"))


def remove_item_if_exists(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def reference_date_timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> float:
    value = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    return datetime_to_apple_reference_date(value)


def write_plist(obj: object, path: Path) -> None:
    with path.open("wb") as handle:
        plistlib.dump(obj, handle)


def create_manifest_database(path: Path, chat_storage_hash: str) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE Files (
                fileID TEXT,
                relativePath TEXT,
                domain TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO Files (fileID, relativePath, domain)
            VALUES (?, ?, ?)
            """,
            (
                chat_storage_hash,
                "ChatStorage.sqlite",
                "AppDomainGroup-group.net.whatsapp.WhatsApp.shared",
            ),
        )
        connection.commit()
    finally:
        connection.close()


def add_manifest_entry(stored_file: StoredFile, backup_url: Path) -> None:
    connection = sqlite3.connect(backup_url / "Manifest.db")
    try:
        connection.execute(
            """
            INSERT INTO Files (fileID, relativePath, domain)
            VALUES (?, ?, ?)
            """,
            (
                stored_file.fileHash,
                stored_file.relativePath,
                "AppDomainGroup-group.net.whatsapp.WhatsApp.shared",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    hash_directory = backup_url / stored_file.fileHash[:2]
    hash_directory.mkdir(parents=True, exist_ok=True)
    (hash_directory / stored_file.fileHash).write_bytes(stored_file.contents)


def make_temporary_backup(
    name: str = "temp-backup",
    additional_manifest_entries: list[StoredFile] | None = None,
    chat_storage_setup: Callable[[sqlite3.Connection], None] | None = None,
) -> TemporaryBackupFixture:
    root_url = make_temporary_directory("PyWABackupAPI-tests")
    backup_url = root_url / name
    backup_url.mkdir(parents=True, exist_ok=True)

    creation_date = datetime.fromtimestamp(1_711_267_200, tz=UTC)
    write_plist({}, backup_url / "Info.plist")
    write_plist({"Date": creation_date}, backup_url / "Status.plist")

    chat_storage_hash = "ab1234567890chatstorage"
    create_manifest_database(backup_url / "Manifest.db", chat_storage_hash)

    for stored_file in additional_manifest_entries or []:
        add_manifest_entry(stored_file, backup_url)

    hash_directory = backup_url / chat_storage_hash[:2]
    hash_directory.mkdir(parents=True, exist_ok=True)
    chat_storage_url = hash_directory / chat_storage_hash

    connection = sqlite3.connect(chat_storage_url)
    try:
        if chat_storage_setup is not None:
            chat_storage_setup(connection)
        connection.commit()
    finally:
        connection.close()

    backup = IPhoneBackup(url=backup_url, creationDate=creation_date)
    return TemporaryBackupFixture(rootURL=root_url, backupURL=backup_url, backup=backup)


def add_contacts_database(
    fixture: TemporaryBackupFixture,
    file_hash: str = "b81234567890contactsv2",
    setup: Callable[[sqlite3.Connection], None] | None = None,
) -> None:
    connection = sqlite3.connect(fixture.backupURL / "Manifest.db")
    try:
        connection.execute(
            """
            INSERT INTO Files (fileID, relativePath, domain)
            VALUES (?, ?, ?)
            """,
            (
                file_hash,
                "ContactsV2.sqlite",
                "AppDomainGroup-group.net.whatsapp.WhatsApp.shared",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    hash_directory = fixture.backupURL / file_hash[:2]
    hash_directory.mkdir(parents=True, exist_ok=True)
    contacts_url = hash_directory / file_hash
    connection = sqlite3.connect(contacts_url)
    try:
        connection.execute(
            """
            CREATE TABLE ZWAADDRESSBOOKCONTACT (
                Z_PK INTEGER PRIMARY KEY,
                ZFULLNAME TEXT,
                ZGIVENNAME TEXT,
                ZBUSINESSNAME TEXT,
                ZLID TEXT,
                ZPHONENUMBER TEXT,
                ZWHATSAPPID TEXT
            )
            """
        )
        if setup is not None:
            setup(connection)
        connection.commit()
    finally:
        connection.close()


def add_lid_database(
    fixture: TemporaryBackupFixture,
    file_hash: str = "e71234567890lidsqlite",
    setup: Callable[[sqlite3.Connection], None] | None = None,
) -> None:
    connection = sqlite3.connect(fixture.backupURL / "Manifest.db")
    try:
        connection.execute(
            """
            INSERT INTO Files (fileID, relativePath, domain)
            VALUES (?, ?, ?)
            """,
            (
                file_hash,
                "LID.sqlite",
                "AppDomainGroup-group.net.whatsapp.WhatsApp.shared",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    hash_directory = fixture.backupURL / file_hash[:2]
    hash_directory.mkdir(parents=True, exist_ok=True)
    lid_url = hash_directory / file_hash
    connection = sqlite3.connect(lid_url)
    try:
        connection.execute(
            """
            CREATE TABLE ZWAZACCOUNT (
                Z_PK INTEGER PRIMARY KEY,
                ZIDENTIFIER TEXT,
                ZPHONENUMBER TEXT,
                ZCREATEDAT DOUBLE
            )
            """
        )
        if setup is not None:
            setup(connection)
        connection.commit()
    finally:
        connection.close()


def create_common_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE ZWACHATSESSION (
            Z_PK INTEGER PRIMARY KEY,
            ZCONTACTJID TEXT,
            ZPARTNERNAME TEXT,
            ZLASTMESSAGEDATE DOUBLE,
            ZMESSAGECOUNTER INTEGER,
            ZSESSIONTYPE INTEGER,
            ZARCHIVED INTEGER
        );

        CREATE TABLE ZWAMESSAGE (
            Z_PK INTEGER PRIMARY KEY,
            ZTOJID TEXT,
            ZMESSAGETYPE INTEGER,
            ZGROUPMEMBER INTEGER,
            ZCHATSESSION INTEGER,
            ZTEXT TEXT,
            ZMESSAGEDATE DOUBLE,
            ZFROMJID TEXT,
            ZMEDIAITEM INTEGER,
            ZISFROMME INTEGER,
            ZGROUPEVENTTYPE INTEGER,
            ZSTANZAID TEXT,
            ZPARENTMESSAGE INTEGER
        );

        CREATE TABLE ZWAGROUPMEMBER (
            Z_PK INTEGER PRIMARY KEY,
            ZMEMBERJID TEXT,
            ZCONTACTNAME TEXT
        );

        CREATE TABLE ZWAPROFILEPUSHNAME (
            ZPUSHNAME TEXT,
            ZJID TEXT
        );

        CREATE TABLE ZWAMEDIAITEM (
            Z_PK INTEGER PRIMARY KEY,
            ZMETADATA BLOB,
            ZTITLE TEXT,
            ZMEDIALOCALPATH TEXT,
            ZMOVIEDURATION INTEGER,
            ZLATITUDE DOUBLE,
            ZLONGITUDE DOUBLE
        );

        CREATE TABLE ZWAMESSAGEINFO (
            Z_PK INTEGER PRIMARY KEY,
            ZRECEIPTINFO BLOB,
            ZMESSAGE INTEGER
        );
        """
    )


def sample_reply_metadata(replying_to: str, quoted_jid: str) -> bytes:
    return bytes([0x2A, len(replying_to.encode("utf-8"))]) + replying_to.encode("utf-8") + bytes(
        [0x32, len(quoted_jid.encode("utf-8"))]
    ) + quoted_jid.encode("utf-8")


def sample_reaction_receipt_info(emoji: str, sender_phone: str | None = None, sender_jid: str | None = None) -> bytes:
    jid = sender_jid or f"{sender_phone}@s.whatsapp.net"
    stanza_id = b"3A038549B0680F155E6F"
    sender = jid.encode("utf-8")
    emoji_bytes = emoji.encode("utf-8")
    reaction_entry = (
        bytes([0x0A, len(stanza_id)])
        + stanza_id
        + bytes([0x12, len(sender)])
        + sender
        + bytes([0x1A, len(emoji_bytes)])
        + emoji_bytes
        + bytes([0x20, 0x01, 0x28, 0x02, 0x38, 0x00])
    )
    return bytes([0x3A, len(reaction_entry) + 2, 0x0A, len(reaction_entry)]) + reaction_entry


def make_sample_backup() -> TemporaryBackupFixture:
    document_path = "Media/Document/fea35851-6a2c-45a3-a784-003d25576b45.pdf"

    def setup(connection: sqlite3.Connection) -> None:
        create_common_tables(connection)

        chat44_latest = reference_date_timestamp(2024, 4, 3, 11, 24, 16)
        chat593_latest = reference_date_timestamp(2024, 4, 2, 9, 0, 0)

        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (44, "08185296386@s.whatsapp.net", "Alias Atlas", chat44_latest, 3, 0, 0),
        )
        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (593, "08185296375@s.whatsapp.net", "Business Contact", chat593_latest, 1, 0, 0),
        )

        connection.execute(
            """
            INSERT INTO ZWAMEDIAITEM
            (Z_PK, ZMETADATA, ZTITLE, ZMEDIALOCALPATH, ZMOVIEDURATION, ZLATITUDE, ZLONGITUDE)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                9001,
                sample_reply_metadata("orig-1", "08185296386@s.whatsapp.net"),
                None,
                None,
                None,
                None,
                None,
            ),
        )
        connection.execute(
            """
            INSERT INTO ZWAMEDIAITEM
            (Z_PK, ZMETADATA, ZTITLE, ZMEDIALOCALPATH, ZMOVIEDURATION, ZLATITUDE, ZLONGITUDE)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (9002, None, None, document_path, None, None, None),
        )

        messages = [
            (
                125470,
                "08185296380@s.whatsapp.net",
                6,
                None,
                44,
                None,
                reference_date_timestamp(2024, 4, 3, 10, 0, 0),
                None,
                None,
                1,
                None,
                "owner-marker-1",
                None,
            ),
            (
                125479,
                "08185296386@s.whatsapp.net",
                0,
                None,
                44,
                "Original message",
                reference_date_timestamp(2024, 4, 3, 11, 0, 0),
                None,
                None,
                1,
                None,
                "orig-1",
                None,
            ),
            (
                125482,
                "08185296380@s.whatsapp.net",
                0,
                None,
                44,
                "Vale, cuando pase por la zona te escribo.",
                chat44_latest,
                "08185296386@s.whatsapp.net",
                9001,
                0,
                None,
                "reply-1",
                None,
            ),
            (
                126279,
                "08185296380@s.whatsapp.net",
                8,
                None,
                44,
                "ARCHIVO RESUMEN CASO DELTA.pdf",
                reference_date_timestamp(2024, 4, 3, 10, 30, 0),
                "08185296386@s.whatsapp.net",
                9002,
                0,
                None,
                "doc-1",
                None,
            ),
            (
                200002,
                "08185296380@s.whatsapp.net",
                0,
                None,
                593,
                "Hello from business",
                chat593_latest,
                "08185296375@s.whatsapp.net",
                None,
                0,
                None,
                "business-text-1",
                None,
            ),
        ]
        connection.executemany(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID, ZPARENTMESSAGE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            messages,
        )

        connection.execute(
            """
            INSERT INTO ZWAMESSAGEINFO
            (Z_PK, ZRECEIPTINFO, ZMESSAGE)
            VALUES (?, ?, ?)
            """,
            (1, sample_reaction_receipt_info("😢", sender_phone="08185296386"), 125482),
        )

    return make_temporary_backup(
        name="sample-backup",
        additional_manifest_entries=[
            StoredFile(
                relativePath=document_path,
                fileHash="cd1234567890sampledocument",
                contents=b"Sample PDF contents",
            )
        ],
        chat_storage_setup=setup,
    )


def make_connected_sample_backup() -> tuple[WABackup, TemporaryBackupFixture]:
    fixture = make_sample_backup()
    wa_backup = WABackup(backupPath=str(fixture.rootURL))
    wa_backup.connectChatStorageDb(fixture.backup)
    return wa_backup, fixture


def make_connected_filtered_chat_backup() -> tuple[WABackup, TemporaryBackupFixture]:
    def setup(connection: sqlite3.Connection) -> None:
        create_common_tables(connection)
        latest = reference_date_timestamp(2024, 4, 9, 12, 0, 0)

        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (800, "08185296377@s.whatsapp.net", "Visible Chat", latest, 1, 0, 0),
        )
        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (801, "newsletter-1@newsletter.whatsapp.net", "Filtered Channel", latest, 1, 5, 0),
        )

        connection.execute(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                800001,
                "08185296380@s.whatsapp.net",
                6,
                None,
                800,
                None,
                latest,
                None,
                None,
                1,
                None,
                "owner-visible-1",
            ),
        )
        connection.execute(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                800002,
                "08185296380@s.whatsapp.net",
                0,
                None,
                800,
                "Visible text message",
                latest,
                "08185296377@s.whatsapp.net",
                None,
                0,
                None,
                "visible-text-1",
            ),
        )

    fixture = make_temporary_backup(name="filtered-chat-backup", chat_storage_setup=setup)
    wa_backup = WABackup(backupPath=str(fixture.rootURL))
    wa_backup.connectChatStorageDb(fixture.backup)
    return wa_backup, fixture


def make_connected_profile_photo_backup() -> tuple[WABackup, TemporaryBackupFixture]:
    def setup(connection: sqlite3.Connection) -> None:
        create_common_tables(connection)
        latest = reference_date_timestamp(2024, 4, 9, 12, 0, 0)

        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (810, "08185296384@s.whatsapp.net", "Photo Contact", latest, 1, 0, 0),
        )
        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (811, "08185296380@s.whatsapp.net", "Me", latest, 1, 0, 0),
        )

        connection.execute(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (811001, "08185296380@s.whatsapp.net", 6, None, 811, None, latest, None, None, 1, None, "owner-photo-1"),
        )
        connection.execute(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                810001,
                "08185296380@s.whatsapp.net",
                0,
                None,
                810,
                "Chat with exported profile photo",
                latest,
                "08185296384@s.whatsapp.net",
                None,
                0,
                None,
                "photo-text-1",
            ),
        )

    fixture = make_temporary_backup(
        name="profile-photo-backup",
        additional_manifest_entries=[
            StoredFile(
                relativePath="Media/Profile/08185296384-1712664000.jpg",
                fileHash="ef1234567890profilephoto",
                contents=b"Fake JPEG contents",
            )
        ],
        chat_storage_setup=setup,
    )
    wa_backup = WABackup(backupPath=str(fixture.rootURL))
    wa_backup.connectChatStorageDb(fixture.backup)
    return wa_backup, fixture


def make_connected_individual_lid_backup() -> tuple[WABackup, TemporaryBackupFixture]:
    def setup(connection: sqlite3.Connection) -> None:
        create_common_tables(connection)
        latest = reference_date_timestamp(2024, 4, 10, 18, 0, 0)

        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (820, "40482648260486@lid", "Alias Birch", latest, 2, 0, 0),
        )
        connection.execute(
            "INSERT INTO ZWAPROFILEPUSHNAME (ZPUSHNAME, ZJID) VALUES (?, ?)",
            ("Alias Birch", "40482648260486@lid"),
        )

        messages = [
            (
                820000,
                "08185296380@s.whatsapp.net",
                6,
                None,
                820,
                None,
                reference_date_timestamp(2024, 4, 10, 17, 59, 0),
                None,
                None,
                1,
                None,
                "individual-lid-owner",
            ),
            (
                820001,
                "08185296380@s.whatsapp.net",
                0,
                None,
                820,
                "Incoming from lid-based individual chat",
                latest,
                "40482648260486@lid",
                None,
                0,
                None,
                "individual-lid-1",
            ),
            (
                820002,
                "08185296380@s.whatsapp.net",
                0,
                None,
                820,
                "Outgoing",
                reference_date_timestamp(2024, 4, 10, 18, 1, 0),
                None,
                None,
                1,
                None,
                "individual-lid-2",
            ),
        ]
        connection.executemany(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            messages,
        )

    fixture = make_temporary_backup(name="individual-lid-backup", chat_storage_setup=setup)

    add_lid_database(
        fixture,
        setup=lambda connection: connection.execute(
            """
            INSERT INTO ZWAZACCOUNT
            (Z_PK, ZIDENTIFIER, ZPHONENUMBER, ZCREATEDAT)
            VALUES (?, ?, ?, ?)
            """,
            (1, "40482648260486@lid", "08185296385", reference_date_timestamp(2025, 2, 10, 12, 0, 0)),
        ),
    )

    wa_backup = WABackup(backupPath=str(fixture.rootURL))
    wa_backup.connectChatStorageDb(fixture.backup)
    return wa_backup, fixture


def make_connected_group_backup() -> tuple[WABackup, TemporaryBackupFixture]:
    def setup(connection: sqlite3.Connection) -> None:
        create_common_tables(connection)
        group_latest = reference_date_timestamp(2024, 4, 8, 11, 0, 0)

        chats = [
            (700, "08185296380-123456@g.us", "Invariant Group", group_latest, 10, 0, 0),
            (701, "08185296380@s.whatsapp.net", "Me", group_latest, 1, 0, 0),
            (702, "08185296370@s.whatsapp.net", "\u200eCarol Contact", group_latest, 1, 0, 0),
            (703, "08185296372@s.whatsapp.net", "+08 185 29 63 72", group_latest, 1, 0, 0),
        ]
        connection.executemany(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            chats,
        )

        group_members = [
            (501, "08185296378@s.whatsapp.net", "\u200eAlice Member"),
            (502, "08185296379@s.whatsapp.net", None),
            (503, "08185296370@s.whatsapp.net", "Carol Group"),
            (504, "40482648260485@lid", None),
            (505, "404826482604827@lid", None),
            (506, "40482648260486@lid", None),
            (507, "404826482600@lid", None),
            (508, "08185296372@s.whatsapp.net", "\u202A+08 185 29 63 72\u202C"),
        ]
        connection.executemany(
            "INSERT INTO ZWAGROUPMEMBER (Z_PK, ZMEMBERJID, ZCONTACTNAME) VALUES (?, ?, ?)",
            group_members,
        )

        push_names = [
            ("\u200eBob Push", "08185296379@s.whatsapp.net"),
            ("Carol Push", "08185296370@s.whatsapp.net"),
            ("Alias Cedar", "40482648260485@lid"),
            ("Delta", "404826482604827@lid"),
            ("Delta", "08185296371@s.whatsapp.net"),
            ("Alias Birch", "40482648260486@lid"),
            ("Mystery Lid", "404826482600@lid"),
            ("Dana Push", "08185296372@s.whatsapp.net"),
        ]
        connection.executemany(
            "INSERT INTO ZWAPROFILEPUSHNAME (ZPUSHNAME, ZJID) VALUES (?, ?)",
            push_names,
        )

        messages = [
            (700001, "08185296380-123456@g.us", 0, 501, 700, "Hello from Alice", reference_date_timestamp(2024, 4, 8, 10, 0, 0), "08185296378@s.whatsapp.net", None, 0, None, "group-1"),
            (700002, "08185296380-123456@g.us", 0, 502, 700, "Hello from Bob", reference_date_timestamp(2024, 4, 8, 10, 30, 0), "08185296379@s.whatsapp.net", None, 0, None, "group-2"),
            (700003, "08185296380-123456@g.us", 0, None, 700, "Hello from me", group_latest, None, None, 1, None, "group-3"),
            (700006, "08185296380-123456@g.us", 0, 503, 700, "Hello from Carol", reference_date_timestamp(2024, 4, 8, 10, 40, 0), "08185296370@s.whatsapp.net", None, 0, None, "group-6"),
            (700007, "08185296380-123456@g.us", 0, 504, 700, "Hello from Cedar via LID", reference_date_timestamp(2024, 4, 8, 10, 42, 0), "40482648260485@lid", None, 0, None, "group-7"),
            (700008, "08185296380-123456@g.us", 0, 505, 700, "Hello from Delta via linked push name", reference_date_timestamp(2024, 4, 8, 10, 43, 0), "404826482604827@lid", None, 0, None, "group-8"),
            (700009, "08185296380-123456@g.us", 0, 506, 700, "Hello from an unresolved LID participant", reference_date_timestamp(2024, 4, 8, 10, 44, 0), "40482648260486@lid", None, 0, None, "group-9"),
            (700010, "08185296380-123456@g.us", 0, 507, 700, "Hello from a still unresolved LID participant", reference_date_timestamp(2024, 4, 8, 10, 44, 30), "404826482600@lid", None, 0, None, "group-10"),
            (700011, "08185296380-123456@g.us", 0, 508, 700, "Phone-only direct chat labels should not outrank push names in groups", reference_date_timestamp(2024, 4, 8, 10, 44, 45), "08185296372@s.whatsapp.net", None, 0, None, "group-11"),
            (700004, "08185296380-123456@g.us", 10, 501, 700, "token", reference_date_timestamp(2024, 4, 8, 10, 45, 0), "08185296378@s.whatsapp.net", None, 0, 40, "group-status-1"),
            (700005, "08185296380-123456@g.us", 10, None, 700, None, reference_date_timestamp(2024, 4, 8, 10, 50, 0), "08185296380-123456@g.us", None, 0, 2, "group-status-2"),
            (701001, "08185296380@s.whatsapp.net", 6, None, 701, None, reference_date_timestamp(2024, 4, 8, 9, 0, 0), None, None, 1, None, "owner-status-1"),
        ]
        connection.executemany(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            messages,
        )

    fixture = make_temporary_backup(name="group-invariant-backup", chat_storage_setup=setup)

    add_contacts_database(
        fixture,
        setup=lambda connection: connection.execute(
            """
            INSERT INTO ZWAADDRESSBOOKCONTACT
            (Z_PK, ZFULLNAME, ZGIVENNAME, ZBUSINESSNAME, ZLID, ZPHONENUMBER, ZWHATSAPPID)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "Alias Cedar", "Cedar", None, "40482648260485@lid", "690 103 286", "08185296389@s.whatsapp.net"),
        ),
    )

    add_lid_database(
        fixture,
        setup=lambda connection: connection.execute(
            """
            INSERT INTO ZWAZACCOUNT
            (Z_PK, ZIDENTIFIER, ZPHONENUMBER, ZCREATEDAT)
            VALUES (?, ?, ?, ?)
            """,
            (1, "40482648260486@lid", "08185296385", reference_date_timestamp(2025, 2, 10, 12, 0, 0)),
        ),
    )

    wa_backup = WABackup(backupPath=str(fixture.rootURL))
    wa_backup.connectChatStorageDb(fixture.backup)
    return wa_backup, fixture


def make_connected_incomplete_location_backup() -> tuple[WABackup, TemporaryBackupFixture]:
    def setup(connection: sqlite3.Connection) -> None:
        create_common_tables(connection)
        latest = reference_date_timestamp(2024, 4, 11, 18, 0, 0)

        connection.execute(
            """
            INSERT INTO ZWACHATSESSION
            (Z_PK, ZCONTACTJID, ZPARTNERNAME, ZLASTMESSAGEDATE, ZMESSAGECOUNTER, ZSESSIONTYPE, ZARCHIVED)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (830, "08185296387@s.whatsapp.net", "Location Contact", latest, 1, 0, 0),
        )
        connection.execute(
            """
            INSERT INTO ZWAMEDIAITEM
            (Z_PK, ZMETADATA, ZTITLE, ZMEDIALOCALPATH, ZMOVIEDURATION, ZLATITUDE, ZLONGITUDE)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (8301, None, None, None, None, None, None),
        )
        connection.execute(
            """
            INSERT INTO ZWAMESSAGE
            (Z_PK, ZTOJID, ZMESSAGETYPE, ZGROUPMEMBER, ZCHATSESSION, ZTEXT, ZMESSAGEDATE, ZFROMJID, ZMEDIAITEM, ZISFROMME, ZGROUPEVENTTYPE, ZSTANZAID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                830001,
                "08185296380@s.whatsapp.net",
                5,
                None,
                830,
                None,
                latest,
                "08185296387@s.whatsapp.net",
                8301,
                0,
                None,
                "location-incomplete-1",
            ),
        )

    fixture = make_temporary_backup(name="incomplete-location-backup", chat_storage_setup=setup)
    wa_backup = WABackup(backupPath=str(fixture.rootURL))
    wa_backup.connectChatStorageDb(fixture.backup)
    return wa_backup, fixture


def canonical_json(value: object) -> str:
    return canonical_json_dumps(value)


def make_full_fixture_backup() -> tuple[WABackup, IPhoneBackup]:
    if not FULL_FIXTURE_ROOT.exists():
        pytest.skip("Large local fixture backup is not available.")

    wa_backup = WABackup(backupPath=str(DATA_ROOT))
    backups = wa_backup.getBackups()
    backup = next((item for item in backups.validBackups if item.identifier == FULL_FIXTURE_IDENTIFIER), None)
    if backup is None:
        pytest.skip("Large local fixture backup is not available.")

    wa_backup.connectChatStorageDb(backup)
    return wa_backup, backup


def swift_oracle(command: str, *args: object) -> object:
    env = os.environ.copy()
    env.setdefault("SWIFT_BUILD_ENABLE_PLUGINS", "0")
    completed = subprocess.run(
        ["swift", "run", "--package-path", str(SWIFT_ORACLE_ROOT), "swift_wa_oracle", command, *[str(arg) for arg in args]],
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )
    return json.loads(completed.stdout)


REACTION_CASES = [
    {
        "chatName": "LPP",
        "messageId": 173611,
        "messageSnippet": "Lo invertimos, pasamos la reunión el martes. ¿Vale?",
        "expectedReactionEmoji": "👍",
        "expectedAuthor": {"displayName": "Cristina Pomares", "phone": "34655468076"},
    },
    {
        "chatName": "LPP",
        "messageId": 173814,
        "messageSnippet": "Aunque esta vez  la campana de Domingo se parece más a una meseta",
        "expectedReactionEmoji": "🙄",
        "expectedAuthor": {"displayName": "Cristina Pomares", "phone": "34655468076"},
    },
    {
        "chatName": "SENIOR UA",
        "messageId": 173590,
        "messageSnippet": "Curso ECONOMIA Y SOCIEDAD.pdf",
        "expectedReactionEmoji": "❤️",
        "expectedAuthor": {"displayName": "~Tete", "phone": "34629185419"},
    },
    {
        "chatName": "SENIOR UA",
        "messageId": 172100,
        "messageSnippet": "José Ramón, Bienvenido al grupo Senior UA",
        "expectedReactionEmoji": "❤️",
        "expectedAuthor": {"displayName": "~José Ramón Martínez-Riera", "phone": "34630403136"},
    },
    {
        "chatName": "Lambda Legends",
        "messageId": 172300,
        "messageSnippet": "Yo la única Gema que conozco es una prima de Eli",
        "expectedReactionEmoji": "😂",
        "expectedAuthor": {"displayName": "Cristina Pomares", "phone": "34655468076"},
    },
    {
        "chatName": "Lambda Legends",
        "messageId": 172359,
        "messageSnippet": "Así me salen 8 ficheros",
        "expectedReactionEmoji": "😅",
        "expectedAuthor": {"displayName": "Cristina Pomares", "phone": "34655468076"},
    },
    {
        "chatName": "Lambda Legends",
        "messageId": 172358,
        "messageSnippet": "pero creo que es mejor ficheros cortos y claros",
        "expectedReactionEmoji": "👍",
        "expectedAuthor": {"displayName": "Cristina Pomares", "phone": "34655468076"},
    },
    {
        "chatName": "Lambda Legends",
        "messageId": 172357,
        "messageSnippet": "Perfecto!! Y he subido todo al NotebookLM",
        "expectedReactionEmoji": "😱",
        "expectedAuthor": {"displayName": "Cristina Pomares", "phone": "34655468076"},
    },
]

FIXTURE_MESSAGE_TYPE_COUNTS = {
    "Audio": 6657,
    "Contact": 41,
    "Document": 316,
    "GIF": 66,
    "Image": 7495,
    "Link": 1150,
    "Location": 55,
    "Sticker": 371,
    "Text": 95678,
    "Video": 787,
}

FIXTURE_CONTACT_SUMMARY = {
    "uniqueContacts": 348,
    "contactsWithImage": 198,
    "contactsWithoutImage": 150,
}
