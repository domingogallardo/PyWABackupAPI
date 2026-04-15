from __future__ import annotations

import plistlib
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .errors import (
    DatabaseConnectionError,
    DirectoryAccessError,
    InvalidBackupError,
    MediaNotFoundError,
    RecordNotFoundError,
    UnexpectedDomainError,
    UnsupportedSchemaError,
)
from .files import FileUtils, FilenameAndHash, MediaCopier
from .models import (
    BackupFetchResult,
    BackupDiscoveryInfo,
    BackupDiscoveryStatus,
    ChatDumpPayload,
    ChatInfo,
    ChatType,
    ContactInfo,
    MessageAuthor,
    MessageAuthorKind,
    MessageAuthorSource,
    MessageInfo,
    Reaction,
)
from .parsers import ReactionParser, extract_reply_stanza_id
from .utils import (
    apple_reference_date_to_datetime,
    check_table_schema,
    ensure_utc,
    extracted_phone,
    is_group_jid,
    is_individual_jid,
    is_lid_jid,
    jid_user,
    normalized_author_field,
    normalize_whatsapp_display_text,
    question_marks,
    row_datetime,
    row_value,
)


MANIFEST_DOMAIN = "AppDomainGroup-group.net.whatsapp.WhatsApp.shared"


class SupportedMessageType:
    TEXT = 0
    IMAGE = 1
    VIDEO = 2
    AUDIO = 3
    CONTACT = 4
    LOCATION = 5
    LINK = 7
    DOC = 8
    GIF = 11
    STICKER = 15

    DESCRIPTIONS = {
        TEXT: "Text",
        IMAGE: "Image",
        VIDEO: "Video",
        AUDIO: "Audio",
        CONTACT: "Contact",
        LOCATION: "Location",
        LINK: "Link",
        DOC: "Document",
        GIF: "GIF",
        STICKER: "Sticker",
    }

    @classmethod
    def all_values(cls) -> list[int]:
        return list(cls.DESCRIPTIONS.keys())

    @classmethod
    def description(cls, raw_value: int) -> str | None:
        return cls.DESCRIPTIONS.get(raw_value)


def _connect_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


@dataclass(slots=True)
class IPhoneBackup:
    url: Path
    creationDate: datetime
    isEncrypted: bool | None = None

    @property
    def path(self) -> str:
        return str(self.url)

    @property
    def identifier(self) -> str:
        return self.url.name

    def getUrl(self, fileHash: str) -> Path:
        return self.url / fileHash[:2] / fileHash

    def fetchWAFileHash(self, relativePath: str) -> str:
        try:
            with _connect_database(self.url / "Manifest.db") as connection:
                row = connection.execute(
                    """
                    SELECT fileID FROM Files
                    WHERE relativePath LIKE ?
                      AND domain = ?
                    """,
                    (f"%{relativePath}", MANIFEST_DOMAIN),
                ).fetchone()
        except Exception as error:
            raise DatabaseConnectionError(error) from error

        if row is None or row["fileID"] is None:
            raise DatabaseConnectionError(MediaNotFoundError(relativePath))

        return str(row["fileID"])

    def fetchWAFileDetails(self, relativePath: str) -> list[FilenameAndHash]:
        try:
            with _connect_database(self.url / "Manifest.db") as connection:
                rows = connection.execute(
                    """
                    SELECT fileID, relativePath FROM Files
                    WHERE relativePath LIKE ?
                      AND domain = ?
                    """,
                    (f"%{relativePath}%", MANIFEST_DOMAIN),
                ).fetchall()
        except Exception:
            return []

        result: list[FilenameAndHash] = []
        for row in rows:
            file_hash = row["fileID"]
            filename = row["relativePath"]
            if file_hash is not None and filename is not None:
                result.append((str(filename), str(file_hash)))
        return result

    def get_url(self, file_hash: str) -> Path:
        return self.getUrl(file_hash)

    def fetch_wa_file_hash(self, relative_path: str) -> str:
        return self.fetchWAFileHash(relative_path)

    def fetch_wa_file_details(self, relative_path: str) -> list[FilenameAndHash]:
        return self.fetchWAFileDetails(relative_path)


class BackupManager:
    def __init__(self, backupPath: str = "~/Library/Application Support/MobileSync/Backup/"):
        self.backupPath = backupPath

    def getBackups(self) -> BackupFetchResult:
        backup_root = Path(self.backupPath).expanduser()
        try:
            contents = list(backup_root.iterdir())
        except Exception as error:
            raise DirectoryAccessError(error) from error

        valid_backups: list[IPhoneBackup] = []
        invalid_backups: list[Path] = []

        for path in contents:
            if not path.is_dir():
                continue
            try:
                valid_backups.append(self._get_backup(path))
            except Exception:
                invalid_backups.append(path)

        return BackupFetchResult(validBackups=valid_backups, invalidBackups=invalid_backups)

    def inspectBackups(self) -> list[BackupDiscoveryInfo]:
        backup_root = Path(self.backupPath).expanduser()
        try:
            contents = list(backup_root.iterdir())
        except Exception as error:
            raise DirectoryAccessError(error) from error

        inspections: list[BackupDiscoveryInfo] = []
        for path in contents:
            if not path.is_dir():
                continue
            inspections.append(self._inspect_backup(path))

        return inspections

    def _get_backup(self, path: Path) -> IPhoneBackup:
        if not path.is_dir():
            raise InvalidBackupError(str(path), "Path is not a directory.")

        for expected_file in ("Info.plist", "Manifest.db", "Status.plist"):
            if not (path / expected_file).exists():
                raise InvalidBackupError(str(path), f"{expected_file} is missing.")

        try:
            with (path / "Status.plist").open("rb") as handle:
                plist = plistlib.load(handle)
            date = plist.get("Date")
            if not isinstance(date, datetime):
                raise InvalidBackupError(str(path), "Status.plist is malformed.")
            backup = IPhoneBackup(
                url=path,
                creationDate=ensure_utc(date),
                isEncrypted=self._encryption_state(path)[0],
            )
            try:
                backup.fetchWAFileHash("ChatStorage.sqlite")
            except Exception as error:
                raise InvalidBackupError(str(path), "WhatsApp database not found.") from error
            return backup
        except InvalidBackupError:
            raise
        except Exception as error:
            raise DirectoryAccessError(error) from error

    def get_backups(self) -> BackupFetchResult:
        return self.getBackups()

    def inspect_backups(self) -> list[BackupDiscoveryInfo]:
        return self.inspectBackups()

    def _inspect_backup(self, path: Path) -> BackupDiscoveryInfo:
        identifier = path.name

        if not path.is_dir():
            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=None,
                status=BackupDiscoveryStatus.MISSING_REQUIRED_FILE,
                isReady=False,
                issue="Path is not a directory.",
            )

        for expected_file in ("Info.plist", "Manifest.db", "Status.plist"):
            if not (path / expected_file).exists():
                return BackupDiscoveryInfo(
                    identifier=identifier,
                    path=str(path),
                    creationDate=None,
                    status=BackupDiscoveryStatus.MISSING_REQUIRED_FILE,
                    isReady=False,
                    issue=f"{expected_file} is missing.",
                )

        try:
            with (path / "Status.plist").open("rb") as handle:
                plist = plistlib.load(handle)
            date = plist.get("Date")
            if not isinstance(date, datetime):
                raise InvalidBackupError(str(path), "Status.plist is malformed.")
            creation_date = ensure_utc(date)
        except InvalidBackupError as error:
            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=None,
                status=BackupDiscoveryStatus.MALFORMED_STATUS_PLIST,
                isReady=False,
                issue=str(error),
            )
        except Exception as error:
            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=None,
                status=BackupDiscoveryStatus.UNREADABLE_BACKUP,
                isReady=False,
                issue=str(error),
            )

        is_encrypted, encryption_issue = self._encryption_state(path)
        backup = IPhoneBackup(url=path, creationDate=creation_date, isEncrypted=is_encrypted)

        try:
            backup.fetchWAFileHash("ChatStorage.sqlite")
        except DatabaseConnectionError as error:
            if isinstance(error.underlying, MediaNotFoundError):
                return BackupDiscoveryInfo(
                    identifier=identifier,
                    path=str(path),
                    creationDate=creation_date,
                    status=BackupDiscoveryStatus.MISSING_WHATSAPP_DATABASE,
                    isReady=False,
                    isEncrypted=is_encrypted,
                    issue="WhatsApp database not found.",
                )

            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=creation_date,
                status=BackupDiscoveryStatus.UNREADABLE_MANIFEST_DATABASE,
                isReady=False,
                isEncrypted=is_encrypted,
                issue=str(error),
            )
        except Exception as error:
            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=creation_date,
                status=BackupDiscoveryStatus.UNREADABLE_MANIFEST_DATABASE,
                isReady=False,
                isEncrypted=is_encrypted,
                issue=str(error),
            )

        if is_encrypted is True:
            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=creation_date,
                status=BackupDiscoveryStatus.ENCRYPTED,
                isReady=False,
                isEncrypted=True,
                issue="Backup is encrypted.",
                backup=backup,
            )

        if is_encrypted is False:
            return BackupDiscoveryInfo(
                identifier=identifier,
                path=str(path),
                creationDate=creation_date,
                status=BackupDiscoveryStatus.READY,
                isReady=True,
                isEncrypted=False,
                issue=None,
                backup=backup,
            )

        return BackupDiscoveryInfo(
            identifier=identifier,
            path=str(path),
            creationDate=creation_date,
            status=BackupDiscoveryStatus.ENCRYPTION_STATUS_UNAVAILABLE,
            isReady=False,
            isEncrypted=None,
            issue=encryption_issue,
            backup=backup,
        )

    def _encryption_state(self, path: Path) -> tuple[bool | None, str | None]:
        manifest_plist = path / "Manifest.plist"
        if not manifest_plist.exists():
            return None, "Manifest.plist is missing, so encryption status could not be determined."

        try:
            with manifest_plist.open("rb") as handle:
                plist = plistlib.load(handle)
        except Exception as error:
            return None, (
                "Manifest.plist could not be read, so encryption status could not be determined: "
                f"{error}"
            )

        if not isinstance(plist, dict):
            return None, "Manifest.plist is malformed, so encryption status could not be determined."

        is_encrypted = plist.get("IsEncrypted")
        if not isinstance(is_encrypted, bool):
            return None, "Manifest.plist does not contain IsEncrypted, so encryption status could not be determined."

        return is_encrypted, None


@dataclass(slots=True)
class Message:
    id: int
    chatSessionId: int
    text: str | None
    date: datetime
    isFromMe: bool
    messageType: int
    groupMemberId: int | None
    mediaItemId: int | None
    fromJid: str | None
    toJid: str | None
    stanzaId: str | None
    parentMessageId: int | None

    TABLE_NAME = "ZWAMESSAGE"
    EXPECTED_COLUMNS = {
        "Z_PK",
        "ZTOJID",
        "ZMESSAGETYPE",
        "ZGROUPMEMBER",
        "ZCHATSESSION",
        "ZTEXT",
        "ZMESSAGEDATE",
        "ZFROMJID",
        "ZMEDIAITEM",
        "ZISFROMME",
        "ZSTANZAID",
        "ZPARENTMESSAGE",
    }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Message":
        return cls(
            id=int(row_value(row, "Z_PK", 0)),
            chatSessionId=int(row_value(row, "ZCHATSESSION", 0)),
            text=row_value(row, "ZTEXT", None),
            date=row_datetime(row, "ZMESSAGEDATE"),
            isFromMe=int(row_value(row, "ZISFROMME", 0)) == 1,
            messageType=int(row_value(row, "ZMESSAGETYPE", -1)),
            groupMemberId=row_value(row, "ZGROUPMEMBER", None),
            mediaItemId=row_value(row, "ZMEDIAITEM", None),
            fromJid=row_value(row, "ZFROMJID", None),
            toJid=row_value(row, "ZTOJID", None),
            stanzaId=row_value(row, "ZSTANZAID", None),
            parentMessageId=row_value(row, "ZPARENTMESSAGE", None),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_messages(cls, chat_id: int, connection: sqlite3.Connection) -> list["Message"]:
        supported = SupportedMessageType.all_values()
        placeholders = question_marks(len(supported))
        rows = connection.execute(
            f"""
            SELECT * FROM {cls.TABLE_NAME}
            WHERE ZCHATSESSION = ?
              AND ZMESSAGETYPE IN ({placeholders})
            ORDER BY ZMESSAGEDATE ASC, Z_PK ASC
            """,
            (chat_id, *supported),
        ).fetchall()
        return [cls.from_row(row) for row in rows]

    @classmethod
    def fetch_public_summary(cls, chat_id: int, connection: sqlite3.Connection) -> tuple[int, datetime | None]:
        supported = SupportedMessageType.all_values()
        placeholders = question_marks(len(supported))
        row = connection.execute(
            f"""
            SELECT COUNT(Z_PK) AS messageCount,
                   MAX(ZMESSAGEDATE) AS publicLastMessageDate
            FROM {cls.TABLE_NAME}
            WHERE ZCHATSESSION = ?
              AND ZMESSAGETYPE IN ({placeholders})
            """,
            (chat_id, *supported),
        ).fetchone()
        count = int(row_value(row, "messageCount", 0)) if row is not None else 0
        if count <= 0:
            return (0, None)
        return (count, row_datetime(row, "publicLastMessageDate"))

    @classmethod
    def fetch_owner_jid(cls, connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            f"""
            SELECT ZTOJID FROM {cls.TABLE_NAME}
            WHERE ZMESSAGETYPE = 6
              AND ZTOJID LIKE '%@s.whatsapp.net'
            LIMIT 1
            """
        ).fetchone()
        if row is not None and row["ZTOJID"] is not None:
            return str(row["ZTOJID"])

        row = connection.execute(
            f"""
            SELECT ZTOJID FROM {cls.TABLE_NAME}
            WHERE ZMESSAGETYPE = 6
              AND ZTOJID IS NOT NULL
            LIMIT 1
            """
        ).fetchone()
        return None if row is None else row_value(row, "ZTOJID", None)

    @classmethod
    def fetch_by_id(cls, message_id: int, connection: sqlite3.Connection) -> "Message" | None:
        row = connection.execute(
            f"SELECT * FROM {cls.TABLE_NAME} WHERE Z_PK = ?",
            (message_id,),
        ).fetchone()
        return None if row is None else cls.from_row(row)

    @classmethod
    def fetch_message_id_by_stanza_id(cls, stanza_id: str, connection: sqlite3.Connection) -> int | None:
        row = connection.execute(
            f"SELECT Z_PK FROM {cls.TABLE_NAME} WHERE ZSTANZAID = ?",
            (stanza_id,),
        ).fetchone()
        if row is None or row["Z_PK"] is None:
            return None
        return int(row["Z_PK"])


@dataclass(slots=True)
class ChatSession:
    id: int
    contactJid: str
    partnerName: str
    lastMessageDate: datetime
    messageCounter: int
    isArchived: bool
    sessionType: int

    TABLE_NAME = "ZWACHATSESSION"
    EXPECTED_COLUMNS = {
        "Z_PK",
        "ZCONTACTJID",
        "ZPARTNERNAME",
        "ZLASTMESSAGEDATE",
        "ZMESSAGECOUNTER",
        "ZSESSIONTYPE",
        "ZARCHIVED",
    }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ChatSession":
        return cls(
            id=int(row_value(row, "Z_PK", 0)),
            contactJid=str(row_value(row, "ZCONTACTJID", "")),
            partnerName=str(row_value(row, "ZPARTNERNAME", "")),
            lastMessageDate=row_datetime(row, "ZLASTMESSAGEDATE"),
            messageCounter=int(row_value(row, "ZMESSAGECOUNTER", 0)),
            isArchived=int(row_value(row, "ZARCHIVED", 0)) == 1,
            sessionType=int(row_value(row, "ZSESSIONTYPE", 0)),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_all_chats(cls, connection: sqlite3.Connection) -> list["ChatSession"]:
        supported = SupportedMessageType.all_values()
        placeholders = question_marks(len(supported))
        rows = connection.execute(
            f"""
            SELECT cs.*,
                   COUNT(m.Z_PK) AS messageCount,
                   MAX(m.ZMESSAGEDATE) AS publicLastMessageDate
            FROM {cls.TABLE_NAME} cs
            JOIN ZWAMESSAGE m ON m.ZCHATSESSION = cs.Z_PK
            WHERE cs.ZCONTACTJID NOT LIKE ?
              AND m.ZMESSAGETYPE IN ({placeholders})
            GROUP BY cs.Z_PK
            """,
            ("%@status", *supported),
        ).fetchall()

        sessions: list[ChatSession] = []
        for row in rows:
            session = cls.from_row(row)
            session.messageCounter = int(row_value(row, "messageCount", session.messageCounter))
            session.lastMessageDate = row_datetime(row, "publicLastMessageDate", session.lastMessageDate)
            sessions.append(session)
        return sessions

    @classmethod
    def fetch_chat(cls, chat_id: int, connection: sqlite3.Connection) -> "ChatSession":
        row = connection.execute(
            f"SELECT * FROM {cls.TABLE_NAME} WHERE Z_PK = ?",
            (chat_id,),
        ).fetchone()
        if row is None:
            raise RecordNotFoundError(cls.TABLE_NAME, chat_id)
        return cls.from_row(row)

    @classmethod
    def fetch_chat_session_name(cls, contact_jid: str, connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            f"""
            SELECT ZPARTNERNAME FROM {cls.TABLE_NAME}
            WHERE ZCONTACTJID = ?
              AND ZSESSIONTYPE = 0
              AND TRIM(ZPARTNERNAME) <> ''
            LIMIT 1
            """,
            (contact_jid,),
        ).fetchone()
        return None if row is None else row_value(row, "ZPARTNERNAME", None)


@dataclass(slots=True)
class GroupMember:
    id: int
    memberJid: str
    contactName: str | None

    TABLE_NAME = "ZWAGROUPMEMBER"
    EXPECTED_COLUMNS = {"Z_PK", "ZMEMBERJID", "ZCONTACTNAME"}
    ACTIVE_MEMBERSHIP_COLUMNS = {"ZCHATSESSION", "ZISACTIVE"}

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "GroupMember":
        return cls(
            id=int(row_value(row, "Z_PK", 0)),
            memberJid=str(row_value(row, "ZMEMBERJID", "")),
            contactName=row_value(row, "ZCONTACTNAME", None),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_group_member(cls, member_id: int, connection: sqlite3.Connection) -> "GroupMember" | None:
        row = connection.execute(
            f"SELECT * FROM {cls.TABLE_NAME} WHERE Z_PK = ?",
            (member_id,),
        ).fetchone()
        return None if row is None else cls.from_row(row)

    @classmethod
    def fetch_active_group_members(cls, chat_id: int, connection: sqlite3.Connection) -> list["GroupMember"]:
        rows = connection.execute(f"PRAGMA table_info({cls.TABLE_NAME})").fetchall()
        column_names = {str(row["name"]).upper() for row in rows}
        if not cls.ACTIVE_MEMBERSHIP_COLUMNS.issubset(column_names):
            return []

        rows = connection.execute(
            f"""
            SELECT *
            FROM {cls.TABLE_NAME}
            WHERE ZCHATSESSION = ?
              AND IFNULL(ZISACTIVE, 0) = 1
            ORDER BY Z_PK
            """,
            (chat_id,),
        ).fetchall()
        return [cls.from_row(row) for row in rows]

    @classmethod
    def fetch_group_member_ids(cls, chat_id: int, connection: sqlite3.Connection) -> list[int]:
        supported = SupportedMessageType.all_values()
        placeholders = question_marks(len(supported))
        rows = connection.execute(
            f"""
            SELECT DISTINCT ZGROUPMEMBER
            FROM ZWAMESSAGE
            WHERE ZCHATSESSION = ?
              AND ZMESSAGETYPE IN ({placeholders})
            """,
            (chat_id, *supported),
        ).fetchall()
        return [int(row["ZGROUPMEMBER"]) for row in rows if row["ZGROUPMEMBER"] is not None]


@dataclass(slots=True)
class ProfilePushName:
    jid: str
    pushName: str

    TABLE_NAME = "ZWAPROFILEPUSHNAME"
    EXPECTED_COLUMNS = {"ZPUSHNAME", "ZJID"}

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ProfilePushName":
        return cls(
            jid=str(row_value(row, "ZJID", "")),
            pushName=str(row_value(row, "ZPUSHNAME", "")),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_all(cls, connection: sqlite3.Connection) -> list["ProfilePushName"]:
        rows = connection.execute(f"SELECT * FROM {cls.TABLE_NAME}").fetchall()
        return [cls.from_row(row) for row in rows]

    @classmethod
    def push_name(cls, contact_jid: str, connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            f"SELECT * FROM {cls.TABLE_NAME} WHERE ZJID = ?",
            (contact_jid,),
        ).fetchone()
        if row is None:
            return None
        return cls.from_row(row).pushName


@dataclass(slots=True)
class MediaItem:
    id: int
    localPath: str | None
    metadata: bytes | None
    title: str | None
    movieDuration: int | None
    latitude: float | None
    longitude: float | None

    TABLE_NAME = "ZWAMEDIAITEM"
    EXPECTED_COLUMNS = {
        "Z_PK",
        "ZMETADATA",
        "ZTITLE",
        "ZMEDIALOCALPATH",
        "ZMOVIEDURATION",
        "ZLATITUDE",
        "ZLONGITUDE",
    }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MediaItem":
        return cls(
            id=int(row_value(row, "Z_PK", 0)),
            localPath=row_value(row, "ZMEDIALOCALPATH", None),
            metadata=row_value(row, "ZMETADATA", None),
            title=row_value(row, "ZTITLE", None),
            movieDuration=row_value(row, "ZMOVIEDURATION", None),
            latitude=row_value(row, "ZLATITUDE", None),
            longitude=row_value(row, "ZLONGITUDE", None),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_media_item(cls, media_item_id: int, connection: sqlite3.Connection) -> "MediaItem" | None:
        row = connection.execute(
            f"SELECT * FROM {cls.TABLE_NAME} WHERE Z_PK = ?",
            (media_item_id,),
        ).fetchone()
        return None if row is None else cls.from_row(row)

    def extract_reply_stanza_id(self) -> str | None:
        if self.metadata is None:
            return None
        return extract_reply_stanza_id(self.metadata)


@dataclass(slots=True)
class MessageInfoRecord:
    messageId: int
    receiptInfo: bytes | None

    TABLE_NAME = "ZWAMESSAGEINFO"
    EXPECTED_COLUMNS = {"Z_PK", "ZRECEIPTINFO", "ZMESSAGE"}

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MessageInfoRecord":
        return cls(
            messageId=int(row_value(row, "ZMESSAGE", 0)),
            receiptInfo=row_value(row, "ZRECEIPTINFO", None),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_by_message_id(cls, message_id: int, connection: sqlite3.Connection) -> "MessageInfoRecord" | None:
        row = connection.execute(
            f"SELECT * FROM {cls.TABLE_NAME} WHERE ZMESSAGE = ?",
            (message_id,),
        ).fetchone()
        return None if row is None else cls.from_row(row)


@dataclass(slots=True)
class AddressBookContact:
    id: int
    fullName: str | None
    givenName: str | None
    businessName: str | None
    lid: str | None
    phoneNumber: str | None
    whatsAppID: str | None

    TABLE_NAME = "ZWAADDRESSBOOKCONTACT"
    EXPECTED_COLUMNS = {
        "Z_PK",
        "ZFULLNAME",
        "ZGIVENNAME",
        "ZBUSINESSNAME",
        "ZLID",
        "ZPHONENUMBER",
        "ZWHATSAPPID",
    }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "AddressBookContact":
        return cls(
            id=int(row_value(row, "Z_PK", 0)),
            fullName=row_value(row, "ZFULLNAME", None),
            givenName=row_value(row, "ZGIVENNAME", None),
            businessName=row_value(row, "ZBUSINESSNAME", None),
            lid=row_value(row, "ZLID", None),
            phoneNumber=row_value(row, "ZPHONENUMBER", None),
            whatsAppID=row_value(row, "ZWHATSAPPID", None),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_all(cls, connection: sqlite3.Connection) -> list["AddressBookContact"]:
        rows = connection.execute(f"SELECT * FROM {cls.TABLE_NAME}").fetchall()
        return [cls.from_row(row) for row in rows]

    @property
    def bestDisplayName(self) -> str | None:
        for value in (self.fullName, self.businessName, self.givenName):
            normalized = normalized_author_field(value)
            if normalized is not None:
                return normalized
        return None

    @property
    def bestResolvedJid(self) -> str | None:
        if self.whatsAppID:
            return self.whatsAppID
        if self.lid:
            return self.lid
        return None

    @property
    def bestResolvedPhone(self) -> str | None:
        if self.whatsAppID:
            phone = extracted_phone(self.whatsAppID)
            if phone:
                return phone
        if self.phoneNumber:
            digits = "".join(ch for ch in self.phoneNumber if ch.isdigit())
            if digits:
                return digits
        return None


class AddressBookIndex:
    def __init__(self, contacts: list[AddressBookContact]):
        self.byLidJid: dict[str, AddressBookContact] = {}
        self.byWhatsAppJid: dict[str, AddressBookContact] = {}
        self.byPhone: dict[str, AddressBookContact] = {}

        for contact in contacts:
            if contact.lid:
                lid = contact.lid.lower()
                if lid:
                    self.byLidJid[lid] = contact
            if contact.whatsAppID:
                jid = contact.whatsAppID.lower()
                if jid:
                    self.byWhatsAppJid[jid] = contact
                    phone = extracted_phone(jid)
                    if phone:
                        self.byPhone[phone] = contact
            phone = contact.bestResolvedPhone
            if phone and phone not in self.byPhone:
                self.byPhone[phone] = contact

    @classmethod
    def load_if_present(cls, backup: IPhoneBackup) -> "AddressBookIndex" | None:
        try:
            file_hash = backup.fetchWAFileHash("ContactsV2.sqlite")
        except Exception:
            return None

        with _connect_database(backup.getUrl(file_hash)) as connection:
            AddressBookContact.check_schema(connection)
            contacts = AddressBookContact.fetch_all(connection)
            return cls(contacts)

    def contact(self, jid: str) -> AddressBookContact | None:
        normalized_jid = jid.lower()
        if normalized_jid in self.byLidJid:
            return self.byLidJid[normalized_jid]
        if normalized_jid in self.byWhatsAppJid:
            return self.byWhatsAppJid[normalized_jid]
        phone = extracted_phone(jid)
        if phone:
            return self.byPhone.get(phone)
        return None


@dataclass(slots=True)
class LidAccount:
    id: int
    identifier: str | None
    phoneNumber: str | None
    createdAt: float | None

    TABLE_NAME = "ZWAZACCOUNT"
    EXPECTED_COLUMNS = {"Z_PK", "ZIDENTIFIER", "ZPHONENUMBER", "ZCREATEDAT"}

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "LidAccount":
        return cls(
            id=int(row_value(row, "Z_PK", 0)),
            identifier=row_value(row, "ZIDENTIFIER", None),
            phoneNumber=row_value(row, "ZPHONENUMBER", None),
            createdAt=row_value(row, "ZCREATEDAT", None),
        )

    @classmethod
    def check_schema(cls, connection: sqlite3.Connection) -> None:
        check_table_schema(connection, cls.TABLE_NAME, cls.EXPECTED_COLUMNS)

    @classmethod
    def fetch_all_resolvable(cls, connection: sqlite3.Connection) -> list["LidAccount"]:
        rows = connection.execute(
            f"""
            SELECT * FROM {cls.TABLE_NAME}
            WHERE ZIDENTIFIER IS NOT NULL
              AND ZIDENTIFIER LIKE '%@lid'
              AND ZPHONENUMBER IS NOT NULL
              AND ZPHONENUMBER != ''
            """
        ).fetchall()
        return [cls.from_row(row) for row in rows]

    @property
    def normalizedLidJid(self) -> str | None:
        if self.identifier is None:
            return None
        normalized = self.identifier.strip().lower()
        return normalized or None

    @property
    def normalizedPhoneNumber(self) -> str | None:
        if self.phoneNumber is None:
            return None
        digits = "".join(ch for ch in self.phoneNumber if ch.isdigit())
        return digits or None

    @property
    def resolvedPhoneJid(self) -> str | None:
        phone = self.normalizedPhoneNumber
        return None if phone is None else f"{phone}@s.whatsapp.net"


class LidAccountIndex:
    def __init__(self, accounts: list[LidAccount]):
        self.byLidJid: dict[str, LidAccount] = {}
        for account in accounts:
            lid_jid = account.normalizedLidJid
            if lid_jid is None:
                continue
            existing = self.byLidJid.get(lid_jid)
            if existing is None:
                self.byLidJid[lid_jid] = account
                continue
            existing_created = float(existing.createdAt or float("-inf"))
            new_created = float(account.createdAt or float("-inf"))
            if new_created > existing_created:
                self.byLidJid[lid_jid] = account

    @classmethod
    def load_if_present(cls, backup: IPhoneBackup) -> "LidAccountIndex" | None:
        try:
            file_hash = backup.fetchWAFileHash("LID.sqlite")
        except Exception:
            return None

        with _connect_database(backup.getUrl(file_hash)) as connection:
            LidAccount.check_schema(connection)
            accounts = LidAccount.fetch_all_resolvable(connection)
            return cls(accounts)

    def account(self, jid: str) -> LidAccount | None:
        return self.byLidJid.get(jid.lower())

    def phoneNumber(self, jid: str) -> str | None:
        account = self.account(jid)
        return None if account is None else account.normalizedPhoneNumber

    def phoneJid(self, jid: str) -> str | None:
        account = self.account(jid)
        return None if account is None else account.resolvedPhoneJid


class PushNamePhoneJidIndex:
    def __init__(self, push_names: list[ProfilePushName]):
        grouped: dict[str, list[ProfilePushName]] = {}
        for row in push_names:
            key = normalize_whatsapp_display_text(row.pushName).lower()
            grouped.setdefault(key, []).append(row)

        self.linkedPhoneJidsByLidJid: dict[str, str] = {}
        for rows in grouped.values():
            lid_jids = sorted({row.jid for row in rows if is_lid_jid(row.jid)})
            phone_jids = sorted({row.jid for row in rows if is_individual_jid(row.jid)})
            if len(lid_jids) == 1 and len(phone_jids) == 1:
                self.linkedPhoneJidsByLidJid[lid_jids[0].lower()] = phone_jids[0]

    @classmethod
    def load(cls, connection: sqlite3.Connection) -> "PushNamePhoneJidIndex":
        return cls(ProfilePushName.fetch_all(connection))

    def linkedPhoneJid(self, jid: str) -> str | None:
        return self.linkedPhoneJidsByLidJid.get(jid.lower())


class WABackup:
    def __init__(self, backupPath: str = "~/Library/Application Support/MobileSync/Backup/"):
        self.phoneBackup = BackupManager(backupPath=backupPath)
        self._delegate: object | None = None
        self.chatDatabase: sqlite3.Connection | None = None
        self.iPhoneBackup: IPhoneBackup | None = None
        self.ownerJid: str | None = None
        self.mediaCopier: MediaCopier | None = None
        self.addressBookIndex: AddressBookIndex | None = None
        self.lidAccountIndex: LidAccountIndex | None = None
        self.pushNamePhoneJidIndex: PushNamePhoneJidIndex | None = None

    @property
    def delegate(self) -> object | None:
        return self._delegate

    @delegate.setter
    def delegate(self, value: object | None) -> None:
        self._delegate = value
        if self.mediaCopier is not None:
            self.mediaCopier.delegate = value

    def getBackups(self) -> BackupFetchResult:
        return self.phoneBackup.getBackups()

    def inspectBackups(self) -> list[BackupDiscoveryInfo]:
        return self.phoneBackup.inspectBackups()

    def connectChatStorageDb(self, backup: IPhoneBackup) -> None:
        chat_storage_hash = backup.fetchWAFileHash("ChatStorage.sqlite")
        chat_storage_url = backup.getUrl(chat_storage_hash)
        try:
            connection = _connect_database(chat_storage_url)
            self._check_schema(connection)
            self.chatDatabase = connection
            self.iPhoneBackup = backup
            self.ownerJid = Message.fetch_owner_jid(connection)
            self.mediaCopier = MediaCopier(backup=backup, delegate=self.delegate)
            self.addressBookIndex = AddressBookIndex.load_if_present(backup)
            self.lidAccountIndex = LidAccountIndex.load_if_present(backup)
            self.pushNamePhoneJidIndex = PushNamePhoneJidIndex.load(connection)
        except UnsupportedSchemaError:
            raise
        except DatabaseConnectionError:
            raise
        except Exception as error:
            raise DatabaseConnectionError(error) from error

    def _check_schema(self, connection: sqlite3.Connection) -> None:
        try:
            Message.check_schema(connection)
            ChatSession.check_schema(connection)
            GroupMember.check_schema(connection)
            ProfilePushName.check_schema(connection)
            MediaItem.check_schema(connection)
            MessageInfoRecord.check_schema(connection)
        except Exception as error:
            raise UnsupportedSchemaError("Incorrect WA Database Schema") from error

    def getChats(self, directoryToSavePhotos: Path | None = None) -> list[ChatInfo]:
        connection, backup = self._require_connection_and_backup()
        chat_infos: list[ChatInfo] = []

        for chat_session in ChatSession.fetch_all_chats(connection):
            if chat_session.sessionType == 5:
                continue

            photo_filename: str | None = None
            if directoryToSavePhotos is not None:
                photo_filename = self.fetchChatPhotoFilename(
                    contactJid=chat_session.contactJid,
                    chatId=chat_session.id,
                    directory=directoryToSavePhotos,
                    backup=backup,
                )

            chat_infos.append(
                ChatInfo(
                    id=chat_session.id,
                    contactJid=chat_session.contactJid,
                    name=self.resolvedChatName(chat_session),
                    numberMessages=chat_session.messageCounter,
                    lastMessageDate=chat_session.lastMessageDate,
                    isArchived=chat_session.isArchived,
                    photoFilename=photo_filename,
                )
            )

        return self.sortChatsByDate(chat_infos)

    def getChat(self, chatId: int, directoryToSaveMedia: Path | None = None) -> ChatDumpPayload:
        connection, backup = self._require_connection_and_backup()
        chat_info = self.fetchChatInfo(chatId, connection)
        messages = self.fetchMessagesFromDatabase(chatId, connection)
        processed_messages = self.processMessages(
            messages=messages,
            chatType=chat_info.chatType,
            directoryToSaveMedia=directoryToSaveMedia,
            backup=backup,
            connection=connection,
        )
        contacts = self.buildContactList(
            chatInfo=chat_info,
            connection=connection,
            backup=backup,
            directory=directoryToSaveMedia,
        )
        return ChatDumpPayload(chatInfo=chat_info, messages=processed_messages, contacts=contacts)

    def _require_connection_and_backup(self) -> tuple[sqlite3.Connection, IPhoneBackup]:
        if self.chatDatabase is None or self.iPhoneBackup is None:
            raise DatabaseConnectionError(RuntimeError("Database or backup not found"))
        return self.chatDatabase, self.iPhoneBackup

    def resolvedChatName(self, chatSession: ChatSession) -> str:
        if self.ownerJid is not None and chatSession.contactJid == self.ownerJid:
            return "Me"
        return normalize_whatsapp_display_text(chatSession.partnerName)

    def sortChatsByDate(self, chats: list[ChatInfo]) -> list[ChatInfo]:
        return sorted(chats, key=lambda chat: chat.lastMessageDate, reverse=True)

    def fetchChatPhotoFilename(
        self,
        contactJid: str,
        chatId: int,
        directory: Path,
        backup: IPhoneBackup,
    ) -> str | None:
        if is_individual_jid(contactJid):
            base_path = f"Media/Profile/{extracted_phone(contactJid)}"
        elif is_group_jid(contactJid):
            base_path = f"Media/Profile/{jid_user(contactJid)}"
        else:
            return None

        files = backup.fetchWAFileDetails(base_path)
        latest = FileUtils.latest_file(base_path, "jpg", files) or FileUtils.latest_file(base_path, "thumb", files)
        if latest is None:
            return None

        extension = ".jpg" if latest[0].endswith(".jpg") else ".thumb"
        file_name = f"chat_{chatId}{extension}"
        if self.mediaCopier is not None:
            self.mediaCopier.copy(latest[1], file_name, directory)
        return file_name

    def fetchChatInfo(self, chatId: int, connection: sqlite3.Connection) -> ChatInfo:
        chat_session = ChatSession.fetch_chat(chatId, connection)
        count, last_message_date = Message.fetch_public_summary(chatId, connection)
        return ChatInfo(
            id=chat_session.id,
            contactJid=chat_session.contactJid,
            name=self.resolvedChatName(chat_session),
            numberMessages=count,
            lastMessageDate=last_message_date or chat_session.lastMessageDate,
            isArchived=chat_session.isArchived,
        )

    def fetchMessagesFromDatabase(self, chatId: int, connection: sqlite3.Connection) -> list[Message]:
        return Message.fetch_messages(chatId, connection)

    def processMessages(
        self,
        messages: list[Message],
        chatType: ChatType,
        directoryToSaveMedia: Path | None,
        backup: IPhoneBackup,
        connection: sqlite3.Connection,
    ) -> list[MessageInfo]:
        result: list[MessageInfo] = []
        for message in messages:
            result.append(
                self.processSingleMessage(
                    message=message,
                    chatType=chatType,
                    directoryToSaveMedia=directoryToSaveMedia,
                    backup=backup,
                    connection=connection,
                )
            )
        return result

    def processSingleMessage(
        self,
        message: Message,
        chatType: ChatType,
        directoryToSaveMedia: Path | None,
        backup: IPhoneBackup,
        connection: sqlite3.Connection,
    ) -> MessageInfo:
        message_type = SupportedMessageType.description(message.messageType)
        if message_type is None:
            raise UnexpectedDomainError("Unsupported message type")

        participant_identity = self.resolveParticipantIdentity(message, chatType, connection)
        message_info = MessageInfo(
            id=message.id,
            chatId=message.chatSessionId,
            message=message.text,
            date=message.date,
            isFromMe=message.isFromMe,
            messageType=message_type,
            author=participant_identity,
        )

        reply_message_id = self.fetchReplyMessageId(message, connection)
        if reply_message_id is not None:
            message_info.replyTo = reply_message_id

        media_info = self.handleMedia(message, directoryToSaveMedia, backup, connection)
        if media_info is not None:
            message_info.mediaFilename = media_info["mediaFilename"]
            message_info.caption = media_info["caption"]
            message_info.seconds = media_info["seconds"]
            message_info.latitude = media_info["latitude"]
            message_info.longitude = media_info["longitude"]
            message_info.error = media_info["error"]

        message_info.reactions = self.fetchReactions(message.id, connection)
        return message_info

    def resolveParticipantIdentity(
        self,
        message: Message,
        chatType: ChatType,
        connection: sqlite3.Connection,
    ) -> MessageAuthor | None:
        if message.isFromMe:
            return MessageAuthor(
                kind=MessageAuthorKind.ME,
                displayName="Me",
                phone=normalized_author_field(extracted_phone(self.ownerJid)) if self.ownerJid else None,
                jid=normalized_author_field(self.ownerJid),
                source=MessageAuthorSource.OWNER,
            )

        if chatType == ChatType.GROUP:
            if message.groupMemberId is not None:
                group_member = GroupMember.fetch_group_member(message.groupMemberId, connection)
                if group_member is not None:
                    return self.makeParticipantAuthor(
                        jid=group_member.memberJid,
                        contactNameGroupMember=group_member.contactName,
                        fallbackSource=MessageAuthorSource.GROUP_MEMBER,
                        connection=connection,
                    )

            if normalized_author_field(message.fromJid) is not None:
                return self.makeParticipantAuthor(
                    jid=normalized_author_field(message.fromJid) or "",
                    contactNameGroupMember=None,
                    fallbackSource=MessageAuthorSource.MESSAGE_JID,
                    connection=connection,
                )
            return None

        chat_session = ChatSession.fetch_chat(message.chatSessionId, connection)
        display_name = normalized_author_field(chat_session.partnerName)
        if display_name is not None:
            return MessageAuthor(
                kind=MessageAuthorKind.PARTICIPANT,
                displayName=display_name,
                phone=self.resolvedPhone(chat_session.contactJid),
                jid=self.resolvedParticipantJid(chat_session.contactJid),
                source=MessageAuthorSource.CHAT_SESSION,
            )

        address_book_author = self.makeAddressBookAuthor(chat_session.contactJid)
        if address_book_author is not None:
            return address_book_author

        return MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName=None,
            phone=self.resolvedPhone(chat_session.contactJid),
            jid=self.resolvedParticipantJid(chat_session.contactJid),
            source=MessageAuthorSource.CHAT_SESSION,
        )

    def fetchReplyMessageId(self, message: Message, connection: sqlite3.Connection) -> int | None:
        if message.parentMessageId is not None:
            return message.parentMessageId

        if message.mediaItemId is not None:
            media_item = MediaItem.fetch_media_item(message.mediaItemId, connection)
            if media_item is not None:
                stanza_id = media_item.extract_reply_stanza_id()
                if stanza_id is not None:
                    return Message.fetch_message_id_by_stanza_id(stanza_id, connection)
        return None

    def handleMedia(
        self,
        message: Message,
        directoryToSaveMedia: Path | None,
        backup: IPhoneBackup,
        connection: sqlite3.Connection,
    ) -> dict[str, Any] | None:
        if message.mediaItemId is None:
            return None

        media_filename = self.fetchMediaFilename(message.mediaItemId, backup, directoryToSaveMedia, connection)
        caption = self.fetchCaption(message.mediaItemId, connection)

        seconds: int | None = None
        latitude: float | None = None
        longitude: float | None = None

        if message.messageType in {SupportedMessageType.VIDEO, SupportedMessageType.AUDIO}:
            seconds = self.fetchDuration(message.mediaItemId, connection)
        if message.messageType == SupportedMessageType.LOCATION:
            latitude, longitude = self.fetchLocation(message.mediaItemId, connection)

        return {
            "mediaFilename": media_filename,
            "caption": caption,
            "seconds": seconds,
            "latitude": latitude,
            "longitude": longitude,
            "error": None,
        }

    def fetchMediaFilename(
        self,
        mediaItemId: int,
        backup: IPhoneBackup,
        directory: Path | None,
        connection: sqlite3.Connection,
    ) -> str | None:
        media_item = MediaItem.fetch_media_item(mediaItemId, connection)
        if media_item is None or media_item.localPath is None:
            return None

        try:
            hash_file = backup.fetchWAFileHash(media_item.localPath)
        except Exception:
            return None

        file_name = Path(media_item.localPath).name
        if self.mediaCopier is not None:
            self.mediaCopier.copy(hash_file, file_name, directory)
        return file_name

    def fetchGroupMemberInfo(
        self,
        memberId: int,
        connection: sqlite3.Connection,
    ) -> tuple[str | None, str | None] | None:
        group_member = GroupMember.fetch_group_member(memberId, connection)
        if group_member is None:
            return None
        return self.fetchResolvedGroupMemberInfo(group_member, connection)

    def fetchResolvedGroupMemberInfo(
        self,
        groupMember: GroupMember,
        connection: sqlite3.Connection,
    ) -> tuple[str | None, str | None]:
        return self.obtainSenderInfo(
            jid=groupMember.memberJid,
            contactNameGroupMember=groupMember.contactName,
            connection=connection,
        )

    def fetchGroupContactMembers(
        self,
        chatId: int,
        connection: sqlite3.Connection,
    ) -> list[GroupMember]:
        active_members = GroupMember.fetch_active_group_members(chatId, connection)
        if active_members:
            return active_members

        members: list[GroupMember] = []
        for member_id in GroupMember.fetch_group_member_ids(chatId, connection):
            group_member = GroupMember.fetch_group_member(member_id, connection)
            if group_member is not None:
                members.append(group_member)
        return members

    def fetchDuration(self, mediaItemId: int, connection: sqlite3.Connection) -> int | None:
        media_item = MediaItem.fetch_media_item(mediaItemId, connection)
        if media_item is None or media_item.movieDuration is None:
            return None
        return int(media_item.movieDuration)

    def fetchReactions(self, messageId: int, connection: sqlite3.Connection) -> list[Reaction] | None:
        message_info = MessageInfoRecord.fetch_by_message_id(messageId, connection)
        if message_info is not None and message_info.receiptInfo is not None:
            parsed = self.parseReactions(message_info.receiptInfo, connection)
            if parsed is not None:
                return parsed
        return self.fetchDuplicateDocumentReactions(messageId, connection)

    def parseReactions(self, reactionsData: bytes, connection: sqlite3.Connection) -> list[Reaction] | None:
        def resolver(sender_jid: str) -> MessageAuthor | None:
            try:
                return self.resolveReactionAuthor(sender_jid, connection)
            except Exception:
                return None

        return ReactionParser.parse(reactionsData, senderAuthorResolver=resolver)

    def fetchDuplicateDocumentReactions(self, messageId: int, connection: sqlite3.Connection) -> list[Reaction] | None:
        message = Message.fetch_by_id(messageId, connection)
        if message is None:
            return None
        current_text = normalized_author_field(message.text)
        if message.messageType != SupportedMessageType.DOC or current_text is None:
            return None

        normalized_current_name = normalized_duplicate_document_name(current_text)
        search_window_start = message.date - timedelta(hours=12)
        start_ref = (search_window_start - datetime(2001, 1, 1, tzinfo=UTC)).total_seconds()
        end_ref = (message.date - datetime(2001, 1, 1, tzinfo=UTC)).total_seconds()

        rows = connection.execute(
            f"""
            SELECT * FROM {Message.TABLE_NAME}
            WHERE ZCHATSESSION = ?
              AND ZMESSAGETYPE = ?
              AND Z_PK <> ?
              AND ZTEXT IS NOT NULL
              AND ZMESSAGEDATE BETWEEN ? AND ?
            ORDER BY ZMESSAGEDATE DESC
            LIMIT 25
            """,
            (message.chatSessionId, message.messageType, message.id, start_ref, end_ref),
        ).fetchall()

        for row in rows:
            candidate = Message.from_row(row)
            candidate_text = normalized_author_field(candidate.text)
            if (
                candidate.groupMemberId != message.groupMemberId
                or candidate.isFromMe != message.isFromMe
                or candidate_text is None
                or normalized_duplicate_document_name(candidate_text) != normalized_current_name
                or current_text == candidate_text
                or not (
                    has_duplicate_document_copy_suffix(current_text)
                    or has_duplicate_document_copy_suffix(candidate_text)
                )
            ):
                continue

            message_info = MessageInfoRecord.fetch_by_message_id(candidate.id, connection)
            if message_info is None or message_info.receiptInfo is None:
                continue

            reactions = self.parseReactions(message_info.receiptInfo, connection)
            if reactions is not None:
                return reactions

        return None

    def resolveReactionAuthor(self, senderJid: str, connection: sqlite3.Connection) -> MessageAuthor | None:
        normalized_jid = normalized_author_field(senderJid)
        if normalized_jid is None:
            return None

        if normalized_jid == normalized_author_field(self.ownerJid):
            return MessageAuthor(
                kind=MessageAuthorKind.ME,
                displayName="Me",
                phone=normalized_author_field(extracted_phone(self.ownerJid)) if self.ownerJid else None,
                jid=normalized_author_field(self.ownerJid),
                source=MessageAuthorSource.OWNER,
            )

        return self.makeParticipantAuthor(
            jid=normalized_jid,
            contactNameGroupMember=None,
            fallbackSource=MessageAuthorSource.MESSAGE_JID,
            connection=connection,
        )

    def fetchCaption(self, mediaItemId: int, connection: sqlite3.Connection) -> str | None:
        media_item = MediaItem.fetch_media_item(mediaItemId, connection)
        if media_item is None or media_item.title is None or media_item.title == "":
            return None
        return media_item.title

    def fetchLocation(self, mediaItemId: int, connection: sqlite3.Connection) -> tuple[float | None, float | None]:
        media_item = MediaItem.fetch_media_item(mediaItemId, connection)
        if media_item is None:
            return (None, None)
        return (media_item.latitude, media_item.longitude)

    def obtainSenderInfo(
        self,
        jid: str,
        contactNameGroupMember: str | None,
        connection: sqlite3.Connection,
    ) -> tuple[str | None, str | None]:
        sender_phone = self.resolvedPhone(jid)
        chat_session_name = normalized_author_field(ChatSession.fetch_chat_session_name(jid, connection))

        if chat_session_name is not None and not self.isPhoneLikeDisplayLabel(chat_session_name, sender_phone):
            return (chat_session_name, sender_phone)

        if self.addressBookIndex is not None:
            address_book_contact = self.addressBookIndex.contact(jid)
            if address_book_contact is not None:
                display_name = normalized_author_field(address_book_contact.bestDisplayName)
                if display_name is not None:
                    return (
                        display_name,
                        normalized_author_field(address_book_contact.bestResolvedPhone) or sender_phone,
                    )

        if self.lidAccountIndex is not None:
            lid_account = self.lidAccountIndex.account(jid)
            if lid_account is not None:
                profile_display_name = normalized_author_field(ProfilePushName.push_name(jid, connection))
                linked_phone_jid = self.linkedPhoneJid(jid) or self.lidAccountIndex.phoneJid(jid)
                linked_phone_display_name: str | None = None
                if linked_phone_jid is not None:
                    linked_phone_display_name = self.resolvedContactDisplayName(
                        jid=linked_phone_jid,
                        profileDisplayName=normalized_author_field(ProfilePushName.push_name(linked_phone_jid, connection)),
                        senderPhone=normalized_author_field(extracted_phone(linked_phone_jid)),
                        connection=connection,
                    )
                return (
                    linked_phone_display_name or profile_display_name,
                    normalized_author_field(lid_account.normalizedPhoneNumber) or sender_phone,
                )

        linked_phone_jid = self.linkedPhoneJid(jid)
        if linked_phone_jid is not None:
            return (
                self.resolvedContactDisplayName(
                    jid=linked_phone_jid,
                    profileDisplayName=normalized_author_field(ProfilePushName.push_name(linked_phone_jid, connection)),
                    senderPhone=normalized_author_field(extracted_phone(linked_phone_jid)),
                    connection=connection,
                ),
                normalized_author_field(extracted_phone(linked_phone_jid)),
            )

        push_name = ProfilePushName.push_name(jid, connection)
        if push_name is not None:
            return (normalized_author_field(push_name), sender_phone)

        if chat_session_name is not None:
            return (chat_session_name, sender_phone)

        return (contactNameGroupMember, sender_phone)

    def resolvedContactDisplayName(
        self,
        jid: str,
        profileDisplayName: str | None,
        senderPhone: str | None,
        connection: sqlite3.Connection,
    ) -> str | None:
        chat_session_name = normalized_author_field(ChatSession.fetch_chat_session_name(jid, connection))
        if chat_session_name is not None and not self.isPhoneLikeDisplayLabel(chat_session_name, senderPhone):
            return chat_session_name

        if self.addressBookIndex is not None:
            address_book_contact = self.addressBookIndex.contact(jid)
            if address_book_contact is not None:
                display_name = normalized_author_field(address_book_contact.bestDisplayName)
                if display_name is not None:
                    return display_name

        return profileDisplayName

    def makeParticipantAuthor(
        self,
        jid: str,
        contactNameGroupMember: str | None,
        fallbackSource: MessageAuthorSource,
        connection: sqlite3.Connection,
    ) -> MessageAuthor:
        normalized_jid = normalized_author_field(jid)
        phone = self.resolvedPhone(jid)
        linked_phone_jid = self.linkedPhoneJid(jid)
        if linked_phone_jid is None and self.lidAccountIndex is not None:
            linked_phone_jid = self.lidAccountIndex.phoneJid(jid)

        chat_session_display_name = normalized_author_field(ChatSession.fetch_chat_session_name(jid, connection))
        profile_display_name = self.whatsAppProfileDisplayName(ProfilePushName.push_name(jid, connection))
        linked_phone_display_name: str | None = None
        if linked_phone_jid is not None:
            linked_phone_display_name = self.whatsAppProfileDisplayName(
                ProfilePushName.push_name(linked_phone_jid, connection)
            )

        if chat_session_display_name is not None and not self.isPhoneLikeDisplayLabel(chat_session_display_name, phone):
            return MessageAuthor(
                kind=MessageAuthorKind.PARTICIPANT,
                displayName=chat_session_display_name,
                phone=phone,
                jid=normalized_jid,
                source=MessageAuthorSource.CHAT_SESSION,
            )

        address_book_author = self.makeAddressBookAuthor(jid)
        if address_book_author is not None:
            return address_book_author

        lid_account_author = self.makeLidAccountAuthor(
            jid=jid,
            profileDisplayName=profile_display_name or linked_phone_display_name,
        )
        if lid_account_author is not None:
            return lid_account_author

        if linked_phone_jid is not None:
            return MessageAuthor(
                kind=MessageAuthorKind.PARTICIPANT,
                displayName=profile_display_name or linked_phone_display_name,
                phone=normalized_author_field(extracted_phone(linked_phone_jid)),
                jid=normalized_author_field(linked_phone_jid),
                source=MessageAuthorSource.PUSH_NAME_PHONE_JID,
            )

        if profile_display_name is not None:
            return MessageAuthor(
                kind=MessageAuthorKind.PARTICIPANT,
                displayName=profile_display_name,
                phone=phone,
                jid=normalized_jid,
                source=MessageAuthorSource.PUSH_NAME,
            )

        if chat_session_display_name is not None:
            return MessageAuthor(
                kind=MessageAuthorKind.PARTICIPANT,
                displayName=chat_session_display_name,
                phone=phone,
                jid=normalized_jid,
                source=MessageAuthorSource.CHAT_SESSION,
            )

        return MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName=normalized_author_field(contactNameGroupMember),
            phone=phone,
            jid=normalized_jid,
            source=fallbackSource,
        )

    def isPhoneLikeDisplayLabel(self, value: str | None, resolvedPhone: str | None) -> bool:
        normalized = normalized_author_field(value)
        if normalized is None:
            return False

        if any(char.isalpha() for char in normalized):
            return False

        digit_string = "".join(char for char in normalized if char.isdigit())
        if len(digit_string) < 7:
            return False

        if resolvedPhone is not None:
            normalized_phone = "".join(char for char in resolvedPhone if char.isdigit())
            return digit_string == normalized_phone

        return re.fullmatch(r"^\+?[\d\s().-]+$", normalized) is not None

    def whatsAppProfileDisplayName(self, value: str | None) -> str | None:
        normalized = normalized_author_field(value)
        return None if normalized is None else f"~{normalized}"

    def resolvedPhone(self, jid: str | None) -> str | None:
        if jid is None:
            return None
        if self.addressBookIndex is not None:
            contact = self.addressBookIndex.contact(jid)
            if contact is not None and contact.bestResolvedPhone is not None:
                return normalized_author_field(contact.bestResolvedPhone)
        if self.lidAccountIndex is not None:
            phone = self.lidAccountIndex.phoneNumber(jid)
            if phone is not None:
                return normalized_author_field(phone)
        linked_phone_jid = self.linkedPhoneJid(jid)
        if linked_phone_jid is not None:
            return normalized_author_field(extracted_phone(linked_phone_jid))
        if not is_individual_jid(jid):
            return None
        return normalized_author_field(extracted_phone(jid))

    def linkedPhoneJid(self, jid: str | None) -> str | None:
        if jid is None or self.pushNamePhoneJidIndex is None:
            return None
        return self.pushNamePhoneJidIndex.linkedPhoneJid(jid)

    def resolvedParticipantJid(self, jid: str | None) -> str | None:
        if jid is None:
            return None
        if self.addressBookIndex is not None:
            contact = self.addressBookIndex.contact(jid)
            if contact is not None and contact.bestResolvedJid is not None:
                return normalized_author_field(contact.bestResolvedJid)
        if self.lidAccountIndex is not None:
            phone_jid = self.lidAccountIndex.phoneJid(jid)
            if phone_jid is not None:
                return normalized_author_field(phone_jid)
        linked_phone_jid = self.linkedPhoneJid(jid)
        if linked_phone_jid is not None:
            return normalized_author_field(linked_phone_jid)
        return normalized_author_field(jid)

    def makeAddressBookAuthor(self, jid: str) -> MessageAuthor | None:
        if self.addressBookIndex is None:
            return None
        contact = self.addressBookIndex.contact(jid)
        if contact is None or normalized_author_field(contact.bestDisplayName) is None:
            return None

        display_name = normalized_author_field(contact.bestDisplayName)
        resolved_jid = normalized_author_field(contact.bestResolvedJid) or normalized_author_field(jid)
        resolved_phone = normalized_author_field(contact.bestResolvedPhone)
        if resolved_phone is None and is_individual_jid(jid):
            resolved_phone = normalized_author_field(extracted_phone(jid))

        return MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName=display_name,
            phone=resolved_phone,
            jid=resolved_jid,
            source=MessageAuthorSource.ADDRESS_BOOK,
        )

    def makeLidAccountAuthor(self, jid: str, profileDisplayName: str | None) -> MessageAuthor | None:
        if self.lidAccountIndex is None:
            return None
        lid_account = self.lidAccountIndex.account(jid)
        if lid_account is None or normalized_author_field(lid_account.normalizedPhoneNumber) is None:
            return None

        return MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName=profileDisplayName,
            phone=normalized_author_field(lid_account.normalizedPhoneNumber),
            jid=self.resolvedParticipantJid(jid),
            source=MessageAuthorSource.LID_ACCOUNT,
        )

    def buildContactList(
        self,
        chatInfo: ChatInfo,
        connection: sqlite3.Connection,
        backup: IPhoneBackup,
        directory: Path | None,
    ) -> list[ContactInfo]:
        contacts: list[ContactInfo] = []
        owner_phone = extracted_phone(self.ownerJid) if self.ownerJid else ""

        owner_contact = ContactInfo(name="Me", phone=owner_phone)
        if directory is not None:
            owner_contact = self.copyContactMedia(owner_contact, backup, directory)
        contacts.append(owner_contact)

        if chatInfo.chatType == ChatType.INDIVIDUAL:
            other_phone = extracted_phone(chatInfo.contactJid)
            if other_phone != owner_phone:
                other_contact = ContactInfo(name=chatInfo.name, phone=other_phone)
                if directory is not None:
                    other_contact = self.copyContactMedia(other_contact, backup, directory)
                contacts.append(other_contact)
        else:
            seen_phones = {owner_phone}
            for member in self.fetchGroupContactMembers(chatInfo.id, connection):
                sender_name, sender_phone = self.fetchResolvedGroupMemberInfo(member, connection)
                if sender_phone is None or sender_phone == owner_phone or sender_phone in seen_phones:
                    continue
                seen_phones.add(sender_phone)
                contact = ContactInfo(name=sender_name or sender_phone, phone=sender_phone)
                if directory is not None:
                    contact = self.copyContactMedia(contact, backup, directory)
                contacts.append(contact)

        return contacts

    def copyContactMedia(self, contact: ContactInfo, backup: IPhoneBackup, directory: Path | None) -> ContactInfo:
        updated = ContactInfo(name=contact.name, phone=contact.phone, photoFilename=contact.photoFilename)
        prefix = f"Media/Profile/{contact.phone}"
        files = backup.fetchWAFileDetails(prefix)
        latest = FileUtils.latest_file(prefix, "jpg", files) or FileUtils.latest_file(prefix, "thumb", files)
        if latest is None:
            return updated

        file_name = f"{contact.phone}{'.jpg' if latest[0].endswith('.jpg') else '.thumb'}"
        if self.mediaCopier is not None:
            self.mediaCopier.copy(latest[1], file_name, directory)
        updated.photoFilename = file_name
        return updated

    def get_backups(self) -> BackupFetchResult:
        return self.getBackups()

    def inspect_backups(self) -> list[BackupDiscoveryInfo]:
        return self.inspectBackups()

    def connect_chat_storage_db(self, backup: IPhoneBackup) -> None:
        self.connectChatStorageDb(backup)

    def get_chats(self, directory_to_save_photos: Path | None = None) -> list[ChatInfo]:
        return self.getChats(directory_to_save_photos)

    def get_chat(self, chat_id: int, directory_to_save_media: Path | None = None) -> ChatDumpPayload:
        return self.getChat(chat_id, directory_to_save_media)


def normalized_duplicate_document_name(value: str) -> str:
    trimmed = value.strip()
    if "." not in trimmed:
        return trimmed
    basename, extension = trimmed.rsplit(".", 1)
    basename = re.sub(r" \(\d+\)$", "", basename)
    basename = re.sub(r"-\d+$", "", basename)
    return f"{basename}.{extension}"


def has_duplicate_document_copy_suffix(value: str) -> bool:
    trimmed = value.strip()
    if "." not in trimmed:
        return False
    basename, _ = trimmed.rsplit(".", 1)
    return re.search(r" \(\d+\)$", basename) is not None or re.search(r"-\d+$", basename) is not None
