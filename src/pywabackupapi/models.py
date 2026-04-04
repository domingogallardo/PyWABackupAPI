from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Protocol

from .utils import is_group_jid


class ChatType(str, Enum):
    GROUP = "group"
    INDIVIDUAL = "individual"


class MessageAuthorKind(str, Enum):
    ME = "me"
    PARTICIPANT = "participant"


class MessageAuthorSource(str, Enum):
    OWNER = "owner"
    CHAT_SESSION = "chatSession"
    ADDRESS_BOOK = "addressBook"
    LID_ACCOUNT = "lidAccount"
    PUSH_NAME = "pushName"
    PUSH_NAME_PHONE_JID = "pushNamePhoneJid"
    GROUP_MEMBER = "groupMember"
    MESSAGE_JID = "messageJid"


@dataclass(slots=True)
class ChatInfo:
    id: int
    contactJid: str
    name: str
    numberMessages: int
    lastMessageDate: datetime
    isArchived: bool
    photoFilename: str | None = None
    chatType: ChatType = field(init=False)

    def __post_init__(self) -> None:
        self.chatType = ChatType.GROUP if is_group_jid(self.contactJid) else ChatType.INDIVIDUAL


@dataclass(slots=True)
class MessageAuthor:
    kind: MessageAuthorKind
    displayName: str | None
    phone: str | None
    jid: str | None
    source: MessageAuthorSource


@dataclass(slots=True)
class Reaction:
    emoji: str
    author: MessageAuthor

    @property
    def senderPhone(self) -> str | None:
        if self.author.kind == MessageAuthorKind.ME:
            return "Me"
        return self.author.phone


@dataclass(slots=True)
class MessageInfo:
    id: int
    chatId: int
    message: str | None
    date: datetime
    isFromMe: bool
    messageType: str
    author: MessageAuthor | None = None
    caption: str | None = None
    replyTo: int | None = None
    mediaFilename: str | None = None
    reactions: list[Reaction] | None = None
    error: str | None = None
    seconds: int | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass(eq=False, slots=True)
class ContactInfo:
    name: str
    phone: str
    photoFilename: str | None = None

    def __hash__(self) -> int:
        return hash(self.phone)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ContactInfo):
            return NotImplemented
        return self.phone == other.phone


@dataclass(slots=True)
class ChatDumpPayload:
    chatInfo: ChatInfo
    messages: list[MessageInfo]
    contacts: list[ContactInfo]


@dataclass(slots=True)
class BackupFetchResult:
    validBackups: list["IPhoneBackup"]
    invalidBackups: list[Path]


class WABackupDelegate(Protocol):
    def didWriteMediaFile(self, fileName: str) -> None:  # pragma: no cover - interface only
        ...
