from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .models import MessageAuthor, MessageAuthorKind, MessageAuthorSource, Reaction
from .utils import extracted_phone, is_individual_jid, is_reaction_sender_jid, is_single_emoji


def read_varint(data: bytes, start_index: int) -> tuple[int | None, int]:
    result = 0
    shift = 0
    index = start_index

    while index < len(data):
        byte = data[index]
        index += 1
        result |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return result, index
        shift += 7
        if shift >= 64:
            return None, index

    return None, index


def looks_like_reply_stanza_id(value: str) -> bool:
    if not value or len(value) > 128 or "@" in value:
        return False
    return all(not char.isspace() and ord(char) >= 32 for char in value)


def extract_reply_stanza_id(metadata: bytes) -> str | None:
    index = 0
    while index < len(metadata):
        key, index = read_varint(metadata, index)
        if key is None:
            return None

        field_number = int(key >> 3)
        wire_type = int(key & 0x07)

        if wire_type == 0:
            _, index = read_varint(metadata, index)
            if index > len(metadata):
                return None
        elif wire_type == 1:
            if index + 8 > len(metadata):
                return None
            index += 8
        elif wire_type == 2:
            length, index = read_varint(metadata, index)
            if length is None or index + length > len(metadata):
                return None
            payload = metadata[index : index + length]
            index += length
            if field_number == 5:
                try:
                    stanza_id = payload.decode("utf-8")
                except UnicodeDecodeError:
                    stanza_id = None
                if stanza_id and looks_like_reply_stanza_id(stanza_id):
                    return stanza_id
        elif wire_type == 5:
            if index + 4 > len(metadata):
                return None
            index += 4
        else:
            return None

    return None


@dataclass(slots=True)
class ParsedReaction:
    emoji: str
    senderJid: str


class ReactionParser:
    @staticmethod
    def parse(
        data: bytes,
        senderAuthorResolver: Callable[[str], MessageAuthor | None] | None = None,
    ) -> list[Reaction] | None:
        parsed = ReactionParser._extract_parsed_reactions(data)
        if not parsed:
            return None

        reactions: list[Reaction] = []
        for item in parsed:
            author = ReactionParser._resolve_author(item.senderJid, senderAuthorResolver)
            if author is not None:
                reactions.append(Reaction(emoji=item.emoji, author=author))

        return reactions or None

    @staticmethod
    def _extract_parsed_reactions(data: bytes) -> list[ParsedReaction]:
        reactions: list[ParsedReaction] = []
        ReactionParser._collect_parsed_reactions(data, reactions)
        return reactions

    @staticmethod
    def _collect_parsed_reactions(data: bytes, reactions: list[ParsedReaction]) -> None:
        index = 0
        candidate_jid: str | None = None
        candidate_emoji: str | None = None
        nested_chunks: list[bytes] = []

        while index < len(data):
            raw_tag, index = read_varint(data, index)
            if raw_tag is None:
                return

            field_number = int(raw_tag >> 3)
            wire_type = int(raw_tag & 0x07)

            if wire_type == 0:
                _, index = read_varint(data, index)
                if index > len(data):
                    return
            elif wire_type == 1:
                if index + 8 > len(data):
                    return
                index += 8
            elif wire_type == 2:
                length, index = read_varint(data, index)
                if length is None or index + length > len(data):
                    return

                chunk = data[index : index + length]
                index += length
                nested_chunks.append(chunk)

                try:
                    candidate = chunk.decode("utf-8")
                except UnicodeDecodeError:
                    candidate = None

                if field_number == 2 and candidate and is_reaction_sender_jid(candidate):
                    candidate_jid = candidate.lower()
                elif field_number == 3 and candidate and is_single_emoji(candidate):
                    candidate_emoji = candidate
            elif wire_type == 5:
                if index + 4 > len(data):
                    return
                index += 4
            else:
                return

        if candidate_jid and candidate_emoji:
            reactions.append(ParsedReaction(emoji=candidate_emoji, senderJid=candidate_jid))
            return

        for chunk in nested_chunks:
            ReactionParser._collect_parsed_reactions(chunk, reactions)

    @staticmethod
    def _resolve_author(
        sender_jid: str,
        sender_author_resolver: Callable[[str], MessageAuthor | None] | None,
    ) -> MessageAuthor | None:
        if sender_author_resolver is not None:
            resolved = sender_author_resolver(sender_jid)
            if resolved is not None:
                return resolved

        if is_individual_jid(sender_jid):
            phone = extracted_phone(sender_jid)
            if phone:
                return MessageAuthor(
                    kind=MessageAuthorKind.PARTICIPANT,
                    displayName=None,
                    phone=phone,
                    jid=sender_jid,
                    source=MessageAuthorSource.MESSAGE_JID,
                )

        return None
