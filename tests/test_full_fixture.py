from __future__ import annotations

import json

from pywabackupapi import MessageAuthorSource
from pywabackupapi.utils import to_jsonable

from .support import (
    DATA_ROOT,
    FIXTURE_CONTACT_SUMMARY,
    FIXTURE_MESSAGE_TYPE_COUNTS,
    FULL_FIXTURE_IDENTIFIER,
    REACTION_CASES,
    make_full_fixture_backup,
    make_temporary_directory,
    remove_item_if_exists,
    swift_oracle,
)


def test_chat_names_match_swift_reference() -> None:
    wa_backup, _ = make_full_fixture_backup()
    python_chats = wa_backup.getChats()
    swift_chats = swift_oracle("list-chats", DATA_ROOT, FULL_FIXTURE_IDENTIFIER)

    assert [chat.name for chat in python_chats] == [item["name"] for item in swift_chats]


def test_chat_ids_and_message_counts_match_swift_reference() -> None:
    wa_backup, _ = make_full_fixture_backup()
    python_chats = wa_backup.getChats()
    swift_chats = swift_oracle("list-chats", DATA_ROOT, FULL_FIXTURE_IDENTIFIER)

    assert [chat.id for chat in python_chats] == [item["id"] for item in swift_chats]
    assert [chat.numberMessages for chat in python_chats] == [item["numberMessages"] for item in swift_chats]
    assert [chat.contactJid for chat in python_chats] == [item["contactJid"] for item in swift_chats]


def test_chat_messages_match_expected_distribution() -> None:
    wa_backup, _ = make_full_fixture_backup()
    chats = wa_backup.getChats()

    message_type_counts: dict[str, int] = {}
    total_messages = 0

    for chat in chats:
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        assert len(dump.messages) == chat.numberMessages
        total_messages += len(dump.messages)

        for message in dump.messages:
            message_type_counts[message.messageType] = message_type_counts.get(message.messageType, 0) + 1

    assert total_messages == sum(FIXTURE_MESSAGE_TYPE_COUNTS.values())
    assert message_type_counts == FIXTURE_MESSAGE_TYPE_COUNTS


def test_chat_contacts_match_expected_summary() -> None:
    wa_backup, _ = make_full_fixture_backup()
    chats = wa_backup.getChats()
    export_directory = make_temporary_directory("PyWABackupAPI-full-fixture-contacts")

    try:
        all_contacts = set()
        for chat in chats:
            dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=export_directory)
            all_contacts.update(dump.contacts)

        contacts_with_image = [contact for contact in all_contacts if contact.photoFilename is not None]
        contacts_without_image = [contact for contact in all_contacts if contact.photoFilename is None]

        assert len(all_contacts) == FIXTURE_CONTACT_SUMMARY["uniqueContacts"]
        assert len(contacts_with_image) == FIXTURE_CONTACT_SUMMARY["contactsWithImage"]
        assert len(contacts_without_image) == FIXTURE_CONTACT_SUMMARY["contactsWithoutImage"]
        assert len(contacts_with_image) + len(contacts_without_image) == len(all_contacts)
        assert len(contacts_with_image) > 0
    finally:
        remove_item_if_exists(export_directory)


def test_message_content_extraction_chat_44_matches_swift_reference() -> None:
    wa_backup, _ = make_full_fixture_backup()
    python_payload = wa_backup.getChat(chatId=44, directoryToSaveMedia=None)
    swift_payload = swift_oracle("get-chat", DATA_ROOT, FULL_FIXTURE_IDENTIFIER, 44)

    assert to_jsonable(python_payload) == swift_payload


def test_full_fixture_chat_exports_satisfy_structural_invariants() -> None:
    wa_backup, _ = make_full_fixture_backup()
    chats = wa_backup.getChats()

    for chat in chats:
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        message_ids = {message.id for message in dump.messages}
        phones = [contact.phone for contact in dump.contacts]

        assert dump.chatInfo.id == chat.id
        assert dump.chatInfo.contactJid == chat.contactJid
        assert dump.chatInfo.name == chat.name
        assert dump.chatInfo.lastMessageDate == chat.lastMessageDate
        assert dump.chatInfo.chatType == chat.chatType
        assert dump.chatInfo.isArchived == chat.isArchived
        assert chat.numberMessages == len(dump.messages)
        assert dump.chatInfo.numberMessages >= len(dump.messages)
        assert all(message.chatId == chat.id for message in dump.messages)
        assert len(message_ids) == len(dump.messages)
        assert all(phone for phone in phones)
        assert len([contact for contact in dump.contacts if contact.name == "Me"]) == 1

        owner_phone = next(contact.phone for contact in dump.contacts if contact.name == "Me")
        for message in dump.messages:
            if message.replyTo is not None:
                assert message.replyTo > 0
                assert message.replyTo != message.id

            if message.isFromMe:
                assert message.author is not None
                assert message.author.source == MessageAuthorSource.OWNER
                assert message.author.displayName == "Me"
            elif chat.chatType.value == "individual":
                assert message.author is not None
                assert message.author.kind.value == "participant"
                assert message.author.displayName is not None
                if chat.contactJid.endswith("@lid"):
                    assert message.author.phone is not None
                    assert message.author.jid is not None
                    assert not message.author.jid.endswith("@g.us")
                else:
                    assert message.author.phone == chat.contactJid.split("@", 1)[0]
                    assert message.author.jid == chat.contactJid
                    assert message.author.source == MessageAuthorSource.CHAT_SESSION

            if message.author is not None and message.author.displayName is not None:
                for codepoint in (0x200E, 0x200F, 0x202A, 0x202B, 0x202C, 0x202D, 0x202E):
                    assert chr(codepoint) not in message.author.displayName

        if chat.chatType.value == "individual" and chat.contactJid.split("@", 1)[0] != owner_phone:
            assert any(contact.phone == chat.contactJid.split("@", 1)[0] for contact in dump.contacts)


def test_web_validated_reaction_cases_match_fixture_backup() -> None:
    wa_backup, _ = make_full_fixture_backup()
    chats = wa_backup.getChats()

    for case in REACTION_CASES:
        chat = next(chat for chat in chats if chat.name == case["chatName"])
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        message = next(message for message in dump.messages if message.id == case["messageId"])
        searchable_text = "\n".join(
            value for value in [message.message, message.mediaFilename] if value is not None
        )

        assert case["messageSnippet"] in searchable_text
        assert message.reactions is not None
        matching = next(reaction for reaction in message.reactions if reaction.emoji == case["expectedReactionEmoji"])
        assert matching.author.displayName == case["expectedAuthor"]["displayName"]
        assert matching.author.phone == case["expectedAuthor"]["phone"]
