from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pywabackupapi import (
    BackupDiscoveryStatus,
    ChatDumpPayload,
    ChatType,
    ContactInfo,
    DatabaseConnectionError,
    DirectoryAccessError,
    MediaNotFoundError,
    MessageAuthor,
    MessageAuthorKind,
    MessageAuthorSource,
    Reaction,
    WABackup,
)
from pywabackupapi.api import FileUtils, IPhoneBackup, MediaItem
from pywabackupapi.errors import UnsupportedSchemaError
from pywabackupapi.parsers import ReactionParser
from pywabackupapi.utils import check_table_schema, question_marks

from .support import (
    MediaWriteDelegateSpy,
    add_lid_database,
    canonical_json,
    make_connected_active_group_members_backup,
    make_connected_filtered_chat_backup,
    make_connected_group_backup,
    make_connected_incomplete_location_backup,
    make_connected_individual_lid_backup,
    make_connected_profile_photo_backup,
    make_connected_sample_backup,
    make_sample_backup,
    make_temporary_backup,
    make_temporary_directory,
    reference_date_timestamp,
    remove_item_if_exists,
    sample_reaction_receipt_info,
)


@pytest.fixture
def connected_sample_backup():
    wa_backup, fixture = make_connected_sample_backup()
    try:
        yield wa_backup, fixture
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_backup_discovery_finds_generated_backup() -> None:
    fixture = make_sample_backup()
    try:
        wa_backup = WABackup(backupPath=str(fixture.rootURL))
        backups = wa_backup.getBackups()

        assert len(backups.validBackups) == 1
        assert backups.invalidBackups == []
        assert backups.validBackups[0].identifier == fixture.backup.identifier
        assert backups.validBackups[0].isEncrypted is False
        assert Path(backups.validBackups[0].path).resolve() == fixture.backup.url.resolve()
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_inspect_backups_returns_ready_backup_diagnostics() -> None:
    fixture = make_sample_backup()
    try:
        wa_backup = WABackup(backupPath=str(fixture.rootURL))
        info = wa_backup.inspectBackups()[0]

        assert info.status == BackupDiscoveryStatus.READY
        assert info.isReady is True
        assert info.isEncrypted is False
        assert info.issue is None
        assert info.backup is not None
        assert info.backup.identifier == fixture.backup.identifier
        assert info.backup.isEncrypted is False
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_inspect_backups_returns_encrypted_backup_diagnostics() -> None:
    fixture = make_temporary_backup(name="encrypted-backup", is_encrypted=True, chat_storage_setup=lambda connection: None)
    try:
        wa_backup = WABackup(backupPath=str(fixture.rootURL))
        info = wa_backup.inspectBackups()[0]

        assert info.status == BackupDiscoveryStatus.ENCRYPTED
        assert info.isReady is False
        assert info.isEncrypted is True
        assert info.issue == "Backup is encrypted."
        assert info.backup is not None
        assert info.backup.isEncrypted is True
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_inspect_backups_reports_unknown_encryption_state_when_manifest_plist_is_missing() -> None:
    fixture = make_temporary_backup(
        name="unknown-encryption-backup",
        is_encrypted=None,
        chat_storage_setup=lambda connection: None,
    )
    try:
        wa_backup = WABackup(backupPath=str(fixture.rootURL))
        info = wa_backup.inspectBackups()[0]

        assert info.status == BackupDiscoveryStatus.ENCRYPTION_STATUS_UNAVAILABLE
        assert info.isReady is False
        assert info.isEncrypted is None
        assert info.issue == "Manifest.plist is missing, so encryption status could not be determined."
        assert info.backup is not None
        assert info.backup.isEncrypted is None
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_connect_chat_storage_database() -> None:
    fixture = make_sample_backup()
    try:
        wa_backup = WABackup(backupPath=str(fixture.rootURL))
        wa_backup.connectChatStorageDb(fixture.backup)
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_get_chats_returns_expected_counts(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()

    assert len(chats) == 2
    assert len([chat for chat in chats if not chat.isArchived]) == 2
    assert len([chat for chat in chats if chat.isArchived]) == 0
    assert [chat.id for chat in chats] == [44, 593]
    assert {chat.name for chat in chats} == {"Alias Atlas", "Business Contact"}
    assert chats[0].numberMessages == 3


def test_get_chat_returns_only_supported_public_messages(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chat_dump = wa_backup.getChat(chatId=593, directoryToSaveMedia=None)

    assert [message.id for message in chat_dump.messages] == [200002]
    assert chat_dump.chatInfo.numberMessages == 1


def test_get_chat_returns_chat_dump_payload(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    payload = wa_backup.getChat(chatId=44, directoryToSaveMedia=None)

    assert isinstance(payload, ChatDumpPayload)
    assert payload.chatInfo.id == 44
    assert payload.chatInfo.name == "Alias Atlas"
    assert len(payload.messages) == 3
    assert len(payload.contacts) == 2


def test_known_reply_is_resolved(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chat_dump = wa_backup.getChat(chatId=44, directoryToSaveMedia=None)
    known_reply = next(message for message in chat_dump.messages if message.id == 125482)

    assert known_reply.replyTo == 125479
    assert known_reply.author is not None
    assert known_reply.author.phone == "08185296386"


def test_messages_expose_structured_author(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chat_dump = wa_backup.getChat(chatId=44, directoryToSaveMedia=None)
    incoming = next(message for message in chat_dump.messages if message.id == 125482)
    outgoing = next(message for message in chat_dump.messages if message.id == 125479)

    assert incoming.author is not None
    assert incoming.author.kind == MessageAuthorKind.PARTICIPANT
    assert incoming.author.displayName == "Alias Atlas"
    assert incoming.author.phone == "08185296386"
    assert incoming.author.jid == "08185296386@s.whatsapp.net"
    assert incoming.author.source == MessageAuthorSource.CHAT_SESSION

    assert outgoing.author is not None
    assert outgoing.author.kind == MessageAuthorKind.ME
    assert outgoing.author.displayName == "Me"
    assert outgoing.author.phone == "08185296380"
    assert outgoing.author.jid == "08185296380@s.whatsapp.net"
    assert outgoing.author.source == MessageAuthorSource.OWNER


def test_media_export_notifies_delegate_set_after_connecting(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    delegate = MediaWriteDelegateSpy()
    temporary_directory = make_temporary_directory("PyWABackupAPI-media-export")
    try:
        wa_backup.delegate = delegate
        wa_backup.getChat(chatId=44, directoryToSaveMedia=temporary_directory)

        assert delegate.fileNames
        assert "fea35851-6a2c-45a3-a784-003d25576b45.pdf" in delegate.fileNames
        assert (temporary_directory / "fea35851-6a2c-45a3-a784-003d25576b45.pdf").exists()
    finally:
        remove_item_if_exists(temporary_directory)


def test_get_backups_throws_for_missing_root_directory() -> None:
    wa_backup = WABackup(backupPath="/tmp/PyWABackupAPI/non-existent-path")
    with pytest.raises(DirectoryAccessError):
        wa_backup.getBackups()


def test_get_backups_reports_incomplete_backup_as_invalid() -> None:
    root_url = make_temporary_directory("PyWABackupAPI-invalid-backup")
    try:
        backup_url = root_url / "incomplete-backup"
        backup_url.mkdir(parents=True, exist_ok=True)
        (backup_url / "Info.plist").write_bytes(b"")

        wa_backup = WABackup(backupPath=str(root_url))
        backups = wa_backup.getBackups()

        assert backups.validBackups == []
        assert [path.resolve() for path in backups.invalidBackups] == [backup_url.resolve()]
    finally:
        remove_item_if_exists(root_url)


def test_inspect_backups_reports_incomplete_backup_details() -> None:
    root_url = make_temporary_directory("PyWABackupAPI-invalid-backup-diagnostics")
    try:
        backup_url = root_url / "incomplete-backup"
        backup_url.mkdir(parents=True, exist_ok=True)
        (backup_url / "Info.plist").write_bytes(b"")

        wa_backup = WABackup(backupPath=str(root_url))
        info = wa_backup.inspectBackups()[0]

        assert info.identifier == "incomplete-backup"
        assert info.status == BackupDiscoveryStatus.MISSING_REQUIRED_FILE
        assert info.isReady is False
        assert info.issue == "Manifest.db is missing."
        assert info.backup is None
    finally:
        remove_item_if_exists(root_url)


def test_get_chats_fails_when_database_is_not_connected() -> None:
    wa_backup = WABackup(backupPath="/tmp")
    with pytest.raises(DatabaseConnectionError):
        wa_backup.getChats()


def test_get_chat_fails_when_database_is_not_connected() -> None:
    wa_backup = WABackup(backupPath="/tmp")
    with pytest.raises(DatabaseConnectionError):
        wa_backup.getChat(chatId=44, directoryToSaveMedia=None)


def test_connect_chat_storage_db_rejects_unsupported_schema() -> None:
    fixture = make_temporary_backup(
        chat_storage_setup=lambda connection: connection.execute(
            "CREATE TABLE NotWhatsApp (id INTEGER PRIMARY KEY)"
        )
    )
    try:
        wa_backup = WABackup(backupPath=str(fixture.rootURL))
        with pytest.raises(UnsupportedSchemaError):
            wa_backup.connectChatStorageDb(fixture.backup)
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_fetch_wa_file_hash_throws_when_media_is_missing() -> None:
    fixture = make_temporary_backup(chat_storage_setup=lambda connection: None)
    try:
        with pytest.raises(DatabaseConnectionError) as error_info:
            fixture.backup.fetchWAFileHash("Media/DefinitelyMissing/nope.bin")

        assert isinstance(error_info.value.underlying, MediaNotFoundError)
        assert error_info.value.underlying.path == "Media/DefinitelyMissing/nope.bin"
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_jid_helpers_detect_supported_formats() -> None:
    backup = make_sample_backup()
    try:
        jid = "08185296376@s.whatsapp.net"
        assert jid.split("@", 1)[0] == "08185296376"
        assert jid.split("@", 1)[1] == "s.whatsapp.net"
    finally:
        remove_item_if_exists(backup.rootURL)


def test_question_marks_produces_sql_placeholder_list() -> None:
    assert question_marks(1) == "?"
    assert question_marks(3) == "?, ?, ?"
    assert question_marks(0) == ""


def test_latest_file_returns_highest_timestamp_match() -> None:
    files = [
        ("Media/Profile/123-100.jpg", "hash-old"),
        ("Media/Profile/123-250.jpg", "hash-new"),
        ("Media/Profile/123-150.jpg", "hash-mid"),
    ]

    latest = FileUtils.latest_file("Media/Profile/123", "jpg", files)
    assert latest == ("Media/Profile/123-250.jpg", "hash-new")


def test_check_table_schema_rejects_missing_columns() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("CREATE TABLE Demo (id INTEGER PRIMARY KEY, name TEXT)")
    with pytest.raises(ValueError):
        check_table_schema(connection, "Demo", {"ID", "MISSING"})


def test_reaction_parser_parses_known_fixture_reaction() -> None:
    receipt_info = sample_reaction_receipt_info("😢", sender_phone="08185296386")
    reactions = ReactionParser.parse(receipt_info)

    assert reactions is not None
    assert len(reactions) == 1
    assert reactions[0].emoji == "😢"
    assert reactions[0].author.kind == MessageAuthorKind.PARTICIPANT
    assert reactions[0].author.phone == "08185296386"


def test_reaction_parser_resolves_lid_sender_via_resolver() -> None:
    receipt_info = sample_reaction_receipt_info("👍", sender_jid="404826482604828@lid")
    reactions = ReactionParser.parse(
        receipt_info,
        senderAuthorResolver=lambda jid: MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName="~ Alias Ember",
            phone="08185296388",
            jid="404826482604828@lid",
            source=MessageAuthorSource.LID_ACCOUNT,
        )
        if jid == "404826482604828@lid"
        else None,
    )

    assert reactions is not None
    assert reactions[0].emoji == "👍"
    assert reactions[0].author.displayName == "~ Alias Ember"
    assert reactions[0].author.phone == "08185296388"


def test_reaction_parser_preserves_heart_emoji_variation_selector() -> None:
    receipt_info = sample_reaction_receipt_info("❤️", sender_jid="4048264826043@lid")
    reactions = ReactionParser.parse(
        receipt_info,
        senderAuthorResolver=lambda jid: MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName="~ Alias Flint",
            phone="08185296373",
            jid="4048264826043@lid",
            source=MessageAuthorSource.LID_ACCOUNT,
        )
        if jid == "4048264826043@lid"
        else None,
    )

    assert reactions is not None
    assert reactions[0].emoji == "❤️"
    assert reactions[0].author.displayName == "~ Alias Flint"
    assert reactions[0].author.phone == "08185296373"


def test_media_item_reply_parser_handles_modern_phone_based_metadata() -> None:
    metadata = bytes([0x2A, 0x06]) + b"orig-1" + bytes([0x32, 0x1A]) + b"08185296386@s.whatsapp.net"
    media_item = MediaItem(
        id=1,
        localPath=None,
        metadata=metadata,
        title=None,
        movieDuration=None,
        latitude=None,
        longitude=None,
    )
    assert media_item.extract_reply_stanza_id() == "orig-1"


def test_media_item_reply_parser_handles_modern_lid_based_metadata() -> None:
    metadata = bytes([0x2A, 0x14]) + b"3A05149DCDBC09B2552E" + bytes([0x32, 0x13]) + b"404826482604828@lid"
    media_item = MediaItem(
        id=1,
        localPath=None,
        metadata=metadata,
        title=None,
        movieDuration=None,
        latitude=None,
        longitude=None,
    )
    assert media_item.extract_reply_stanza_id() == "3A05149DCDBC09B2552E"


def test_listed_chat_metadata_matches_chat_export_header(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()

    for chat in chats:
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        assert dump.chatInfo.id == chat.id
        assert dump.chatInfo.contactJid == chat.contactJid
        assert dump.chatInfo.name == chat.name
        assert dump.chatInfo.numberMessages == chat.numberMessages
        assert dump.chatInfo.lastMessageDate == chat.lastMessageDate
        assert dump.chatInfo.chatType == chat.chatType
        assert dump.chatInfo.isArchived == chat.isArchived


def test_chats_are_sorted_by_descending_last_message_date(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()
    sorted_ids = [chat.id for chat in sorted(chats, key=lambda item: item.lastMessageDate, reverse=True)]
    assert [chat.id for chat in chats] == sorted_ids


def test_chat_export_messages_stay_within_requested_chat(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()

    for chat in chats:
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        assert dump.chatInfo.id == chat.id
        assert dump.chatInfo.numberMessages == len(dump.messages)
        assert all(message.chatId == chat.id for message in dump.messages)


def test_reply_targets_always_exist_within_same_chat(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()

    for chat in chats:
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        message_ids = {message.id for message in dump.messages}
        for message in dump.messages:
            if message.replyTo is not None:
                assert message.replyTo in message_ids


def test_individual_incoming_messages_resolve_chat_partner_identity(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()

    for chat in chats:
        if chat.chatType != ChatType.INDIVIDUAL:
            continue
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        expected_phone = chat.contactJid.split("@", 1)[0]

        for message in dump.messages:
            assert message.author is not None
            if message.isFromMe:
                assert message.author.kind == MessageAuthorKind.ME
                assert message.author.displayName == "Me"
                assert message.author.source == MessageAuthorSource.OWNER
            else:
                assert message.author.kind == MessageAuthorKind.PARTICIPANT
                assert message.author.displayName == chat.name
                assert message.author.phone == expected_phone
                assert message.author.jid == chat.contactJid
                assert message.author.source == MessageAuthorSource.CHAT_SESSION


def test_contact_lists_contain_owner_exactly_once_and_use_unique_phones(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    chats = wa_backup.getChats()

    for chat in chats:
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        phones = [contact.phone for contact in dump.contacts]
        me_contacts = [contact for contact in dump.contacts if contact.name == "Me"]

        assert len(me_contacts) == 1
        assert len(set(phones)) == len(phones)
        owner_phone = me_contacts[0].phone
        assert owner_phone

        other_phone = chat.contactJid.split("@", 1)[0]
        if other_phone != owner_phone:
            assert any(contact.phone == other_phone for contact in dump.contacts)


def test_reported_media_files_exist_after_export(connected_sample_backup) -> None:
    wa_backup, _ = connected_sample_backup
    export_directory = make_temporary_directory("PyWABackupAPI-media-invariants")
    try:
        dump = wa_backup.getChat(chatId=44, directoryToSaveMedia=export_directory)
        reported_files = [message.mediaFilename for message in dump.messages if message.mediaFilename is not None]
        assert reported_files
        for file_name in reported_files:
            assert (export_directory / file_name).exists()
    finally:
        remove_item_if_exists(export_directory)


def test_get_chats_excludes_unsupported_session_types() -> None:
    wa_backup, fixture = make_connected_filtered_chat_backup()
    try:
        chats = wa_backup.getChats()
        assert [chat.id for chat in chats] == [800]
        assert chats[0].name == "Visible Chat"
        assert chats[0].chatType == ChatType.INDIVIDUAL
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_profile_photo_export_writes_reported_file() -> None:
    wa_backup, fixture = make_connected_profile_photo_backup()
    export_directory = make_temporary_directory("PyWABackupAPI-photo-invariants")
    try:
        chats = wa_backup.getChats(directoryToSavePhotos=export_directory)
        chat = next(chat for chat in chats if chat.id == 810)
        assert chat.photoFilename == "chat_810.jpg"
        assert (export_directory / chat.photoFilename).exists()
    finally:
        remove_item_if_exists(export_directory)
        remove_item_if_exists(fixture.rootURL)


def test_individual_lid_chats_resolve_partner_phone_through_lid_account() -> None:
    wa_backup, fixture = make_connected_individual_lid_backup()
    try:
        chat = next(chat for chat in wa_backup.getChats() if chat.id == 820)
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        incoming_message = next(message for message in dump.messages if not message.isFromMe)

        assert chat.contactJid == "40482648260486@lid"
        assert incoming_message.author is not None
        assert incoming_message.author.displayName == "Alias Birch"
        assert incoming_message.author.phone == "08185296385"
        assert incoming_message.author.jid == "08185296385@s.whatsapp.net"
        assert incoming_message.author.source == MessageAuthorSource.CHAT_SESSION
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_location_messages_keep_nil_coordinates_when_media_item_lacks_them() -> None:
    wa_backup, fixture = make_connected_incomplete_location_backup()
    try:
        chat = next(chat for chat in wa_backup.getChats() if chat.id == 830)
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        message = next(message for message in dump.messages if message.id == 830001)

        assert message.messageType == "Location"
        assert message.latitude is None
        assert message.longitude is None
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_group_incoming_messages_resolve_member_identity() -> None:
    wa_backup, fixture = make_connected_group_backup()
    try:
        chat = next(chat for chat in wa_backup.getChats() if chat.id == 700)
        dump = wa_backup.getChat(chatId=chat.id, directoryToSaveMedia=None)
        message_by_id = {message.id: message for message in dump.messages}

        assert message_by_id[700001].author.displayName == "Alice Member"
        assert message_by_id[700001].author.phone == "08185296378"
        assert message_by_id[700001].author.jid == "08185296378@s.whatsapp.net"
        assert message_by_id[700001].author.source == MessageAuthorSource.GROUP_MEMBER

        assert message_by_id[700002].author.displayName == "~Bob Push"
        assert message_by_id[700002].author.phone == "08185296379"
        assert message_by_id[700002].author.jid == "08185296379@s.whatsapp.net"
        assert message_by_id[700002].author.source == MessageAuthorSource.PUSH_NAME

        assert message_by_id[700006].author.displayName == "Carol Contact"
        assert message_by_id[700006].author.phone == "08185296370"
        assert message_by_id[700006].author.jid == "08185296370@s.whatsapp.net"
        assert message_by_id[700006].author.source == MessageAuthorSource.CHAT_SESSION

        assert message_by_id[700007].author.displayName == "Alias Cedar"
        assert message_by_id[700007].author.phone == "08185296389"
        assert message_by_id[700007].author.jid == "08185296389@s.whatsapp.net"
        assert message_by_id[700007].author.source == MessageAuthorSource.ADDRESS_BOOK

        assert message_by_id[700008].author.displayName == "~Delta"
        assert message_by_id[700008].author.phone == "08185296371"
        assert message_by_id[700008].author.jid == "08185296371@s.whatsapp.net"
        assert message_by_id[700008].author.source == MessageAuthorSource.PUSH_NAME_PHONE_JID

        assert message_by_id[700009].author.displayName == "~Alias Birch"
        assert message_by_id[700009].author.phone == "08185296385"
        assert message_by_id[700009].author.jid == "08185296385@s.whatsapp.net"
        assert message_by_id[700009].author.source == MessageAuthorSource.LID_ACCOUNT

        assert message_by_id[700010].author.displayName == "~Mystery Lid"
        assert message_by_id[700010].author.phone is None
        assert message_by_id[700010].author.jid == "404826482600@lid"
        assert message_by_id[700010].author.source == MessageAuthorSource.PUSH_NAME

        assert message_by_id[700011].author.displayName == "~Dana Push"
        assert message_by_id[700011].author.phone == "08185296372"
        assert message_by_id[700011].author.jid == "08185296372@s.whatsapp.net"
        assert message_by_id[700011].author.source == MessageAuthorSource.PUSH_NAME

        assert message_by_id[700003].author.displayName == "Me"
        assert message_by_id[700003].author.phone == "08185296380"
        assert message_by_id[700003].author.jid == "08185296380@s.whatsapp.net"
        assert message_by_id[700003].author.source == MessageAuthorSource.OWNER

        assert 700004 not in message_by_id
        assert 700005 not in message_by_id
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_group_contact_list_contains_owner_and_distinct_members() -> None:
    wa_backup, fixture = make_connected_group_backup()
    try:
        dump = wa_backup.getChat(chatId=700, directoryToSaveMedia=None)
        phones = [contact.phone for contact in dump.contacts]

        assert len(set(phones)) == len(dump.contacts)
        assert set(phones) == {
            "08185296380",
            "08185296378",
            "08185296379",
            "08185296370",
            "08185296371",
            "08185296372",
            "08185296385",
            "08185296389",
        }
        assert len([contact for contact in dump.contacts if contact.name == "Me"]) == 1
    finally:
        remove_item_if_exists(fixture.rootURL)


def test_group_contact_list_prefers_active_membership_and_deduplicates_history() -> None:
    wa_backup, fixture = make_connected_active_group_members_backup()
    try:
        dump = wa_backup.getChat(chatId=710, directoryToSaveMedia=None)
        contacts_by_phone = {contact.phone: contact for contact in dump.contacts}

        assert len(dump.contacts) == 4
        assert set(contacts_by_phone) == {
            "08185296380",
            "08185296378",
            "08185296371",
            "08185296390",
        }
        assert contacts_by_phone["08185296380"].name == "Me"
        assert contacts_by_phone["08185296378"].name == "Alice Active"
        assert contacts_by_phone["08185296371"].name == "Linked Delta"
        assert contacts_by_phone["08185296390"].name == "Nova Member"
    finally:
        remove_item_if_exists(fixture.rootURL)
