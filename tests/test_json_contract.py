from __future__ import annotations

from datetime import UTC, datetime

from pywabackupapi import (
    ChatDumpPayload,
    ChatInfo,
    ContactInfo,
    MessageAuthor,
    MessageAuthorKind,
    MessageAuthorSource,
    MessageInfo,
    Reaction,
)

from .support import canonical_json


def test_reaction_json_contract() -> None:
    reaction = Reaction(
        emoji="👍",
        author=MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName="~ Alias Ember",
            phone="08185296388",
            jid="404826482604828@lid",
            source=MessageAuthorSource.LID_ACCOUNT,
        ),
    )

    assert canonical_json(reaction) == """{
  "author" : {
    "displayName" : "~ Alias Ember",
    "jid" : "404826482604828@lid",
    "kind" : "participant",
    "phone" : "08185296388",
    "source" : "lidAccount"
  },
  "emoji" : "👍"
}"""


def test_message_author_json_contract() -> None:
    author = MessageAuthor(
        kind=MessageAuthorKind.PARTICIPANT,
        displayName="Alias Atlas",
        phone="08185296386",
        jid="08185296386@s.whatsapp.net",
        source=MessageAuthorSource.CHAT_SESSION,
    )

    assert canonical_json(author) == """{
  "displayName" : "Alias Atlas",
  "jid" : "08185296386@s.whatsapp.net",
  "kind" : "participant",
  "phone" : "08185296386",
  "source" : "chatSession"
}"""


def test_chat_info_json_contract() -> None:
    date = datetime(2024, 4, 3, 11, 24, 16, tzinfo=UTC)
    chat_info = ChatInfo(
        id=44,
        contactJid="08185296386@s.whatsapp.net",
        name="Alias Atlas",
        numberMessages=153,
        lastMessageDate=date,
        isArchived=False,
        photoFilename="chat_44.jpg",
    )

    assert canonical_json(chat_info) == """{
  "chatType" : "individual",
  "contactJid" : "08185296386@s.whatsapp.net",
  "id" : 44,
  "isArchived" : false,
  "lastMessageDate" : "2024-04-03T11:24:16Z",
  "name" : "Alias Atlas",
  "numberMessages" : 153,
  "photoFilename" : "chat_44.jpg"
}"""


def test_message_info_json_contract() -> None:
    date = datetime(2024, 4, 3, 11, 24, 16, tzinfo=UTC)
    message_info = MessageInfo(
        id=125482,
        chatId=44,
        message="Vale, cuando pase por la zona te escribo.",
        date=date,
        isFromMe=False,
        messageType="Text",
        author=MessageAuthor(
            kind=MessageAuthorKind.PARTICIPANT,
            displayName="Alias Atlas",
            phone="08185296386",
            jid="08185296386@s.whatsapp.net",
            source=MessageAuthorSource.CHAT_SESSION,
        ),
        caption="Example caption",
        replyTo=125479,
        mediaFilename="example.jpg",
        reactions=[
            Reaction(
                emoji="👍",
                author=MessageAuthor(
                    kind=MessageAuthorKind.ME,
                    displayName="Me",
                    phone=None,
                    jid=None,
                    source=MessageAuthorSource.OWNER,
                ),
            )
        ],
        seconds=12,
        latitude=38.3456,
        longitude=-0.4815,
    )

    assert canonical_json(message_info) == """{
  "author" : {
    "displayName" : "Alias Atlas",
    "jid" : "08185296386@s.whatsapp.net",
    "kind" : "participant",
    "phone" : "08185296386",
    "source" : "chatSession"
  },
  "caption" : "Example caption",
  "chatId" : 44,
  "date" : "2024-04-03T11:24:16Z",
  "id" : 125482,
  "isFromMe" : false,
  "latitude" : 38.3456,
  "longitude" : -0.4815,
  "mediaFilename" : "example.jpg",
  "message" : "Vale, cuando pase por la zona te escribo.",
  "messageType" : "Text",
  "reactions" : [
    {
      "author" : {
        "displayName" : "Me",
        "kind" : "me",
        "source" : "owner"
      },
      "emoji" : "👍"
    }
  ],
  "replyTo" : 125479,
  "seconds" : 12
}"""


def test_contact_info_json_contract() -> None:
    contact = ContactInfo(name="Alias Atlas", phone="08185296386", photoFilename="08185296386.jpg")
    assert canonical_json(contact) == """{
  "name" : "Alias Atlas",
  "phone" : "08185296386",
  "photoFilename" : "08185296386.jpg"
}"""


def test_chat_dump_payload_json_contract() -> None:
    date = datetime(2024, 4, 3, 11, 24, 16, tzinfo=UTC)
    payload = ChatDumpPayload(
        chatInfo=ChatInfo(
            id=44,
            contactJid="08185296386@s.whatsapp.net",
            name="Alias Atlas",
            numberMessages=1,
            lastMessageDate=date,
            isArchived=False,
            photoFilename="chat_44.jpg",
        ),
        messages=[
            MessageInfo(
                id=125482,
                chatId=44,
                message="Vale, cuando pase por la zona te escribo.",
                date=date,
                isFromMe=False,
                messageType="Text",
                author=MessageAuthor(
                    kind=MessageAuthorKind.PARTICIPANT,
                    displayName="Alias Atlas",
                    phone="08185296386",
                    jid="08185296386@s.whatsapp.net",
                    source=MessageAuthorSource.CHAT_SESSION,
                ),
                replyTo=125479,
                reactions=[
                    Reaction(
                        emoji="👍",
                        author=MessageAuthor(
                            kind=MessageAuthorKind.ME,
                            displayName="Me",
                            phone=None,
                            jid=None,
                            source=MessageAuthorSource.OWNER,
                        ),
                    )
                ],
            )
        ],
        contacts=[ContactInfo(name="Alias Atlas", phone="08185296386", photoFilename="08185296386.jpg")],
    )

    assert canonical_json(payload) == """{
  "chatInfo" : {
    "chatType" : "individual",
    "contactJid" : "08185296386@s.whatsapp.net",
    "id" : 44,
    "isArchived" : false,
    "lastMessageDate" : "2024-04-03T11:24:16Z",
    "name" : "Alias Atlas",
    "numberMessages" : 1,
    "photoFilename" : "chat_44.jpg"
  },
  "contacts" : [
    {
      "name" : "Alias Atlas",
      "phone" : "08185296386",
      "photoFilename" : "08185296386.jpg"
    }
  ],
  "messages" : [
    {
      "author" : {
        "displayName" : "Alias Atlas",
        "jid" : "08185296386@s.whatsapp.net",
        "kind" : "participant",
        "phone" : "08185296386",
        "source" : "chatSession"
      },
      "chatId" : 44,
      "date" : "2024-04-03T11:24:16Z",
      "id" : 125482,
      "isFromMe" : false,
      "message" : "Vale, cuando pase por la zona te escribo.",
      "messageType" : "Text",
      "reactions" : [
        {
          "author" : {
            "displayName" : "Me",
            "kind" : "me",
            "source" : "owner"
          },
          "emoji" : "👍"
        }
      ],
      "replyTo" : 125479
    }
  ]
}"""
