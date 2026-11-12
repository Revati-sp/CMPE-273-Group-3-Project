"""The producer's events must match what the Chat Consumer accepts."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "services" / "chat-service"))

import fake_reply_producer  # noqa: E402
from app.replies.schemas import VirtualAgentReplyProduced  # noqa: E402


def test_event_matches_the_consumer_schema():
    args = fake_reply_producer.parse_args(["--conversation-id", "conversation-123", "--content", "Out for delivery"])
    event = VirtualAgentReplyProduced.model_validate(fake_reply_producer.build_event(args, 0))
    assert event.conversation_id == "conversation-123" and event.content == "Out for delivery"
    assert event.sender_type == "virtual_agent" and event.sender_id == "order-tracking-agent"
    assert event.message_id.startswith("message-") and event.timestamp.tzinfo is not None


def test_each_reply_gets_a_unique_id_unless_one_is_fixed():
    args = fake_reply_producer.parse_args(["--conversation-id", "c", "--count", "3"])
    ids = {fake_reply_producer.build_event(args, i)["message_id"] for i in range(3)}
    assert len(ids) == 3
    fixed = fake_reply_producer.parse_args(["--conversation-id", "c", "--message-id", "message-dup-1"])
    assert fake_reply_producer.build_event(fixed, 0)["message_id"] == "message-dup-1"


def test_numbered_content_when_sending_several():
    args = fake_reply_producer.parse_args(["--conversation-id", "c", "--count", "2", "--content", "Hi"])
    assert fake_reply_producer.build_event(args, 1)["content"] == "Hi (2/2)"


def test_invalid_arguments_are_rejected():
    with pytest.raises(SystemExit):
        fake_reply_producer.parse_args(["--conversation-id", "c", "--count", "0"])


def test_unreachable_broker_is_reported_without_a_traceback(capsys):
    code = fake_reply_producer.main(["--conversation-id", "c", "--bootstrap-servers", "127.0.0.1:1"])
    assert code == 1 and "could not publish" in capsys.readouterr().err
