import pytest
from engagehub_common.events import EventEnvelope, EventType, derive_event_id
from pydantic import ValidationError


def message(**overrides):
    fields = dict(
        event_type=EventType.CUSTOMER_MESSAGE,
        conversation_id="c1",
        tenant_id="t1",
        customer_id="u1",
        message_id="m1",
        sequence_number=1,
        producer_service="test",
        payload={"text": "hi"},
    )
    return EventEnvelope(**{**fields, **overrides})


def test_envelope_round_trips_with_every_required_field():
    decoded = EventEnvelope.from_bytes(message().to_bytes())
    for name in (
        "event_id", "event_type", "conversation_id", "tenant_id", "customer_id", "message_id",
        "sequence_number", "correlation_id", "timestamp", "producer_service", "payload",
    ):  # fmt: skip
        assert getattr(decoded, name) is not None


@pytest.mark.parametrize(
    "overrides",
    [
        {"message_id": None},
        {"sequence_number": None},
        {"sequence_number": 0},
        {"conversation_id": None},
        {"payload": {}},
        {"tenant_id": ""},
    ],
)
def test_invalid_message_events_are_rejected(overrides):
    with pytest.raises(ValidationError):
        message(**overrides)


def test_unknown_fields_are_rejected():
    raw = message().model_dump()
    raw["surprise"] = 1
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(raw)


def test_partition_key_is_the_conversation_id():
    assert message().partition_key == "c1"


def test_derived_event_ids_are_deterministic_per_purpose():
    assert derive_event_id("e1", "task") == derive_event_id("e1", "task")
    assert derive_event_id("e1", "task") != derive_event_id("e1", "reply")
