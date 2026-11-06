"""WebSocket frames exchanged between clients and the chat service."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter

MAX_MESSAGE_CHARS = 4000

CLOSE_UNAUTHORIZED = 4401
CLOSE_FORBIDDEN = 4403
CLOSE_NOT_FOUND = 4404
CLOSE_SUPERSEDED = 4409


class HelloFrame(BaseModel):
    type: Literal["hello"]
    token: str
    conversation_id: str = Field(min_length=1, max_length=64)
    # Highest contiguous sequence number the client holds. Null means "use the server's record".
    last_seen_seq: int | None = Field(default=None, ge=0)


class MessageFrame(BaseModel):
    type: Literal["message"]
    client_message_id: str = Field(min_length=8, max_length=64)  # the idempotency key
    text: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)


class AckFrame(BaseModel):
    type: Literal["ack"]
    up_to_seq: int = Field(ge=0)
    message_id: str = ""


class TypingFrame(BaseModel):
    type: Literal["typing"]
    is_typing: bool


class ResyncFrame(BaseModel):
    type: Literal["resync"]
    after_seq: int = Field(ge=0)


class PingFrame(BaseModel):
    type: Literal["ping"]


ClientFrame = TypeAdapter(
    Annotated[MessageFrame | AckFrame | TypingFrame | ResyncFrame | PingFrame, Field(discriminator="type")]
)
