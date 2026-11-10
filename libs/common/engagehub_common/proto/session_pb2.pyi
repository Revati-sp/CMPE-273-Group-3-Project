from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetMessagesRequest(_message.Message):
    __slots__ = ("tenant_id", "conversation_id", "after_seq", "limit")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_SEQ_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    conversation_id: str
    after_seq: int
    limit: int
    def __init__(self, tenant_id: _Optional[str] = ..., conversation_id: _Optional[str] = ..., after_seq: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class GetMessagesResponse(_message.Message):
    __slots__ = ("envelopes",)
    ENVELOPES_FIELD_NUMBER: _ClassVar[int]
    envelopes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, envelopes: _Optional[_Iterable[str]] = ...) -> None: ...

class RestoreSessionRequest(_message.Message):
    __slots__ = ("tenant_id", "conversation_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    conversation_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., conversation_id: _Optional[str] = ...) -> None: ...

class RestoreSessionResponse(_message.Message):
    __slots__ = ("found", "head_seq")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    HEAD_SEQ_FIELD_NUMBER: _ClassVar[int]
    found: bool
    head_seq: int
    def __init__(self, found: _Optional[bool] = ..., head_seq: _Optional[int] = ...) -> None: ...
