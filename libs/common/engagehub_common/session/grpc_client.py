"""gRPC client for session-service with a timeout and circuit breaker."""

from __future__ import annotations

import grpc

from engagehub_common.auth import service_token
from engagehub_common.events import EventEnvelope
from engagehub_common.proto import session_pb2, session_pb2_grpc
from engagehub_common.resilience import CircuitBreaker, CircuitOpenError
from engagehub_common.session.history import HistoryUnavailable


class GrpcHistoryClient:
    def __init__(self, target: str, jwt_secret: str, service_name: str, timeout: float = 5.0) -> None:
        self._channel = grpc.aio.insecure_channel(target)
        self._stub = session_pb2_grpc.SessionHistoryStub(self._channel)
        self._secret = jwt_secret
        self._service = service_name
        self._timeout = timeout
        self._breaker = CircuitBreaker("session-service-grpc")

    def _metadata(self, tenant_id: str) -> tuple:
        token = service_token(self._secret, self._service, tenant_id)
        return (("authorization", f"Bearer {token}"),)

    async def _call(self, method, request, tenant_id: str):
        try:
            return await self._breaker.call(
                lambda: method(request, timeout=self._timeout, metadata=self._metadata(tenant_id))
            )
        except (grpc.aio.AioRpcError, CircuitOpenError) as exc:
            raise HistoryUnavailable(str(exc)) from exc

    async def messages_after(
        self, tenant_id: str, conversation_id: str, after_seq: int, limit: int = 1000
    ) -> list[EventEnvelope]:
        request = session_pb2.GetMessagesRequest(
            tenant_id=tenant_id, conversation_id=conversation_id, after_seq=after_seq, limit=limit
        )
        response = await self._call(self._stub.GetMessagesAfter, request, tenant_id)
        return [EventEnvelope.model_validate_json(e) for e in response.envelopes]

    async def restore_session(self, tenant_id: str, conversation_id: str) -> bool:
        request = session_pb2.RestoreSessionRequest(tenant_id=tenant_id, conversation_id=conversation_id)
        response = await self._call(self._stub.RestoreSession, request, tenant_id)
        return response.found

    async def close(self) -> None:
        await self._channel.close()
