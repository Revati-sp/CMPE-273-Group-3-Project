"""gRPC API for internal callers. The tenant in each request must match the service token."""

from __future__ import annotations

import grpc
from engagehub_common.auth import ROLE_SERVICE, TokenError, verify_token
from engagehub_common.proto import session_pb2, session_pb2_grpc

from app.history import HistoryService


class SessionHistoryServicer(session_pb2_grpc.SessionHistoryServicer):
    def __init__(self, history: HistoryService, jwt_secret: str) -> None:
        self.history = history
        self.jwt_secret = jwt_secret

    async def _authorize(self, context, tenant_id: str) -> None:
        metadata = dict(context.invocation_metadata() or ())
        scheme, _, token = metadata.get("authorization", "").partition(" ")
        try:
            if scheme.lower() != "bearer":
                raise TokenError("missing token")
            principal = verify_token(self.jwt_secret, token)
        except TokenError:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid token")
        if principal.role != ROLE_SERVICE or principal.tenant_id != tenant_id:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "tenant mismatch")

    async def GetMessagesAfter(self, request, context):
        await self._authorize(context, request.tenant_id)
        events = await self.history.messages_after(
            request.tenant_id, request.conversation_id, request.after_seq, request.limit or 1000
        )
        return session_pb2.GetMessagesResponse(envelopes=[e.model_dump_json() for e in events])

    async def RestoreSession(self, request, context):
        await self._authorize(context, request.tenant_id)
        found, head = await self.history.restore_session(request.tenant_id, request.conversation_id)
        return session_pb2.RestoreSessionResponse(found=found, head_seq=head)


async def start_grpc_server(history: HistoryService, jwt_secret: str, address: str) -> tuple[grpc.aio.Server, int]:
    server = grpc.aio.server()
    session_pb2_grpc.add_SessionHistoryServicer_to_server(SessionHistoryServicer(history, jwt_secret), server)
    port = server.add_insecure_port(address)
    await server.start()
    return server, port
