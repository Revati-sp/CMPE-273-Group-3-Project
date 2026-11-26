"""End to end, on the real stack: fake producer -> Kafka -> Chat Consumer -> PostgreSQL ->
Redis cursor -> WebSocket (through the NGINX gateway) -> customer client.

    docker compose up -d --build
    ENGAGEHUB_E2E=1 python -m pytest tests/e2e -v
"""

import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import demo_reply_replay  # noqa: E402

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.environ.get("ENGAGEHUB_E2E") != "1", reason="set ENGAGEHUB_E2E=1 with the stack running"),
]


async def test_full_flow_including_chat_service_restart():
    args = demo_reply_replay.parse_args(["--conversation-id", f"conversation-e2e-{uuid.uuid4().hex[:8]}"])
    result = await demo_reply_replay.run(args)
    assert result["base"] == 0
