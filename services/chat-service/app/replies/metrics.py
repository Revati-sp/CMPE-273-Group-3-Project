from prometheus_client import Counter, Gauge, Histogram

# engagehub_messages_replayed_total and engagehub_kafka_consumer_lag live in
# engagehub_common.metrics, shared with the rest of the platform.

ACTIVE_WEBSOCKETS = Gauge("engagehub_active_websocket_connections", "Open WebSocket connections", ["endpoint"])
CUSTOMER_WS_CONNECTIONS = ACTIVE_WEBSOCKETS.labels("/ws/customer")
CUSTOMER_RECONNECTS = Counter("engagehub_customer_reconnects_total", "Customer reconnections to a conversation")
REPLAYS = Counter("engagehub_customer_replays_total", "Replays performed on connect")
DUPLICATES_IGNORED = Counter("engagehub_duplicate_messages_ignored_total", "Duplicate messages dropped", ["stage"])
GAPS_DETECTED = Counter(
    "engagehub_message_gaps_detected_total", "Sequence gaps found and filled before delivery", ["where"]
)
CURSOR_UPDATES = Counter("engagehub_customer_cursor_updates_total", "Cursor advances after a customer ack")
MESSAGES_APPENDED = Counter("engagehub_chat_messages_appended_total", "Messages appended to the durable log")
DELIVERY_LATENCY = Histogram(
    "engagehub_customer_delivery_latency_seconds",
    "Producer timestamp to live WebSocket send",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
RECOVERY_TIME = Histogram(
    "engagehub_recovery_seconds",
    "Reconnect accepted to replay completed",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
SESSION_CLAIMS = Counter("engagehub_session_claims_total", "Session ownership claims", ["kind"])
SESSION_CONFLICTS = Counter(
    "engagehub_session_conflicts_total", "Session writes or events refused because of version", ["source"]
)
STALE_POD_UPDATES_REJECTED = Counter(
    "engagehub_stale_pod_updates_rejected_total", "Writes from a fenced (superseded) connection"
)
HANDOVERS = Counter("engagehub_handovers_total", "Sessions moved to a different pod", ["reason"])
HANDOVERS_FAILED = Counter("engagehub_handovers_failed_total", "Handovers that started but did not complete")
SESSION_RESTORES = Counter("engagehub_session_restores_total", "Session snapshots restored", ["source"])
SESSION_STATE_PUBLISH_FAILURES = Counter(
    "engagehub_session_state_publish_failures_total", "Snapshots not published to chat.session-state"
)
