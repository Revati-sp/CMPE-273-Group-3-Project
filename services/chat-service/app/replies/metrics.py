from prometheus_client import Counter, Gauge, Histogram

CUSTOMER_WS_CONNECTIONS = Gauge("engagehub_customer_ws_connections", "Open customer reply-stream sockets")
CUSTOMER_RECONNECTS = Counter("engagehub_customer_reconnects_total", "Customer reconnections to a conversation")
REPLAYS = Counter("engagehub_customer_replays_total", "Replays performed on connect")
MESSAGES_REPLAYED = Counter("engagehub_customer_messages_replayed_total", "Messages sent during replay")
DUPLICATES_IGNORED = Counter("engagehub_customer_duplicates_ignored_total", "Duplicates dropped", ["stage"])
CURSOR_UPDATES = Counter("engagehub_customer_cursor_updates_total", "Cursor advances after a customer ack")
MESSAGES_APPENDED = Counter("engagehub_chat_messages_appended_total", "Messages appended to the durable log")
DELIVERY_LATENCY = Histogram(
    "engagehub_customer_delivery_latency_seconds",
    "Producer timestamp to live WebSocket send",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
RECOVERY_TIME = Histogram(
    "engagehub_customer_recovery_seconds",
    "Reconnect accepted to replay completed",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
