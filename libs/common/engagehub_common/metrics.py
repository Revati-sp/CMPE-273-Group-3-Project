"""Prometheus metrics shared across services."""

from prometheus_client import Counter, Gauge, Histogram

EVENTS_PUBLISHED = Counter("engagehub_events_published_total", "Events acknowledged by Kafka", ["topic", "event_type"])
EVENTS_CONSUMED = Counter(
    "engagehub_events_consumed_total", "Events handled by consumers", ["group", "topic", "outcome"]
)
EVENT_RETRIES = Counter("engagehub_event_retries_total", "Handler retries", ["group", "topic"])
CONSUMER_LAG_SECONDS = Histogram(
    "engagehub_event_age_seconds",
    "Time between event creation and consumption",
    ["group"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 30, 120, 600),
)
WS_CONNECTIONS = Gauge("engagehub_ws_connections", "Open WebSocket connections")
SESSION_MIGRATIONS = Counter("engagehub_session_migrations_total", "Sessions moved between pods", ["tenant"])
RECONNECTS = Counter("engagehub_session_reconnects_total", "Client reconnects with a resume point", ["tenant"])
MESSAGES_REPLAYED = Counter("engagehub_messages_replayed_total", "Messages replayed on resume", ["source"])
DUPLICATES_SUPPRESSED = Counter("engagehub_duplicates_suppressed_total", "Duplicates dropped", ["stage"])
PENDING_RECONCILED = Counter("engagehub_pending_reconciled_total", "Reserved messages republished by reconciliation")
SEQUENCE_CONFLICTS = Counter(
    "engagehub_sequence_conflicts_total", "Same sequence number seen with a different message ID"
)
ROUTING_DECISIONS = Counter("engagehub_routing_decisions_total", "Routing outcomes", ["tenant", "agent", "intent"])
ESCALATIONS = Counter("engagehub_escalations_total", "Escalations to a human", ["tenant", "reason"])
HUMAN_QUEUE_DEPTH = Gauge("engagehub_human_queue_depth", "Conversations waiting for a human", ["tenant"])
QUEUE_DEPTH = Gauge("engagehub_queue_depth", "Items waiting in a work queue", ["queue", "tenant"])
# Virtual-agent work. Labels are bounded: eight agent types, a few outcomes, error class names.
AGENT_TASKS_RECEIVED = Counter("engagehub_agent_tasks_received_total", "Agent tasks taken from Kafka", ["agent_type"])
AGENT_TASKS_PROCESSED = Counter(
    "engagehub_agent_tasks_processed_total", "Agent tasks completed", ["agent_type", "outcome"]
)
AGENT_TASKS_FAILED = Counter(
    "engagehub_agent_tasks_failed_total", "Agent task attempts that raised (retried or dead-lettered)",
    ["agent_type", "reason"],
)  # fmt: skip
AGENT_PROCESSING_SECONDS = Histogram(
    "engagehub_agent_processing_seconds",
    "Time to handle one agent task",
    ["agent_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
KAFKA_CONSUMER_LAG = Gauge(
    "engagehub_kafka_consumer_lag",
    "Records between the consumer position and the partition high watermark",
    ["group", "topic", "partition"],
)
CIRCUIT_OPEN = Counter("engagehub_circuit_open_total", "Circuit breaker trips", ["dependency"])
