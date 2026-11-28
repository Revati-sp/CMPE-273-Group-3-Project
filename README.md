# EngageHub: A Fault-Tolerant Hybrid AI–Human Customer Engagement Platform with Reliable Live-Chat Migration

## Implementation status

The platform runs locally with Docker Compose. Kubernetes manifests for horizontal scaling are in `deploy/k8s` (Kustomize). They are rendered and checked by automated tests, but have not been applied to a cluster from this repository, and nothing is deployed to a cloud. See [Horizontal scaling](#horizontal-scaling).

### Implemented

- Docker Compose stack: NGINX API gateway, React frontend, the FastAPI services, a three-broker Kafka cluster, a Redis Cluster, PostgreSQL, Prometheus, Grafana, and Jaeger.
- Authentication, tenants, customers, products, orders, returns, notifications, analytics, and audit logs.
- Eight virtual-agent workers. The eighth handler is the **Supervisor**, which asks a clarifying question and owns the handoff workflow. The other seven are Product Advisor, Order Tracking, Returns and Refunds, Billing, Technical Support, FAQ, and Complaint Management. Replies come from rules and the product and order services. The configured provider is the rules provider.
- Support chat on WebSocket `/ws`. A customer message is published to Kafka topic `chat.messages`. The agent orchestrator consumes that topic, classifies the text, and either assigns one virtual agent or escalates to the human queue. Agent tasks travel on `chat.agent-tasks.<agent>`. Product and order calls from an agent are REST.
- A human-agent queue for two agents: accept, reject, transfer, resolve, notes, and a handoff summary.
- Live session state in Redis. Conversation history is read from session-service over gRPC.
- The customer reply stream on `/ws/customer`: a sequenced PostgreSQL log, a Redis acknowledgement cursor, replay after reconnect, and dead-letter handling. Virtual-agent replies on `agent.responses` are delivered on this socket.
- Chat Service recovery in Docker Compose. Two chat replicas run behind the gateway. Stopping one replica, or stopping `chat-service` and starting it again, reconnects the customer and replays messages they have not acknowledged.
- Session migration between Chat Service pods for the reply stream: a versioned session snapshot in Redis, mirrored to the compacted Kafka topic `chat.session-state`; a handover protocol (`handover_started`, `handover_completed`); and fencing that rejects writes from a pod whose session was claimed by another, including an old pod that survives a network partition. See [Session state, handover and fencing](#session-state-handover-and-fencing).
- Kubernetes manifests (Kustomize) for every service: two replicas of each scaled service, HPAs, PodDisruptionBudgets, probes, graceful shutdown, network policies, plus overlays for KEDA (Kafka-lag autoscaling of the virtual agents), kind (local cluster with single-node Kafka, Redis and PostgreSQL) and a cloud template. They render offline and pass static tests; they have not been applied to a cluster here.
- Virtual-agent replicas share one Kafka consumer group per agent type, so tasks are spread across replicas and a failed replica's tasks are redelivered to the others. An in-process load test with 50 customers and replicated workers demonstrates this; see [Horizontal scaling](#horizontal-scaling).

### Future work

- Running the platform on a Kubernetes cluster (kind or cloud) and publishing load-test results from a real cluster. The manifests, the kind bootstrap script and the scaling demo script are ready but have not been run here.
- Continuous integration. GitHub Actions is named in the technology list below. This repository has no workflow files.
- Load-test results for concurrent users, response latency, and recovery time.
- Publishing customer messages typed on `/ws/customer` to the agent orchestrator. The support-chat socket `/ws` already does this. The reply-stream socket stores those messages in its own log.
- Several virtual agents collaborating on one customer turn. Routing currently chooses one specialist, or a human agent.
- A generative model behind the agent `respond` interface.

## Abstract

EngageHub is a multi-tenant customer-support platform with eight virtual agents and two human agents. Customers search products, track orders, chat in real time, and reach a human agent when the virtual agent escalates.

The services run as separate containers in Docker Compose. Kafka carries chat messages, agent tasks, escalations, notifications, and analytics. Redis Cluster holds the live session: assigned agent, workflow state, acknowledgements, and typing status. PostgreSQL holds customers, tenants, products, orders, conversation history, and the sequenced reply log.

The reliability goal is live-chat migration when a Chat Service process stops. Each message has an ID and a sequence number. Another Chat Service replica restores the session from Redis and replays only the messages the customer has not acknowledged. Prometheus, Grafana, and Jaeger are part of the local stack. What is running, and what is still ahead, is listed under Implementation status.

## Business value

- **24/7 customer support:** The virtual agents handle product questions, order tracking, returns, billing, technical support, FAQs, and complaints. The Supervisor clarifies the request or hands the conversation to a human agent.
- **Faster issue resolution:** The orchestrator sends each support-chat message to one specialist, or to the human queue.
- **Reduced operational cost:** Routine questions stay with a virtual agent. Human agents take sensitive, uncertain, or escalated cases.
- **More than one chat replica:** Docker Compose runs two Chat Service replicas. The gateway reconnects a customer to the replica that is still up. Autoscaling is specified in the Kubernetes manifests, which are not deployed from this repository.
- **Reliable customer conversations:** Session state in Redis, the Kafka log, and replay keep an acknowledged conversation going after a Chat Service restart.
- **Handoff context:** Sentiment and a short summary go with an escalation, so the human agent sees why the conversation was transferred.
- **Multi-tenant separation:** Tenants share the infrastructure. Customer and conversation records stay scoped to a tenant.

**Why session recovery matters:** A dropped conversation can mean a lost sale or an unresolved complaint. EngageHub stores each live conversation as durable state, so a Chat Service restart can continue it.

## Architecture

The diagram is the system that Docker Compose runs. It is not a picture of a Kubernetes cluster or a cloud deployment.

```mermaid
flowchart TB
    Client["Customer web app"]
    HumanUI["Human agent console"]

    subgraph Local["Local stack (Docker Compose)"]
        Gateway["API Gateway<br/>NGINX"]
        Auth["Authentication Service"]
        Tenant["Customer and Tenant Service"]

        subgraph ChatLayer["Chat Service (two replicas)"]
            Chat1["Replica 1"]
            Chat2["Replica 2"]
        end

        Session["Session Service"]
        Orch["Agent Orchestrator"]

        subgraph VA["8 virtual-agent workers"]
            A1["Product Advisor"]
            A2["Order Tracking"]
            A3["Returns and Refunds"]
            A4["Billing"]
            A5["Technical Support"]
            A6["FAQ"]
            A7["Complaint Management"]
            A8["Supervisor"]
        end

        Queue["Human Agent and Queue Service<br/>(2 human agents)"]
        Product["Product Service"]
        Order["Order Service"]
        Notif["Notification Service"]
        Analytics["Analytics Service"]
        Audit["Audit Service"]

        Redis[("Redis Cluster<br/>live session state")]
        Kafka[["Apache Kafka<br/>key = conversation ID"]]
        DB[("PostgreSQL")]
    end

    Client -->|"HTTPS / WebSocket"| Gateway
    HumanUI -->|"WebSocket"| Gateway
    Gateway -->|"REST"| Auth
    Gateway -->|"REST"| Tenant
    Gateway -->|"WebSocket /ws and /ws/customer"| ChatLayer

    ChatLayer -->|"gRPC history"| Session
    Session <-->|"live session state"| Redis
    ChatLayer -.->|"chat.messages from /ws"| Kafka
    Kafka -.->|"customer messages"| Orch
    Orch -.->|"chat.agent-tasks.*"| VA
    VA -->|"REST"| Product
    VA -->|"REST"| Order
    Product --> DB
    Order --> DB
    Tenant --> DB

    Orch -.->|"escalations"| Kafka
    Kafka -.->|"replay and fan-out"| ChatLayer
    Kafka -.->|"escalations"| Queue
    Kafka -.->|"consume"| Notif
    Kafka -.->|"consume"| Analytics
    Kafka -.->|"consume"| Audit
    Queue -->|"WebSocket"| HumanUI
```

**Legend:** Solid arrows are synchronous calls (REST, gRPC, WebSocket). Dotted arrows are Kafka.

**How the running system behaves:**

1. The customer and the human agent connect through the NGINX gateway. Support chat is WebSocket `/ws`. The reply-stream demo is WebSocket `/ws/customer`.
2. A message on `/ws` gets a message ID, conversation ID, sequence number, timestamp, tenant ID, and correlation ID. Chat Service publishes it to `chat.messages` with the conversation ID as the key.
3. Session state for that conversation lives in Redis: assigned agent, workflow state, last acknowledgement, unacknowledged messages, and typing status. History reads go to session-service over gRPC.
4. The orchestrator consumes `chat.messages` from `/ws`, picks one virtual agent, and publishes a task on that agent's topic. If the customer asks for a person, the topic is sensitive, the customer is repeatedly frustrated, or that agent has no worker, it publishes an escalation instead. The eighth handler is the Supervisor, the worker that asks for clarification.
5. If one Chat Service replica stops, the gateway sends the reconnect to the other replica. That replica reads the session from Redis and replays messages after the last acknowledgement. Message IDs and idempotency keys drop duplicates. The same recovery is what `docker compose stop chat-service` and `docker compose start chat-service` demonstrate. Kubernetes restart of Chat Service is specified in `deploy/k8s` and is not a deployment this repository performs.
6. Notification, Analytics, and Audit each consume Kafka in their own consumer group.

**Reply stream, separately:** Messages the customer types on `/ws/customer` are appended to the reply log and are not published to `chat.messages`. They do not reach the orchestrator. Agent replies that virtual agents publish on `agent.responses` do travel the other way and show up on `/ws/customer`. Connecting those customer messages to the orchestrator is future work.

## Technology in this repository

| Technology | Where it is used | Status |
|---|---|---|
| **React** | Customer chat, human-agent console, admin analytics, reply-stream demo | Running in Docker Compose |
| **FastAPI** | Backend services | Running in Docker Compose |
| **REST** | Gateway to services, and virtual agents to product-service and order-service | Running in Docker Compose |
| **gRPC** | History reads against session-service | Running in Docker Compose |
| **WebSockets** | `/ws` support chat and `/ws/customer` reply stream | Running in Docker Compose |
| **Apache Kafka** | Chat messages, agent tasks, escalations, notifications, analytics, audit, reply delivery | Running in Docker Compose |
| **Redis Cluster** | Live session state, reply-stream cursors, idempotency | Running in Docker Compose |
| **PostgreSQL** | Tenants, customers, products, orders, history, reply log, audit | Running in Docker Compose |
| **Docker Compose** | The local stack | This is how the system runs |
| **NGINX** | API gateway container in Compose | Running in Docker Compose |
| **Prometheus, Grafana, Jaeger** | Metrics and traces in the local stack | Running in Docker Compose |
| **Kubernetes** | Manifests for every service (`deploy/k8s`, Kustomize), with KEDA, kind and cloud overlays | Rendered and statically tested; not applied to a cluster |
| **Cloud** | — | No cloud account, cluster, or deployment is in this repository |
| **GitHub Actions** | — | Not in this repository |

### Distributed-systems behavior that is implemented

- Kafka topics, partitions keyed by conversation ID, and separate consumer groups.
- Redis session state that another Chat Service replica can read.
- Replay of messages after the customer's last acknowledgement.
- Idempotent writes using message IDs and idempotency keys.
- Retries with backoff, then a dead-letter topic for events that still fail. A poison agent task is dead-lettered after a bounded number of attempts.
- Circuit breakers and timeouts on calls to product, order, customer, and session-service.
- Two Chat Service replicas in Compose, with reconnect when one stops.
- Versioned session snapshots with optimistic concurrency, lease heartbeats and old-pod fencing; a compacted `chat.session-state` topic as the fallback copy of session state.
- OpenTelemetry traces exported to Jaeger when the local stack is up.

### Distributed-systems behavior that is future work

- Kubernetes self-healing and horizontal pod autoscaling observed on a running cluster. The manifests describe probes, PodDisruptionBudgets, HPAs and KEDA ScaledObjects, but are not applied here.
- A cloud deployment.
- GitHub Actions for test and image builds.
- Load and fault-injection results beyond the Chat Service restart demo and the automated tests. A network-partition script (`scripts/chaos/network_partition_demo.py`) and a Kubernetes rolling-update script (`scripts/k8s/rolling_update_demo.sh`) are included, but no recorded results are published here.
- Multi-agent collaboration on one turn, and a generative model in place of the rules provider.

---

## Component: Customer Reply Stream (WebSocket, Sequenced Log, Redis Cursor, Replay)

This component delivers agent replies to a customer over a WebSocket. Replies come from two sources: real virtual-agent responses on `agent.responses`, and fake replies on `chat.replies` for testing and demos. Either way, it guarantees that a customer who disconnects, or whose Chat Service pod dies, gets every missed message back, in order and without duplicates, when they reconnect.

It lives in `services/chat-service` (module `app/replies/`) next to the existing omnichannel chat flow, and shares its conversation IDs.

### Architecture

```mermaid
flowchart LR
    VA["Virtual agents<br/>virtual-agent-service"]
    AR[["Kafka: agent.responses<br/>key = conversation_id"]]
    P["Fake Reply Producer<br/>scripts/fake_reply_producer.py<br/>or POST /dev/fake-replies"]
    K[["Kafka: chat.replies<br/>key = conversation_id"]]
    C["Chat Consumer<br/>groups chat-replies-consumer,<br/>chat-agent-responses-consumer"]
    DB[("PostgreSQL<br/>chat_messages<br/>chat_conversations")]
    D[["Kafka: chat.replies.delivery<br/>(fan-out to every pod)"]]
    H["ReplyHub on each<br/>Chat Service pod"]
    WS["Customer WebSocket<br/>/ws/customer/{id}"]
    R[("Redis<br/>chat:cursor:{conv}:{customer}")]
    UI["Customer screen<br/>Reply stream demo"]
    DLQ[["chat.replies.dlq"]]

    P --> K --> C
    VA --> AR --> C
    C -->|"assign sequence, append"| DB
    C -->|"sequenced message"| D --> H --> WS --> UI
    C -.->|"permanent failures"| DLQ
    UI -->|"ack"| WS -->|"advance cursor"| R
    H -->|"replay after cursor"| DB
    H -->|"read cursor on connect"| R
```

| Piece | Role |
|---|---|
| **Kafka** (`agent.responses`, `chat.replies`) | Durable, asynchronous transport for replies. The conversation ID is the key, so one conversation's replies stay ordered in one partition. |
| **PostgreSQL** (`chat_messages`) | The durable, sequenced, append-only message log. The source of truth for replay. |
| **Redis** (`chat:cursor:…`) | Fast store for the customer's last acknowledged message, plus active-session markers. Never the only copy of a message. |
| **WebSocket** | Real-time delivery to the customer. |
| **Customer UI** | Reconnects automatically, shows replay status, deduplicates by `message_id`, acknowledges what it displayed. |

### WebSocket flow

URL: `ws://localhost:18080/ws/customer/{conversation_id}` (through the NGINX gateway).

1. The client connects and sends `{"type": "hello", "token": "<JWT>"}` within 5 seconds. Tokens are sent in a frame, not the URL, so they never appear in access logs. The server checks the token and that the conversation belongs to this customer in this tenant.
2. The server sends `connection_ack` with the pod name, the Redis cursor, whether this is a reconnect, and the latest sequence number.
3. The server replays every message after the cursor: `replay_started`, the `chat_message` frames, then `replay_completed`.
4. New messages arrive live as `chat_message` frames.
5. The client sends `{"type": "ack", "sequence_number": n, "message_id": "..."}`. The server advances the Redis cursor and answers `ack_confirmed`.

Close codes: `4401` authentication failed, `4404` conversation not found (or not yours), `1012` server restarting (reconnect at once), `1013` dependency unavailable (retry).

Other client frames: `customer_message` (`client_message_id`, `content`), which is appended to the same log idempotently, and `ping`.

### Message schemas

Event on `chat.replies`:

```json
{
  "event_type": "VirtualAgentReplyProduced",
  "message_id": "message-456",
  "conversation_id": "conversation-123",
  "sender_type": "virtual_agent",
  "sender_id": "order-tracking-agent",
  "sender_name": "Order Tracking Agent",
  "content": "Your order is out for delivery.",
  "timestamp": "2026-11-12T12:00:00Z",
  "correlation_id": "0f7c…"
}
```

The event has no sequence number: the Chat Consumer assigns it. A Kafka offset is never used as the conversation sequence. Unknown fields are rejected.

Frame sent to the customer:

```json
{
  "type": "chat_message",
  "conversation_id": "conversation-123",
  "message_id": "message-456",
  "sequence_number": 5,
  "sender_type": "virtual_agent",
  "sender_id": "product-advisor-agent",
  "sender_name": "Product Advisor Agent",
  "content": "Here are three products that match your request.",
  "timestamp": "2026-11-12T12:00:00Z",
  "correlation_id": "0f7c…",
  "replayed": false
}
```

### Real agent responses (`agent.responses`)

When a virtual agent answers a customer, `virtual-agent-service` sequences the reply into the platform's chat flow as before and also publishes a record to `agent.responses`. Chat Service commits that record to the same PostgreSQL log, where it gets the next sequence number for the conversation and is delivered and replayed exactly like a fake reply.

The record is the platform's standard `EventEnvelope`, built by one shared function (`agent_response_event` in `libs/common/engagehub_common/events.py`) that both sides use:

```json
{
  "event_id": "5c1e…",
  "event_type": "agent_response",
  "conversation_id": "conversation-123",
  "tenant_id": "acme",
  "customer_id": "cust-acme-dana",
  "message_id": "9f0a…",
  "sequence_number": null,
  "correlation_id": "0f7c…",
  "timestamp": "2026-11-12T12:00:00Z",
  "producer_service": "virtual-agent-service",
  "payload": {
    "text": "Order ORD-1001 is delivered.",
    "sender": {"type": "virtual_agent", "id": "order_tracking", "name": "Order Tracking"},
    "metadata": {"order_id": "ORD-1001"},
    "source_message_id": "…"
  }
}
```

- **Idempotent end to end.** The `message_id` is the reply's ID in the platform chat flow, and the event ID is derived from the agent task. A redelivered task republishes an identical record, and Chat Service stores it once.
- **Conversation registration.** A record carries its tenant and customer, so a conversation Chat Service has not seen is registered for that customer. A record pointing at a conversation owned by a different customer or tenant is refused and dead-lettered.
- **Validation.** Records missing the conversation, customer, message ID, text or sender, or with an unknown sender type, go to `chat.replies.dlq` with the reason and source topic. Other event types on the topic are ignored, so the topic can grow.
- **Separate consumer group** (`chat-agent-responses-consumer`), so the fake-producer path behaves exactly as before.

### Kafka topics

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `agent.responses` | 12 | conversation ID | Real virtual-agent responses (`EventEnvelope`, `agent_response`). Consumer group `chat-agent-responses-consumer`. |
| `chat.replies` | 12 | conversation ID | Fake or external replies (`VirtualAgentReplyProduced`). Consumer group `chat-replies-consumer`. |
| `chat.replies.dlq` | 3 | conversation ID | Events from either input topic that can never succeed: schema errors, unknown conversation, a message ID reused in another conversation, a conversation owned by someone else. |
| `chat.replies.delivery` | 12 | conversation ID | Sequenced messages, read by every Chat Service pod (no consumer group) to push to its own sockets. |
| `chat.session-state` | 12 | conversation ID | **Compacted** (`cleanup.policy=compact`). The latest session snapshot per conversation (`SessionStateUpdated`). Every pod reads it from the beginning with no consumer group. |
| `chat.session-state.dlq` | 3 | conversation ID | Session-state records that fail schema validation or whose payload disagrees with the record. |

Topics are created by the `kafka-init` service in Docker Compose (`python -m engagehub_common.messaging.provision`) and by the `kafka-topics` Job in Kubernetes. Both are idempotent.

Consumer behaviour (`libs/common/engagehub_common/messaging/consumer.py`):

- Offsets are committed manually, only after the message is stored, or dead-lettered.
- Handler errors are retried with exponential backoff (4 attempts), then the event goes to `chat.replies.dlq`.
- Database and broker outages are not the event's fault. They are retried until the dependency recovers, holding the partition so order is kept, and are never dead-lettered.
- A crash before the commit means Kafka redelivers the event, and the database constraints turn the repeat into a no-op.

### Database schema

```sql
CREATE TABLE chat_conversations (
  conversation_id VARCHAR(64) PRIMARY KEY,
  tenant_id       VARCHAR(64) NOT NULL,
  customer_id     VARCHAR(128) NOT NULL,
  last_sequence   BIGINT NOT NULL DEFAULT 0,      -- per-conversation counter
  created_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE chat_messages (
  id              BIGSERIAL PRIMARY KEY,
  message_id      VARCHAR(64)  NOT NULL UNIQUE,
  conversation_id VARCHAR(64)  NOT NULL,
  sequence_number BIGINT       NOT NULL,
  sender_type     VARCHAR(32)  NOT NULL,
  sender_id       VARCHAR(128) NOT NULL,
  sender_name     VARCHAR(128) NOT NULL DEFAULT '',
  content         TEXT         NOT NULL,
  correlation_id  VARCHAR(64)  NOT NULL DEFAULT '',
  timestamp       TIMESTAMPTZ  NOT NULL,           -- when the producer created it
  created_at      TIMESTAMPTZ  NOT NULL,           -- when it was appended
  UNIQUE (conversation_id, sequence_number)
);
```

The tables are created by the service at startup in the `eh_chat` database (which it also creates if an existing PostgreSQL volume predates it).

**Sequence assignment.** In one transaction the consumer locks the conversation row (`SELECT … FOR UPDATE`), checks whether the `message_id` already exists, then writes the message with `last_sequence + 1` and updates the counter. Two appends to the same conversation, on any pods, therefore cannot get the same number, and sequences have no gaps because a rolled-back transaction also rolls back the counter. The unique constraints are a second line of defence.

### Redis cursor

| Key | Value | Written when |
|---|---|---|
| `chat:cursor:{conversation_id}:{customer_id}` | `{"last_acknowledged_sequence": 4, "last_message_id": "message-004", "session_version": 12, "updated_at": "…"}` | The customer acknowledges a message |
| `chat:delivered:{conversation_id}:{customer_id}` | highest sequence sent to that customer | A message is sent on a socket (validates REST acks) |
| `chat:session:{conversation_id}:{customer_id}` | the session snapshot (a hash; see below) | Claim on connect, each send, ack, typing change, lease renewal, disconnect |

- The cursor only moves forward. A Lua script compares before writing, so a late or reordered ack cannot rewind it.
- An ack beyond what the socket was actually sent is rejected (`invalid_ack`), as is a `message_id` that does not match the sequence number.
- Keys expire after 7 days without activity (`CURSOR_TTL_SECONDS`).
- The keys contain no `{…}` hash tags, and each operation touches one key, so they work on Redis Cluster.
- **Redis unavailable on reconnect:** the connection still succeeds. The server replays from the durable log, starting after the later of the client's `last_received_sequence` (sent in `hello`) and the last acknowledgement in the latest `chat.session-state` snapshot. It reports `cursor_source` as `client`, `kafka_session_state`, or `unavailable` when neither exists (then the whole history is replayed). The client deduplicates by `message_id`. Acks during the outage get a retryable `cursor_unavailable` error and the client resends them. The REST cursor and ack endpoints return `503`. When Redis returns, the pod closes such sessions with `1013` so the client reconnects and claims the session properly.

### Replay algorithm

On every connection:

1. Read the cursor `c` from Redis (fallbacks above).
2. Register the socket, so live messages arriving from now on are held, not lost.
3. Query `chat_messages` where `sequence_number > c`, ordered by sequence.
4. Send `replay_started {from_sequence: c+1, to_sequence}`, the messages, then `replay_completed {last_sequence}`.
5. Release the held live messages through the normal delivery check.

Every socket tracks `last_sent`, the highest sequence it has been sent. A live message with `seq <= last_sent` is a duplicate and dropped. One with `seq > last_sent + 1` means a gap, which is filled from PostgreSQL before sending. So each socket receives a strictly increasing, gap-free stream, whatever order the fan-out arrives in.

### Session state, handover and fencing

Customers' sockets are spread across Chat Service replicas, and any replica can serve any conversation (no sticky sessions). When a replica crashes, is cut off by a network partition, or is replaced during a rolling update, the customer reconnects through the gateway to another replica, which reconstructs the session and continues.

```text
Customer browser ─► NGINX gateway ─► any Chat Service replica
                                        ├─ Redis: live session snapshot + ack cursor
                                        ├─ PostgreSQL: durable sequenced message log
                                        └─ Kafka: ordered message events + compacted session state
```

**Session snapshot** (`chat:session:{conversation_id}:{customer_id}`, a Redis hash; every script touches one key, so it is Redis Cluster safe):

```json
{
  "conversation_id": "conversation-123", "tenant_id": "acme", "customer_id": "cust-acme-dana",
  "assigned_agent_type": "product_advisor", "assigned_agent_id": "product_advisor",
  "assigned_agent_name": "Product Advisor", "agent_kind": "virtual_agent",
  "workflow_state": "ACTIVE", "last_acknowledged_sequence": 5, "last_acknowledged_message_id": "…",
  "last_sent_sequence": 7, "pending_message_ids": ["message-6", "message-7"],
  "typing_status": "customer_typing", "current_chat_pod": "chat-service-2", "connection_id": "…",
  "connected": true, "lease_expires_at": 1795000000.0, "session_version": 12,
  "last_disconnect_reason": "", "reconnect_count": 3, "handover_count": 1,
  "created_at": "…", "last_updated_at": "2026-11-28T12:00:00Z"
}
```

`workflow_state` and a human agent assignment come from the support-chat session (`/ws`) when the conversation has one (`virtual` → `ACTIVE`, `waiting_human` → `PENDING_HANDOFF`, `human_active` → `HUMAN_ACTIVE`, `resolved` → `RESOLVED`). Otherwise the virtual agent is the sender of the latest agent message. The snapshot expires after 24 hours without activity (`SESSION_SNAPSHOT_TTL_SECONDS`); the cursor and the PostgreSQL log outlive it.

**Version and fencing rules** (`services/chat-service/app/replies/session.py`):

- *Claim.* A pod accepting a connection claims the session, which increments `session_version` and records the pod and connection. The newest connection always wins: the customer has reconnected to that pod. The new version is the connection's fencing token.
- *Optimistic writes.* Every later write (each delivered message, ack, typing change, disconnect) carries the version its connection holds. If the stored version differs, the write is rejected (`stale session update rejected`), the connection is fenced (`old pod fenced`) and closed with `4409`. Example: pod 1 holds version 10, pod 2 claims (version 11), pod 1 writes with 10 and is rejected.
- *Before sending.* A live message is sent only after the version check succeeds, so a fenced pod does not deliver more messages once it can reach Redis.
- *Lease.* Each pod renews a 15 s lease every 5 s (`SESSION_LEASE_SECONDS`, `SESSION_LEASE_RENEW_SECONDS`) with the same version check, without changing the version. A partitioned pod is therefore fenced within one renewal interval of reaching Redis again. A claim also records why the previous owner went away: `owner_unreachable` (lease expired), `takeover_while_owner_alive` (lease still valid: partition, half-open socket or another tab), `server_draining` or `client_closed`.
- *Kafka.* Each snapshot change is published to `chat.session-state`. A pod that sees a newer version for a conversation it is serving fences its own socket. This also fences an old pod that can reach Kafka but has nothing to write to Redis.
- *Restore.* A snapshot from Kafka replaces the Redis copy only if its version is newer (`put_if_newer`), and the session table in each pod ignores records older than the one it holds. A session lost from Redis (for example by a failover that dropped recent writes) is restored from Kafka before the claim, so versions keep increasing.

**Handover protocol.** On a connection whose previous owner was a different pod:

```text
connection_ack     {pod, previous_pod, handover: true, session_version, cursor, workflow_state, agent, typing_status}
handover_started   {old_pod, new_pod, reason, session_version}
replay_started     {from_sequence, to_sequence, count}
chat_message …     (every message after the last acknowledgement, from PostgreSQL)
replay_completed   {last_sequence}
handover_completed {old_pod, new_pod, replayed_from_sequence, replayed_to_sequence, replayed_count,
                    session_version, recovery_ms}
```

The resume point is the last acknowledged message, taken from the first available of: the Redis cursor (or the snapshot copy if it is ahead), the latest `chat.session-state` snapshot, and the client's `last_received_sequence`. Replay always reads PostgreSQL, never the compacted topic, because compaction removes older records.

**What is guaranteed, and under what conditions.** In the scenarios the tests exercise (pod crash, restart, network partition of a pod, rolling drain, Redis outage, Redis losing a snapshot, Kafka redelivery, concurrent claims):

- A message is shown to the customer as delivered only after it is committed to PostgreSQL, and is acknowledged only when the client sends an ack. A reconnect never resumes from before the last acknowledgement Redis or Kafka recorded, so acknowledged messages are not replayed and are not lost.
- Sent but unacknowledged messages are replayed after reconnect.
- Messages reach each socket in sequence order with gaps filled, and the client drops repeats by `message_id`, so no duplicates are displayed.
- Only one connection per conversation can keep writing session state; superseded ones are closed.

These rely on PostgreSQL keeping the log. They do not cover losing PostgreSQL itself, nor a pod that is partitioned from Redis and Kafka while still reaching the customer. That pod cannot be fenced until it reaches one of them, though nothing it does can move the cursor backwards or create sequence numbers.

**Rolling update.** The `preStop` hook calls `POST /internal/drain` (accepted only from inside the pod, and blocked at the gateway), which makes `/ready` fail so the endpoints controller stops routing new sockets to the pod. After the pause Kubernetes sends SIGTERM. The pod sends `server_draining`, records each session as drained, closes the sockets with `1012`, and leaves its consumer groups. Clients reconnect at once and the next pod reports `handover_started` with reason `server_draining`. A pod that is draining also refuses new sockets with `1012`.

**Customer screen.** The **Reply stream demo** tab shows connected, reconnecting, handover started and completed, replaying, the current pod, session version, last received and last acknowledged sequence, messages replayed, duplicates ignored, gaps detected and recovery time. The client reconnects with exponential backoff and the same conversation ID. It sends a ping every 15 s and reconnects if no reply arrives (a half-open socket during a partition), asks the server to `resync` when it sees a gap, and resends an ack that was in flight when the connection dropped.

### Duplicate prevention, layer by layer

| Layer | Mechanism |
|---|---|
| Kafka redelivery or a producer sending twice | `UNIQUE(message_id)`: the second append returns the existing row and assigns no new sequence |
| Two pods sequencing one conversation | Row lock on `chat_conversations` plus `UNIQUE(conversation_id, sequence_number)` |
| Same message ID in another conversation | Rejected and dead-lettered |
| Customer resending a message | `message_id` derived from `(conversation, customer, client_message_id)` |
| Fan-out delivered twice, or replay overlapping live delivery | Per-socket `last_sent` check |
| UI | Messages kept in a map keyed by `message_id` |

### REST endpoints (chat-service, behind the gateway at `/api/chat`)

| Method and path | Description |
|---|---|
| `GET /health`, `GET /ready` | Liveness; readiness (Redis, PostgreSQL and consumer tasks). Not exposed through the gateway. |
| `POST /conversations` | Create a conversation. Optional `conversation_id` such as `conversation-123`. Returns `201`, or `200` if you already own it; `409` if another customer does. |
| `GET /conversations/{id}/messages?after_sequence=4` | Messages after a sequence number, from PostgreSQL. |
| `GET /conversations/{id}/cursor` | The Redis cursor and last delivered sequence. |
| `POST /conversations/{id}/ack` | `{"sequence_number": 4, "message_id": "…"}`. Rejected with `409` if beyond what was delivered. |
| `POST /dev/fake-replies?conversation_id=…` | Demo only (`ENABLE_DEV_ENDPOINTS=true`): publishes fake replies to Kafka. |
| `WS /ws/customer/{id}` | The customer socket described above. |

All endpoints need a customer JWT (`/api/auth/login`). Customers can only reach their own conversations; staff can read conversations in their own tenant.

### Running locally

```bash
cp .env.example .env      # then set JWT_SECRET to 32+ random characters
docker compose up --build -d
```

| URL | What |
|---|---|
| http://localhost:18080 | Frontend (sign in as `dana@acme.example`, password `engagehub-demo`, then open **Reply stream demo**) |
| ws://localhost:18080/ws/customer/conversation-123 | Customer WebSocket |
| http://localhost:18080/api/chat/… | Chat Service REST API |
| localhost:19092-19094 | Kafka brokers, for host tools |
| http://localhost:13000 / :19090 / :16686 | Grafana / Prometheus / Jaeger |

To run only what this component needs (lighter on memory):

```bash
docker compose up --build -d api-gateway frontend auth-service tenant-service chat-service
```

Frontend development server with hot reload (uses the gateway above): `cd frontend && npm install && npm run dev`.

### Fake producer

```bash
pip install -r scripts/requirements.txt

python scripts/fake_reply_producer.py \
  --conversation-id conversation-123 \
  --content "Your order is currently out for delivery."

# several replies in sequence
python scripts/fake_reply_producer.py --conversation-id conversation-123 --count 3 --interval 0.5

# a duplicate event: same message ID three times, stored and shown once
python scripts/fake_reply_producer.py --conversation-id conversation-123 \
  --message-id message-dup-1 --repeat 3 --content "Only once"
```

The conversation must exist first (connect once from the screen, or `POST /api/chat/conversations`). An event for an unknown conversation is sent to `chat.replies.dlq`. Inside the Compose network, add `--bootstrap-servers kafka-1:9092`.

### Failure demonstration

Automated (plays the customer screen's protocol and checks every step):

```bash
python scripts/demo_reply_replay.py                  # all 17 steps, including the Chat Service restart
python scripts/demo_reply_replay.py --skip-restart   # steps 1 to 14
```

By hand, on the screen (**Reply stream demo** tab):

1. Start the stack: `docker compose up --build -d`.
2. Open http://localhost:18080 and sign in as `dana@acme.example`.
3. Enter `conversation-123` and click **Connect**. Untick **Acknowledge automatically**.
4. Publish reply 1: `python scripts/fake_reply_producer.py --conversation-id conversation-123 --content "Reply 1"`.
5. Publish reply 2 the same way.
6. Both appear, with sequence numbers shown under **Developer details**.
7. Click **Ack up to here** on reply 1 only. **Redis cursor** shows `last_acknowledged_sequence: 1`.
8. Click **Disconnect**.
9. Publish replies 3, 4 and 5.
10. `curl -H "Authorization: Bearer $TOKEN" "localhost:18080/api/chat/conversations/conversation-123/messages?after_sequence=2"` shows them stored. The cursor panel shows they were not delivered.
11. Click **Reconnect**.
12. The banner reports the replay. Replies 2 to 5 arrive in order, marked *replayed*.
13. Publish a duplicate: click **Publish duplicate**, or use `--message-id` with the ID of reply 5.
14. No new message appears, and *Duplicates ignored* stays the same.
15. `docker compose stop chat-service`. The screen shows *Reconnecting* with an "unavailable" message.
16. `docker compose start chat-service`.
17. The screen reconnects by itself and replays anything published meanwhile.

To show a pod failover rather than a full restart, stop one of the two replicas (`docker stop engagehub-chat-service-1`): the gateway sends the reconnect to the other replica.

**Network partition of one replica** (automated, checks every step, always restores the network):

```bash
docker compose up -d --build
python scripts/chaos/network_partition_demo.py               # detach replica 1 from the network
python scripts/chaos/network_partition_demo.py --mode stop   # or stop it instead
```

It connects a customer to replica 1, publishes 5 replies and acknowledges 2, then runs `docker network disconnect <network> <replica-1>`. The replica keeps running, which is what makes it a partition rather than a crash. The script publishes 3 replies during the failure and reconnects through the gateway, which now reaches replica 2. It checks for `handover_started`, the acknowledged position (2), replies 3 to 8 replayed in order, a duplicate event showing nothing, and live chat continuing. It then runs `docker network connect`, after which replica 1 logs `old pod fenced` when its lease renewal is rejected. The equivalent manual commands:

```bash
docker compose ps chat-service                               # find the replica names
docker network disconnect engagehub_default engagehub-chat-service-1
# … customer reconnects and is handed over to engagehub-chat-service-2 …
docker network connect engagehub_default engagehub-chat-service-1
docker logs engagehub-chat-service-1 2>&1 | grep -E "old pod fenced|stale session update rejected"
```

The same flow is in `tests/e2e/test_reply_replay_e2e.py::test_network_partition_of_one_replica_migrates_the_session` (`ENGAGEHUB_E2E=1`).

### Kubernetes

Chat Service's manifests are in `deploy/k8s/chat-service`, part of the platform-wide layout described in [Horizontal scaling](#horizontal-scaling). Applying them requires a cluster you operate; this repository does not deploy to a cloud.

```bash
cp deploy/k8s/base/secret.env.example deploy/k8s/base/secret.env   # set real values
kubectl kustomize deploy/k8s          # render and inspect
kubectl apply -k deploy/k8s
```

Chat Service has a ConfigMap of its own settings, a Deployment with 2 replicas, a Service, a PodDisruptionBudget and a HorizontalPodAutoscaler (2 to 6 pods). The namespace, shared configuration, secret, topic-creation Job and the single Ingress (to the gateway) are shared with the other services.

The Deployment uses readiness (`/ready`), liveness (`/health`) and startup probes, CPU and memory requests and limits, a non-root read-only container, a `RollingUpdate` strategy with `maxUnavailable: 0` and `maxSurge: 1`, a PodDisruptionBudget, `terminationGracePeriodSeconds: 45`, and graceful shutdown: the `preStop` hook drains the pod (readiness fails), waits for endpoints to drop it, then on SIGTERM the pod sends `server_draining`, closes customer sockets with code `1012`, finishes its in-flight Kafka records and leaves the consumer group. The Service has no session affinity: any pod can resume any conversation from Redis, Kafka and PostgreSQL. Kafka, Redis Cluster and PostgreSQL are expected to be installed separately (for example through operators); their addresses are in the ConfigMap.

Rolling-update demonstration (needs a cluster with the infrastructure and these manifests applied; not run in this repository):

```bash
JWT_SECRET=<value from secret.env> scripts/k8s/rolling_update_demo.sh
```

It holds a customer session open through `kubectl port-forward svc/chat-service`, publishes a reply every second, runs `kubectl rollout restart deployment/chat-service`, and reports the pods the session moved through, each handover, and whether every stored message was received once and in order. It then prints the pods' `draining`, `handover completed` and `old pod fenced` log lines.

### Observability

Structured JSON log events (each with `correlation_id`, `conversation_id` and, where relevant, `message_id`, `sequence_number` and `session_version`): `websocket connected`, `websocket disconnected`, `customer reconnecting`, `conversation claimed by new pod`, `handover started`, `handover completed`, `handover failed`, `session snapshot written`, `session snapshot restored`, `stale session update rejected`, `stale session event ignored`, `old pod fenced`, `cursor updated`, `replay started`, `replay completed`, `gap detected`, `duplicate message ignored`, `kafka event redelivered`, `message received from kafka`, `message appended to durable log`, `handler failed; retrying`, `dependency unavailable; retrying until it recovers`, `event dead-lettered`, `draining: readiness now fails`, `recovery completed`. The partition script prints `network partition simulated`.

Prometheus metrics (on `/metrics`):

| Metric | Meaning |
|---|---|
| `engagehub_active_websocket_connections{endpoint}` | Open sockets on `/ws` and `/ws/customer` |
| `engagehub_session_claims_total{kind}` | Claims: `new`, `reconnect`, `handover` |
| `engagehub_session_conflicts_total{source}` | Writes or records refused on version: `redis_cas`, `lease`, `kafka_event`, `stale_kafka_event` |
| `engagehub_stale_pod_updates_rejected_total` | Writes from a fenced connection |
| `engagehub_handovers_total{reason}`, `engagehub_handovers_failed_total` | Sessions moved to another pod, and handovers that did not complete |
| `engagehub_session_restores_total{source}` | Snapshots restored from `redis` or `kafka` |
| `engagehub_messages_replayed_total{source}` | Messages replayed (`postgres` for this stream) |
| `engagehub_duplicate_messages_ignored_total{stage}` | Duplicates dropped by the consumer or at delivery |
| `engagehub_message_gaps_detected_total{where}` | Gaps filled before delivery (`server`) or reported by a client (`client`) |
| `engagehub_recovery_seconds` | Reconnect accepted to replay completed |
| `engagehub_customer_delivery_latency_seconds` | Producer timestamp to live WebSocket send |
| `engagehub_kafka_consumer_lag{group,topic,partition}` | Records behind the high watermark, from each consumer |
| `engagehub_customer_reconnects_total`, `engagehub_customer_replays_total`, `engagehub_customer_cursor_updates_total`, `engagehub_chat_messages_appended_total`, `engagehub_session_state_publish_failures_total` | Reconnects, replays, cursor updates, appends, unpublished snapshots |
| `engagehub_events_consumed_total{outcome="dead_letter"}`, `engagehub_event_retries_total` | Failed and retried Kafka events |
| `kafka_consumergroup_lag` | Consumer lag (from kafka-exporter) |

Grafana dashboard **EngageHub Session Recovery** (`deploy/grafana/dashboards/engagehub-session-recovery.json`) has panels for active sockets, handovers, session conflicts, stale writes rejected, failed handovers, replays, duplicates, gaps, recovery time (p50, p95), Kafka consumer lag, delivery latency, claims, and Chat Service pods up.

Tracing: the gateway forwards `traceparent`. Chat Service adds spans `chat.session.open`, `redis.session.claim`, `postgres.replay` and `kafka.session_state.publish`, and puts the trace context on its Kafka records. A trace therefore continues from the pod that claimed a session to the old pod that consumes the claim and fences its socket.

### Tests

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements/base.txt -r requirements/dev.txt -e libs/common
cd services/chat-service && ../../.venv/bin/python -m pytest tests -v        # reply stream, failures, k8s manifests
TEST_POSTGRES_URL=postgresql+asyncpg://user:pass@localhost:5432/eh_chat_test \
  ../../.venv/bin/python -m pytest tests -v                                   # same suite on real PostgreSQL
cd ../../frontend && npm install && npm test                                  # client reconnect, replay, dedupe
cd .. && .venv/bin/python -m pytest scripts/tests -v                          # fake producer
ENGAGEHUB_E2E=1 .venv/bin/python -m pytest tests/e2e -v                       # needs the Compose stack running
```

| Requirement | Test |
|---|---|
| Spec's replay scenario (steps 1 to 13) | `test_replay_after_reconnect_matches_the_specified_scenario` |
| Connection, auth, disconnect detection | `test_connection_ack_*`, `test_unauthorized_connections_are_closed`, `test_disconnect_is_detected_*` |
| Acks and cursor | `test_ack_cannot_exceed_delivered_or_mismatch_or_rewind`, `test_rest_ack_is_validated_*` |
| Ordering | `test_messages_arrive_in_publish_order_*`, `test_live_fanout_fills_gaps_and_drops_repeats` |
| Duplicate Kafka events and message IDs | `test_duplicate_kafka_events_*`, `test_message_id_reused_in_another_conversation_*` |
| Concurrent messages for one conversation | `test_concurrent_appends_*` (SQLite) and `test_reply_log_postgres.py` (real row locks) |
| Kafka consumer restart | `test_consumer_restart_after_lost_commit_does_not_duplicate` |
| Redis failure | `test_redis_outage_falls_back_to_the_durable_log` |
| Database failure | `test_database_outage_is_retried_*`, `test_database_outage_rejects_new_sockets_*` |
| Chat Service restart | `test_chat_service_restart_keeps_acknowledged_state_and_replays`, `test_shutdown_closes_customer_sockets_*` |
| Automatic reconnect, UI deduplication | `frontend/src/replay/ReplayClient.test.ts` |
| Real agent responses | `test_real_agent_response_is_committed_and_delivered`, `test_real_and_fake_replies_share_one_sequence_*`, `test_redelivered_agent_response_*`, `test_agent_response_*` (chat-service); `test_reply_is_published_to_agent_responses_*`, `test_redelivered_task_republishes_*`, `test_failed_agent_response_publish_*` (virtual-agent-service) |
| Session snapshot create, version, conflict, stale pod rejection | `tests/test_session_migration.py`: `test_claim_creates_*`, `test_writes_bump_the_version_*`, `test_spec_example_old_pod_write_*`, `test_put_if_newer_*`, `test_concurrent_claims_leave_exactly_one_owner` |
| Kafka compacted session state | `test_state_table_keeps_the_newest_version_*`, `test_invalid_session_state_events_are_dead_lettered`, `test_lost_redis_snapshot_is_restored_from_kafka_*`, `test_redis_outage_resumes_from_the_kafka_snapshot`, `libs/common/tests/test_topics.py` |
| Handover after crash, partition, drain | `test_handover_after_pod_crash_*`, `test_partition_old_pod_is_fenced_*`, `test_lease_renewal_fences_*`, `test_newer_session_state_event_on_kafka_fences_*`, `test_rolling_update_drain_*`, `test_owner_with_expired_lease_*` |
| Agent, workflow and typing restored | `test_agent_assignment_workflow_and_typing_survive_the_handover`, `test_human_assignment_from_support_chat_is_restored` |
| Acknowledged-message preservation | `test_acknowledged_messages_are_not_replayed_and_unacknowledged_ones_are` |
| Recovery metrics | `test_recovery_metrics_are_recorded`, `test_gap_in_live_fanout_*`, `libs/common/tests/test_consumer_runner.py::test_consumer_lag_metric_*` |
| API: typing, resync, session view, drain | `tests/test_session_api.py` |
| Rolling-update configuration | `test_k8s_manifests.py::test_rolling_update_drains_before_termination`, `test_lease_settings_are_configured` |
| Message log tenant and customer columns, migration | `test_reply_log_postgres.py::test_messages_carry_tenant_and_customer`, `test_migration_adds_and_backfills_columns_on_an_old_table` |
| Client handover, heartbeat, gap resync, ack resend | `frontend/src/replay/ReplayClient.test.ts` (`session migration` block) |
| End to end | `tests/e2e/test_reply_replay_e2e.py`: the replay demo, and the network-partition flow (`scripts/chaos/network_partition_demo.py`) |

Terminal output from the last run:

```text
services/chat-service           105 passed, 4 skipped  (SQLite; the 4 skipped need TEST_POSTGRES_URL)
services/chat-service           109 passed             (TEST_POSTGRES_URL set, PostgreSQL 16)
libs/common                     55 passed
services/virtual-agent-service  26 passed
services/api-gateway            7 passed
frontend                        45 passed (ReplayClient 23, ChatConnection 22)
tests + scripts/tests           7 passed, 2 skipped    (the 2 skipped are the e2e tests: ENGAGEHUB_E2E=1)
```

The e2e tests, the network-partition script and the Kubernetes rolling-update script were not run for these results: the development machine had too little free disk to start the Docker stack.

### Design decisions

- **Sequence in PostgreSQL, not Kafka offsets or Redis.** Offsets are per partition and change if topics are repartitioned; Redis can lose recent writes on failover. The database transaction makes the number durable together with the message.
- **Kafka fan-out for live delivery.** Only one pod's consumer sequences a reply, but the customer's socket may be on another pod. Each pod reads `chat.replies.delivery` and serves its own sockets. If that publish fails, the consumer retries, and any message missed live is recovered by the gap check or the next replay, since PostgreSQL has it.
- **Outages retry, bad events dead-letter.** Sending events to the DLQ because the database was briefly down would lose them from the log.
- **No sticky sessions.** Every pod can serve every conversation, which is what makes pod failure recoverable.
- **The cursor tracks acknowledgment, not delivery.** A message sent but not acknowledged is sent again after a reconnect; the client drops the repeat by `message_id`. Delivery is at least once; display is free of duplicates.
- **Newest connection wins, older ones are fenced.** A reconnecting customer is the best evidence of where the session lives now, so a claim always succeeds and bumps the version. Rejecting older writes is cheap (one Lua compare per write) and needs no coordinator.
- **Compacted topic for state, log for history.** `chat.session-state` keeps only the latest snapshot per conversation, which is right for state and wrong for history. Replay reads PostgreSQL.
- **Session snapshot writes do not wait for Kafka.** Redis holds the live copy. A failed publish is counted and superseded by the next change, so a Kafka outage does not stall chat.

### Known limitations

- Replay sends at most 5,000 messages per connection; longer histories would need paging.
- The cursor is per customer, not per device: two devices of one customer share it.
- If Redis is down, the client sends no `last_received_sequence`, and no `chat.session-state` record exists for the conversation, the whole history is replayed (and deduplicated by the client).
- The `chat.session-state` copy can lag Redis: per-message progress (`last_sent_sequence`) is published with the next ack or state change, not on every send, and lease renewals are not published. Acknowledgements are published as they happen.
- Each pod keeps every conversation's latest snapshot from `chat.session-state` in memory. A pod that has just started may not have read the whole topic when a reconnect arrives; it then relies on Redis, or on the client's position if Redis is also down.
- A pod partitioned from Redis and Kafka while still connected to the customer cannot be fenced until it reaches one of them. It cannot move the cursor backwards or assign sequence numbers in the meantime.
- Two browser tabs on one conversation fence each other: the most recent connection wins and the other is closed with `4409`.
- REST acks (`POST /conversations/{id}/ack`) update the cursor only, not the session snapshot. The resume point takes the later of the two.
- During a rolling update from a version without session snapshots, an older pod can overwrite the snapshot key with its old connection marker. The next claim converts it back.
- Every pod reads every message on `chat.replies.delivery`. That is simple and fine for this scale; very large deployments would route by conversation instead.
- Virtual agents publish each reply to both `chat.messages` (the support-chat flow) and `agent.responses` (this log), so a reply exists in two logs with the same `message_id` and independent sequence numbers.
- Customer messages on `/ws` are published to `chat.messages`, and the agent orchestrator consumes that topic. Customer messages on `/ws/customer` are stored in this log only. Forwarding them to the orchestrator is future work, listed under Implementation status.
- The Kubernetes manifests assume Kafka, Redis Cluster, and PostgreSQL are already installed (or use the single-node ones in `overlays/kind`). This repository does not deploy the platform to a cloud.

---

## Horizontal scaling

Many customers at once: Kafka carries the work, several replicas of each virtual agent take it from shared consumer groups, and several Chat Service replicas hold the customers' WebSockets. Any chat replica can serve any conversation (no sticky sessions), so replicas can be added or removed at any time.

```text
customers ─► gateway (2+) ─► chat-service (2–6) ─► chat.messages ─► agent-orchestrator (2–6)
                 │                 ▲                                    │
                 │                 │ replies (keyed by conversation)    ▼
                 │                 └──── va-<agent> (2–10 each) ◄── chat.agent-tasks.<agent>
                 └─► human-agent (2–4), notification (2–6), analytics (2–6)
```

### What was tested, and where

| Claim | Evidence | Where it ran |
|---|---|---|
| Replicas of one agent share a consumer group; each task is processed by one replica; work is spread; a conversation stays in order | `libs/common/tests/test_consumer_groups.py`, `services/virtual-agent-service/tests/test_scaling_load.py` | In process (real worker and consumer code, in-memory Kafka that splits partitions between group members) |
| A crashed worker loses no task | Same tests: the crashed replica's uncommitted tasks are redelivered to the others and answered once | In process |
| 50 concurrent customers handled by replicated workers | `test_scaling_load.py` (report below) | In process |
| Chat failover, replay, session recovery, ordering, duplicates | `services/chat-service/tests/` (handover, fencing, replay suites), unchanged and passing | In process; PostgreSQL 16 for the database tests |
| Manifests: replicas, probes, HPAs, PDBs, KEDA, network policy, no session affinity, per-service databases | `tests/test_k8s_scaling.py`, `services/chat-service/tests/test_k8s_manifests.py` (render with `kubectl kustomize`, then check) | Offline render, no cluster |
| Load test against a running stack | `scripts/load/load_test.py`, `scripts/load/k6-chat.js` (smoke-tested in `scripts/tests/test_load_test.py` against a fake server) | **Not run against EngageHub here** |
| Autoscaling on a cluster, KEDA, kind, cloud | `scripts/k8s/kind_up.sh`, `scripts/k8s/scaling_demo.sh`, `deploy/k8s/overlays/*` | **Not run here** |

The machine used for this work had under 1 GB of free disk, so Docker, kind and the real-stack load test could not be started. The cloud overlay is a template; nothing has been deployed to a cloud.

### Replica strategy

| Service | Initial replicas | Autoscaling | Why it can scale |
|---|---|---|---|
| api-gateway | 2 | HPA 2–6 (CPU, memory) | Stateless NGINX; resolves upstreams per request |
| chat-service | 2 | HPA 2–6 | Session snapshot and cursor in Redis, log in PostgreSQL, session state on Kafka; handover and fencing between pods |
| agent-orchestrator | 2 | HPA 2–6 | Shared group `agent-orchestrator` on `chat.messages` |
| virtual agents (8 Deployments) | 2 each | HPA 2–10, or KEDA on Kafka lag | Shared group `va-<agent_type>` per agent |
| human-agent-service | 2 | HPA 2–4 | Shared group `human-agent-queue`; queue changes take a per-tenant PostgreSQL advisory lock |
| notification-service | 2 | HPA 2–6 | Shared group; the event ID is the primary key |
| analytics-service | 2 | HPA 2–6 | Shared group; counters and the processed-event row in one transaction |
| supporting services (auth, tenant, customer, product, order, session, audit, frontend) | 1–2 | none | Needed by the scaled tier; not the subject of this work |

All Deployments use rolling updates with `maxUnavailable: 0`, readiness, liveness and startup probes, CPU and memory requests and limits, a `preStop` pause (Chat Service also drains) and a termination grace period. Each scaled Deployment has a PodDisruptionBudget (`minAvailable: 1`). HPAs scale up by at most 2 pods a minute and down by 1 pod every 2 minutes after a 5-minute window: removing a chat pod forces reconnects, removing a worker forces a rebalance.

### Kafka consumer-group strategy

| Consumer | Group | Topics | Partitions |
|---|---|---|---|
| Virtual agent `<type>` (all replicas) | `va-<type>`, for example `va-product_advisor` | `chat.agent-tasks.<type>` | 12 |
| Supervisor (all replicas) | `va-supervisor-escalations` | `chat.escalations` | 6 |
| Agent orchestrator | `agent-orchestrator` | `chat.messages` | 12 |
| Human agent service | `human-agent-queue` | `chat.escalations` | 6 |
| Notification / analytics / audit | `notification-service` / `analytics-service` / `audit-service` | their topics | 6 |
| Chat Service reply log | `chat-replies-consumer`, `chat-agent-responses-consumer` | `chat.replies`, `agent.responses` | 12 |
| Chat Service fan-out and session table | no group (every pod reads everything) | `chat.messages`, `chat.replies.delivery`, `chat.session-state`, … | — |

- Every message that needs ordering is keyed by `conversation_id`, so one conversation is on one partition and handled by one replica at a time, in order.
- Kafka gives each partition to one member of a group: a task is processed by one replica. A group uses at most as many replicas as the topic has partitions, so the agent-task topics have 12 partitions (above the autoscaler maximum of 10). Topics created before this change keep their partition count; add partitions with `kafka-topics.sh --alter --partitions 12` only when the topic is empty, because adding partitions changes which partition a key maps to.
- The group and topic names are unchanged from before. The spec's examples (`product-advisor-workers`, `chat.agent-tasks.product`) were not adopted, because renaming a live consumer group would make it re-read every retained task.
- Offsets are committed after a task is handled. If a replica dies, its partitions move to the survivors, which resume from the last commit, so tasks in flight are processed again. The reply's idempotency key turns that into one reply. In the load test this happened to 20–26 tasks per run, with no duplicate replies and nothing lost.

### Autoscaling: HPA and KEDA

- **Default (`kubectl apply -k deploy/k8s`):** HPAs on CPU (70%) and memory (80%) for every scaled service. This needs metrics-server.
- **KEDA (`kubectl apply -k deploy/k8s/overlays/keda`):** for the virtual agents, CPU is a weak signal (most time is spent waiting on other services). The overlay replaces their HPAs with KEDA ScaledObjects that read the real lag of `va-<type>` on `chat.agent-tasks.<type>`: about one replica per 20 pending tasks, 2 to 10 replicas, plus a CPU trigger. KEDA creates its own HPA, so the overlay deletes the CPU HPA for those Deployments. Other services keep their HPAs.
- No custom metric is invented. The in-process lag gauge `engagehub_kafka_consumer_lag` (from each consumer) and kafka-exporter's `kafka_consumergroup_lag` are for dashboards; KEDA reads lag from Kafka itself.

### Kubernetes layout

```text
deploy/k8s/
  kustomization.yaml          # kubectl apply -k deploy/k8s
  platform/                   # everything in namespace engagehub; the overlays build on this
  base/                       # namespace, engagehub-config, engagehub-secrets (from secret.env),
                              # service account, network policies, kafka-topics Job
  api-gateway/  chat-service/  agent-orchestrator/  human-agent-service/
  notification-service/  analytics-service/
  virtual-agent-service/      # 8 Deployments + HPAs + PDBs, one per agent type
  supporting-services/        # auth, tenant, customer, product, order, session, audit, frontend
  overlays/keda/              # Kafka-lag ScaledObjects for the agents
  overlays/kind/              # + single-node Kafka (KRaft), Redis and PostgreSQL for a laptop
  overlays/cloud/             # template: registry images, TLS Ingress, managed data stores, KEDA
```

- Kafka, Redis Cluster and PostgreSQL are external dependencies. Set their addresses in `base/configmap.yaml` (or use `overlays/kind`).
- Each service's `DATABASE_URL` is assembled in the Pod from the shared secret (`$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST)`) and its own database name. No URL is stored in a ConfigMap.
- In Kubernetes the gateway gets `UPSTREAM_SUFFIX=.engagehub.svc.cluster.local`, because nginx's resolver does not apply DNS search domains. In Compose the suffix is empty.
- Network policy: deny ingress by default, allow traffic within the namespace, allow the gateway from anywhere (it is behind the Ingress) and port 8000 from a `monitoring` namespace (Prometheus).
- Cloud readiness (template, not deployed): environment-based service URLs, an Ingress with TLS via cert-manager, registry image names, a Secret template, a service account without an API token, probes, rolling updates, PDBs and autoscaling.

### Local setup

Docker Compose stays the local development environment, and is unchanged: `docker compose up --build -d`. To scale a worker in Compose (replicas join the same consumer group): `docker compose up -d --scale va-faq=3`.

Local Kubernetes with kind (needs about 8 GB of memory for Docker and 15 GB of disk; not run here):

```bash
scripts/k8s/kind_up.sh            # kind cluster, images, metrics-server, overlays/kind
KEDA=1 scripts/k8s/kind_up.sh     # the same with KEDA and Kafka-lag scaling for the agents
kubectl get pods -n engagehub
kubectl get hpa -n engagehub
kubectl -n engagehub port-forward svc/api-gateway 18080:8080
```

### Load testing

Against a running stack (Compose or Kubernetes), through the gateway:

```bash
pip install -r scripts/requirements.txt
python scripts/load/load_test.py --customers 50 --duration 120 --prometheus http://localhost:19090
python scripts/load/load_test.py --customers 50 --kubectl-namespace engagehub    # adds replica and HPA counts
k6 run -e GATEWAY=http://localhost:18080 scripts/load/k6-chat.js                 # k6 alternative
```

Each simulated customer registers, opens a conversation and chats on `/ws`: product, order and FAQ questions and requests for a human; some reconnect mid-conversation. The report gives requests per second, average and p95 latency of the server's ack and of the agent's reply, error rate, reconnects and recovery time, duplicates shown, and (with `--prometheus`) consumer lag and the number of replicas of each agent that processed tasks, which is the evidence that replicas work in parallel.

In-process load test, which ran for this work (real worker, sequencing and consumer code; in-memory Kafka; not a measure of network or broker throughput):

```bash
cd services/virtual-agent-service && ../../.venv/bin/python -m pytest tests/test_scaling_load.py -s
```

```text
In-process load test (real worker code, in-memory Kafka with consumer-group balancing)
  customers: 50, messages: 200 (195 agent tasks, 5 escalations)
  elapsed: 0.52 s, throughput: 386 tasks/s
  reply latency: avg 203.1 ms, p95 351.5 ms, max 448.7 ms
  events handled per replica: {'faq-1': 30, 'faq-2': 24, 'order-tracking-1': 38, 'order-tracking-2': 25,
    'product-advisor-1': 15, 'product-advisor-2': 43, 'product-advisor-3': 45, 'supervisor-1': 6, 'supervisor-2': 4}
  tasks per agent type (metric): {'product_advisor': 103.0, 'order_tracking': 63.0, 'faq': 54.0}
  crashed replica: product-advisor-1; tasks processed more than once during the crash and rebalance: 25
    (absorbed by idempotency keys); duplicate reply records: 0; tasks lost: 0
```

Every replica of every agent type processed tasks. `product-advisor-1` was killed mid-run without committing; its tasks were redelivered and answered once. Each backend call is simulated as a 10 ms request.

### Horizontal-scaling demonstration

`scripts/k8s/scaling_demo.sh` walks through the 16 steps on a cluster prepared by `kind_up.sh`. It has not been run here. Useful commands while it runs:

```bash
kubectl get pods -n engagehub -w
kubectl get hpa -n engagehub
kubectl get scaledobjects -n engagehub
kubectl describe hpa va-product-advisor -n engagehub
kubectl logs -n engagehub deployment/va-product-advisor | grep "agent task processed"
```

1–3. The cluster runs; every scaled Deployment shows 2 ready replicas and its HPA or ScaledObject.
4. The load test starts with 50 customers.
5–6. `engagehub_kafka_consumer_lag` for `va-*` rises. KEDA, or the HPA once CPU rises, adds agent replicas.
7. "agent task processed" log lines appear on several pods of each agent, and the Grafana panel *Agent tasks per second, by replica* shows one line per pod.
8–9. One `va-product-advisor` pod is deleted. The group rebalances, and the remaining pods take its partitions and redo its uncommitted tasks.
10–14. One `chat-service` pod is deleted. Its customers reconnect through the gateway to another pod, which claims their sessions (`handover completed` in the logs) and replays unacknowledged messages from PostgreSQL in order. The load report shows reconnects and `duplicates_shown: 0`.
15–16. The load test ends, lag drains, and after the 5-minute scale-down window the agents return to 2 replicas.

### Metrics and dashboards

| Metric | Labels |
|---|---|
| `engagehub_active_websocket_connections` | `endpoint` |
| `engagehub_kafka_consumer_lag` | `group`, `topic`, `partition` |
| `engagehub_agent_tasks_received_total` | `agent_type` |
| `engagehub_agent_tasks_processed_total` | `agent_type`, `outcome` (`replied`, `escalated`, `degraded`, `skipped`) |
| `engagehub_agent_tasks_failed_total` | `agent_type`, `reason` (exception class); counts attempts, before retry or dead-lettering |
| `engagehub_agent_processing_seconds` | `agent_type` |
| `engagehub_queue_depth` | `queue` (`human_agents`), `tenant` |
| `engagehub_handovers_total` | `reason` (the spec's `engagehub_handover_total`; the existing name is kept) |
| `engagehub_recovery_seconds`, `engagehub_messages_replayed_total` | —, `source` |

Prometheus adds `job` and `instance` (the service and the replica). No metric is labelled with a customer or conversation ID.

Grafana dashboard **EngageHub Horizontal Scaling** (`deploy/grafana/dashboards/engagehub-scaling.json`) shows running replicas per service, consumer lag by group, tasks per agent type and per replica, task processing time, failed tasks, queue depth, active sockets, handovers and recovery time. The *Ready replicas* and *HPA desired vs current* panels need kube-state-metrics, so they are empty under Docker Compose.

### Known limitations

- The real-stack load test, the kind workflow, KEDA and the cloud overlay have not been run; only the in-process load test and the offline manifest checks have.
- A consumer group cannot use more replicas than its topic has partitions. Existing Compose volumes keep 6 partitions per agent topic, so more than 6 replicas of one agent would idle there.
- A replica that dies after handling tasks but before committing causes those tasks to be processed again by another replica (at-least-once). Replies are deduplicated by idempotency key; any non-idempotent side effect in a new agent must be made idempotent too.
- Rebalances pause a group briefly. Aggressive scale-down would cause frequent pauses, hence the 5-minute window.
- Supporting services run with fixed replica counts and no autoscaling.
- The kind overlay's Kafka, Redis and PostgreSQL are single-node and not durable; they are for demonstrations only.

