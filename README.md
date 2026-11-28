# EngageHub: A Fault-Tolerant Hybrid AI–Human Customer Engagement Platform with Reliable Live-Chat Migration

## Implementation status

The platform runs locally with Docker Compose. This repository does not deploy it to a cloud. The only Kubernetes manifests are for Chat Service, in `deploy/k8s/chat-service`.

### Implemented

- Docker Compose stack: NGINX API gateway, React frontend, the FastAPI services, a three-broker Kafka cluster, a Redis Cluster, PostgreSQL, Prometheus, Grafana, and Jaeger.
- Authentication, tenants, customers, products, orders, returns, notifications, analytics, and audit logs.
- Eight virtual-agent workers. The eighth handler is the **Supervisor**, which asks a clarifying question and owns the handoff workflow. The other seven are Product Advisor, Order Tracking, Returns and Refunds, Billing, Technical Support, FAQ, and Complaint Management. Replies come from rules and the product and order services. The configured provider is the rules provider.
- Support chat on WebSocket `/ws`. A customer message is published to Kafka topic `chat.messages`. The agent orchestrator consumes that topic, classifies the text, and either assigns one virtual agent or escalates to the human queue. Agent tasks travel on `chat.agent-tasks.<agent>`. Product and order calls from an agent are REST.
- A human-agent queue for two agents: accept, reject, transfer, resolve, notes, and a handoff summary.
- Live session state in Redis. Conversation history is read from session-service over gRPC.
- The customer reply stream on `/ws/customer`: a sequenced PostgreSQL log, a Redis acknowledgement cursor, replay after reconnect, and dead-letter handling. Virtual-agent replies on `agent.responses` are delivered on this socket.
- Chat Service recovery in Docker Compose. Two chat replicas run behind the gateway. Stopping one replica, or stopping `chat-service` and starting it again, reconnects the customer and replays messages they have not acknowledged.
- Chat Service Kubernetes manifests that can be rendered with Kustomize. Applying them requires a cluster that already has Kafka, Redis, and PostgreSQL. This repository does not apply them.

### Future work

- Kubernetes manifests for the services other than Chat Service, and a deployment of the whole platform to a cluster or a cloud.
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
- **More than one chat replica:** Docker Compose runs two Chat Service replicas. The gateway reconnects a customer to the replica that is still up. Autoscaling is specified only in the Chat Service Kubernetes manifests, and those manifests are not deployed from this repository.
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
5. If one Chat Service replica stops, the gateway sends the reconnect to the other replica. That replica reads the session from Redis and replays messages after the last acknowledgement. Message IDs and idempotency keys drop duplicates. The same recovery is what `docker compose stop chat-service` and `docker compose start chat-service` demonstrate. Kubernetes restart of Chat Service is specified in `deploy/k8s/chat-service` and is not a deployment this repository performs.
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
| **Kubernetes** | Manifests for Chat Service only (`deploy/k8s/chat-service`) | Not applied. No other service has manifests |
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
- OpenTelemetry traces exported to Jaeger when the local stack is up.

### Distributed-systems behavior that is future work

- Kubernetes self-healing and horizontal pod autoscaling for the platform. The Chat Service manifests describe probes, a PodDisruptionBudget, and an autoscaler from 2 to 6 pods. They are not applied here, and the other services have no manifests.
- A cloud deployment.
- GitHub Actions for test and image builds.
- Load and fault-injection results beyond the Chat Service restart demo and the automated tests.
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
| `chat:cursor:{conversation_id}:{customer_id}` | `{"last_acknowledged_sequence": 4, "last_message_id": "message-004", "updated_at": "…"}` | The customer acknowledges a message |
| `chat:delivered:{conversation_id}:{customer_id}` | highest sequence sent to that customer | A message is sent on a socket (validates REST acks) |
| `chat:session:{conversation_id}:{customer_id}` | last pod and connect time | A socket connects (detects reconnects) |

- The cursor only moves forward. A Lua script compares before writing, so a late or reordered ack cannot rewind it.
- An ack beyond what the socket was actually sent is rejected (`invalid_ack`), as is a `message_id` that does not match the sequence number.
- Keys expire after 7 days without activity (`CURSOR_TTL_SECONDS`).
- The keys contain no `{…}` hash tags, and each operation touches one key, so they work on Redis Cluster.
- **Redis unavailable on reconnect:** the connection still succeeds. The server replays from the durable log, starting after the `last_received_sequence` the client sent in `hello` (or from the beginning if it sent none), and reports `cursor_source` as `client` or `unavailable`. The client deduplicates by `message_id`. Acks during the outage get a retryable `cursor_unavailable` error and the client resends them. The REST cursor and ack endpoints return `503`.

### Replay algorithm

On every connection:

1. Read the cursor `c` from Redis (fallbacks above).
2. Register the socket, so live messages arriving from now on are held, not lost.
3. Query `chat_messages` where `sequence_number > c`, ordered by sequence.
4. Send `replay_started {from_sequence: c+1, to_sequence}`, the messages, then `replay_completed {last_sequence}`.
5. Release the held live messages through the normal delivery check.

Every socket tracks `last_sent`, the highest sequence it has been sent. A live message with `seq <= last_sent` is a duplicate and dropped. One with `seq > last_sent + 1` means a gap, which is filled from PostgreSQL before sending. So each socket receives a strictly increasing, gap-free stream, whatever order the fan-out arrives in.

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

### Kubernetes

Chat Service is the only service with Kubernetes manifests. They are in `deploy/k8s/chat-service` (Kustomize). Rendering or applying them is optional and requires a cluster you already operate. This repository does not deploy Chat Service, or any other service, to a cloud.

```bash
cp deploy/k8s/chat-service/secret.env.example deploy/k8s/chat-service/secret.env   # set real values
kubectl kustomize deploy/k8s/chat-service          # render and inspect
kubectl apply -k deploy/k8s/chat-service
```

They contain a Namespace, a ConfigMap, a Secret (generated from the git-ignored `secret.env`), a Deployment with 2 replicas, a Service, a PodDisruptionBudget, a HorizontalPodAutoscaler (2 to 6 pods), Ingress rules for `/ws/customer` and `/api/chat`, and the topic-creation Job.

The Deployment uses readiness (`/ready`), liveness (`/health`) and startup probes, CPU and memory requests and limits, a non-root read-only container, `maxUnavailable: 0` rollouts, and graceful shutdown: a `preStop` pause so endpoints drop the pod, then on SIGTERM the pod closes customer sockets with code `1012`, finishes its in-flight Kafka records and leaves the consumer group. The Service has no session affinity: any pod can resume any conversation from Redis and PostgreSQL. Kafka, Redis Cluster and PostgreSQL are expected to be installed separately (for example through operators); their addresses are in the ConfigMap.

### Observability

Structured JSON log events (each with `correlation_id`, `conversation_id` and, where relevant, `message_id` and `sequence_number`): `websocket connected`, `websocket disconnected`, `message received from kafka`, `message appended to durable log`, `cursor updated`, `replay started`, `replay completed`, `duplicate message ignored`, `handler failed; retrying`, `dependency unavailable; retrying until it recovers`, `event dead-lettered`, `customer reconnected`, `session recovery completed`.

Prometheus metrics (on `/metrics`):

| Metric | Meaning |
|---|---|
| `engagehub_customer_ws_connections` | Active customer sockets |
| `engagehub_customer_reconnects_total` | Reconnections |
| `engagehub_customer_replays_total`, `engagehub_customer_messages_replayed_total` | Replays and messages replayed |
| `engagehub_customer_duplicates_ignored_total{stage}` | Duplicates dropped by the consumer or at delivery |
| `engagehub_customer_delivery_latency_seconds` | Producer timestamp to live WebSocket send |
| `engagehub_customer_recovery_seconds` | Reconnect to replay completed |
| `engagehub_chat_messages_appended_total`, `engagehub_customer_cursor_updates_total` | Appends and cursor updates |
| `engagehub_events_consumed_total{outcome="dead_letter"}`, `engagehub_event_retries_total` | Failed and retried Kafka events |
| `kafka_consumergroup_lag{consumergroup="chat-replies-consumer"}` | Consumer lag (from kafka-exporter) |

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
| End to end | `tests/e2e/test_reply_replay_e2e.py` (runs `scripts/demo_reply_replay.py`) |

Terminal output from the last run:

```text
services/chat-service           71 passed, 2 skipped   (SQLite; the 2 skipped need TEST_POSTGRES_URL)
services/chat-service           73 passed              (TEST_POSTGRES_URL set, PostgreSQL 16)
services/virtual-agent-service  22 passed
libs/common             52 passed
frontend                36 passed (ReplayClient 14, ChatConnection 22)
scripts/tests           5 passed
```

### Design decisions

- **Sequence in PostgreSQL, not Kafka offsets or Redis.** Offsets are per partition and change if topics are repartitioned; Redis can lose recent writes on failover. The database transaction makes the number durable together with the message.
- **Kafka fan-out for live delivery.** Only one pod's consumer sequences a reply, but the customer's socket may be on another pod. Each pod reads `chat.replies.delivery` and serves its own sockets. If that publish fails, the consumer retries, and any message missed live is recovered by the gap check or the next replay, since PostgreSQL has it.
- **Outages retry, bad events dead-letter.** Sending events to the DLQ because the database was briefly down would lose them from the log.
- **No sticky sessions.** Every pod can serve every conversation, which is what makes pod failure recoverable.
- **The cursor tracks acknowledgment, not delivery.** A message sent but not acknowledged is sent again after a reconnect; the client drops the repeat by `message_id`. That turns at-least-once delivery into exactly-once display.

### Known limitations

- Replay sends at most 5,000 messages per connection; longer histories would need paging.
- The cursor is per customer, not per device: two devices of one customer share it.
- If Redis is down and the client sends no `last_received_sequence`, the whole history is replayed (and deduplicated by the client).
- Every pod reads every message on `chat.replies.delivery`. That is simple and fine for this scale; very large deployments would route by conversation instead.
- Virtual agents publish each reply to both `chat.messages` (the support-chat flow) and `agent.responses` (this log), so a reply exists in two logs with the same `message_id` and independent sequence numbers.
- Customer messages on `/ws` are published to `chat.messages`, and the agent orchestrator consumes that topic. Customer messages on `/ws/customer` are stored in this log only. Forwarding them to the orchestrator is future work, listed under Implementation status.
- The Chat Service manifests assume Kafka, Redis Cluster, and PostgreSQL are already installed in a cluster. No other service has a manifest, and this repository does not deploy the platform to a cloud.
