## 1. EngageHub: Enterprise Omnichannel Customer Engagement Platform

EngageHub is a distributed customer-engagement platform inspired by enterprise platforms such as TouchCommerce. It combines AI-powered virtual agents and human support agents to provide real-time customer assistance through chat, product support, order tracking, and notifications.

The platform will support a hybrid workforce of **10 agents: 8 specialized virtual AI agents and 2 human agents**.

### Main Challenge

### Global Session State Reconciliation and Reliable Live-Chat Migration During Failures

In a large enterprise platform, thousands of customers may have active WebSocket conversations connected to different Chat Service pods. If a pod crashes, becomes unreachable, or is restarted during a Kubernetes deployment, the customer should not lose the conversation.

The system must preserve:

- Conversation history
- Unsent and unacknowledged messages
- Message order
- Typing indicators
- Customer and agent session information
- Current virtual-agent workflow
- Agent assignment
- Customer context during failover

The main goal is to provide reliable chat migration with no acknowledged messages lost and no duplicate messages after recovery.

### Hybrid Agent Workforce

#### Eight Virtual Agents

- Product Advisor Agent
- Order Tracking Agent
- Returns and Refunds Agent
- Billing Agent
- Technical Support Agent
- FAQ Agent
- Complaint Management Agent
- Supervisor and Routing Agent

#### Two Human Agents

Human agents handle:

- Complex customer problems
- Angry or frustrated customers
- Payment and privacy-related issues
- Requests that require human approval
- Cases where virtual agents are uncertain
- VIP or high-priority customers

### Core Functionalities

- Customer registration and authentication
- Multi-tenant support for multiple businesses
- Real-time WebSocket chat
- AI-powered customer support
- Intelligent routing to specialized virtual agents
- Collaboration between multiple virtual agents
- Automatic escalation to human agents
- Smart agent queue management
- Product search and recommendations
- Order-status lookup
- Returns and refund requests
- Customer sentiment and frustration detection
- AI-generated handoff summaries for human agents
- Agent availability and workload tracking
- Customer conversation history
- Notifications and customer feedback
- Supervisor analytics dashboard
- Audit logs for all important actions

### Distributed Architecture

The system will contain independently deployable services:

- API Gateway
- Authentication Service
- Customer and Tenant Service
- Chat Gateway Service
- Session Management Service
- Virtual Agent Orchestrator
- Individual Virtual Agent Services
- Human Agent and Queue Service
- Product Service
- Order Service
- Notification Service
- Analytics Service
- Audit Service

### Distributed Session State Ring

Redis Cluster will maintain the live state of active conversations, including:

- Session ID
- Customer ID
- Assigned agent
- Chat Service pod
- Last acknowledged message ID
- Unacknowledged messages
- Typing status
- Current workflow state
- Tenant information

Session data will be replicated so that another Chat Service pod can continue the conversation if the original pod fails.

### Stateful Handover and Message Replay

Every chat message will receive:

- A unique message ID
- A conversation ID
- A monotonically increasing sequence number
- A correlation ID
- A timestamp
- A tenant ID

Chat events will be published to Kafka using the conversation ID as the partition key. This preserves message order for each conversation.

When a pod fails:

1. The client reconnects through the API Gateway.
2. The Gateway identifies the replacement Chat Service pod.
3. The replacement pod retrieves session state from Redis.
4. It reads the latest confirmed message ID.
5. It replays missing events from Kafka.
6. The client receives only messages it has not acknowledged.
7. Duplicate messages are removed using message IDs and idempotency tokens.

### Conflict-Free Message Ordering

The system will prevent duplicate or incorrectly ordered messages by using:

- Monotonically increasing sequence numbers
- Unique message IDs
- Kafka partition ordering
- Client acknowledgment numbers
- Idempotency keys
- Duplicate-event detection
- Client-side message deduplication
- Server-side message validation

If the same event is delivered more than once, it will be ignored after the first successful processing.

### Kafka Topics

Example Kafka topics include:

- `chat.messages`
- `chat.session-events`
- `chat.agent-events`
- `chat.escalations`
- `chat.notifications`
- `chat.analytics`
- `chat.audit-events`
- `chat.dead-letter-events`

Each event will contain the conversation ID, tenant ID, message ID, sequence number, event type, timestamp, and payload.

### Example Failure Demonstration

1. A customer starts a conversation with the Product Advisor Agent.
2. The agent sends a product recommendation.
3. The customer begins typing another message.
4. The Chat Service pod handling the session is stopped.
5. Kubernetes detects the failure and starts a replacement pod.
6. The customer reconnects automatically.
7. The replacement pod restores the session from Redis.
8. Missing messages are replayed from Kafka.
9. The conversation continues without losing acknowledged messages.
10. Duplicate messages are prevented using idempotency and sequence numbers.
11. The customer is automatically transferred to a human agent if the virtual agent is unavailable.

### Distributed-Systems Concepts Demonstrated

- Kafka event streaming
- Kafka partitions and consumer groups
- WebSocket connection migration
- Replicated session state
- Redis Cluster
- Message replay
- Idempotent processing
- Exactly-once user-visible behavior
- At-least-once backend event delivery
- Retry handling
- Dead-letter queues
- Circuit breakers
- Timeouts
- Kubernetes self-healing
- Horizontal scaling
- Distributed tracing
- Fault injection
- Graceful degradation

### Startup-Worthy Differentiator

EngageHub is not only a chatbot or customer-support application. Its key innovation is a **fault-tolerant hybrid AI-human workforce** that can continue live conversations even when services or Kubernetes pods fail.

The system combines:

- Eight specialized virtual agents
- Two human agents
- Intelligent agent collaboration
- Automatic human escalation
- Global session-state reconciliation
- Kafka-based message replay
- Duplicate-free message delivery
- Kubernetes-based recovery and scaling

This creates a reliable enterprise customer-engagement platform that protects the customer experience during service failures and network disruptions.
