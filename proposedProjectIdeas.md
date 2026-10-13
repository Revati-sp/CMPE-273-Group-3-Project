# CMPE 273 Distributed Systems Project Ideas

This repository contains three proposed project ideas for **CMPE 273: Enterprise Distributed Systems**.

The projects will use advanced backend and distributed-systems technologies, including **Apache Kafka, Docker, Kubernetes, REST/gRPC, Redis, distributed databases, monitoring, tracing, and fault-tolerance patterns**.

## Common Technology Stack

- Apache Kafka for event streaming and asynchronous communication
- Docker for containerizing microservices
- Kubernetes for orchestration, scaling, service discovery, and self-healing
- REST APIs or gRPC for synchronous communication
- Redis for caching and session management
- PostgreSQL or MongoDB for persistent storage
- Prometheus and Grafana for monitoring
- OpenTelemetry and Jaeger for distributed tracing
- NGINX or Kubernetes Ingress as an API gateway
- GitHub Actions for continuous integration

## Distributed-System Features

- Kafka producers, consumers, topics, partitions, and consumer groups
- Message acknowledgment and retry handling
- Dead-letter queues for failed messages
- Idempotent event processing
- Kubernetes Deployments and Services
- Multiple replicas of critical services
- Liveness and readiness probes
- Horizontal Pod Autoscaling
- Timeouts and circuit breakers
- Saga pattern for distributed transactions
- Centralized logging and monitoring
- Fault-injection testing
- Failure recovery and graceful degradation

# Proposed Project Ideas

## 1. EngageHub: Enterprise Omnichannel Customer Engagement Platform

EngageHub is a distributed customer-engagement platform that helps businesses communicate with customers through chatbots, live-agent support, product assistance, and order services. The concept is inspired by enterprise digital-engagement platforms such as TouchCommerce.

### Core Services

- Customer and Authentication Service
- Chat Service using WebSockets
- Chatbot Service
- Live-Agent and Queue Service
- Product Service
- Order Service
- Notification Service
- Analytics Service

### Distributed-Systems Focus

Kafka will transfer customer-chat, order, escalation, and notification events between services. Kubernetes will manage multiple Chat and Agent Service replicas. Redis will maintain active customer sessions and agent availability.

If the chatbot service fails, the customer request can be transferred to a live agent. If the Notification Service is unavailable, Kafka will retain the event for later processing.

### Example Demonstration

1. A customer asks a product question through the web application.
2. The chatbot attempts to answer the question.
3. An unanswered question is published to Kafka.
4. An available support agent receives the conversation.
5. The customer receives product or order assistance.
6. Analytics are updated asynchronously.
7. A service or Kubernetes pod is stopped to demonstrate recovery.

## 2. TripSync: Distributed Travel Booking Assistant

TripSync is a distributed travel-booking platform that helps users search, compare, and reserve flights, hotels, and activities through one application. It can also include a conversational assistant that helps users build a travel itinerary.

### Core Services

- User and Authentication Service
- Flight Search Service
- Hotel Search Service
- Activity Service
- Booking and Itinerary Service
- Payment Service using simulated payments
- Recommendation Service
- Notification Service

### Distributed-Systems Focus

The Flight, Hotel, and Activity Services will process search requests independently. Kafka will publish booking, payment, cancellation, and confirmation events.

The Saga pattern will coordinate reservations across multiple services. Idempotency will prevent duplicate bookings, while retries and dead-letter queues will handle failed events.

### Example Demonstration

1. A user searches for a trip.
2. Flight and Hotel Services process the search independently.
3. The user selects a flight and hotel.
4. The Booking Service reserves both resources.
5. The Payment Service processes a simulated payment.
6. Kafka publishes a confirmation event.
7. If payment fails, the Saga workflow releases the reservations.

## 3. DocuSphere: Fault-Tolerant Distributed AI Document Processing and Retrieval System

DocuSphere is a distributed AI platform that allows users to upload documents and ask questions about their content.

The system separates document uploading, text extraction, document chunking, embedding generation, vector search, and retrieval-augmented generation into independent services.

The primary focus will be:

- Distributed document-processing pipelines
- Kafka-based asynchronous communication
- Worker replication
- Kubernetes-based scaling
- Fault recovery
- Distributed storage
- Monitoring and tracing
- Retrieval-augmented generation

### Core Services

- Upload and Document Metadata Service
- Text Extraction/OCR Service
- Document Chunking Service
- Embedding Generation Service
- Vector Search Service
- RAG Question-Answering Service
- Notification Service
- Analytics Service

### Distributed-Systems Focus

Kafka will coordinate the document-processing pipeline through events such as:

- `DocumentUploaded`
- `TextExtracted`
- `ChunksCreated`
- `EmbeddingsGenerated`
- `DocumentIndexed`

Multiple processing workers will run as Kubernetes replicas. Failed jobs will be retried, and messages that repeatedly fail will be moved to a dead-letter queue.

Idempotent processing will prevent the same document from being processed more than once.

### Example Demonstration

1. A user uploads a PDF.
2. The Upload Service stores the document and publishes an event to Kafka.
3. Worker services extract text, create chunks, and generate embeddings.
4. Embeddings are stored in a vector database.
5. The user asks a question about the document.
6. The Retrieval Service finds relevant content.
7. The RAG Service generates an answer.
8. One worker pod is stopped while processing.
9. Kubernetes restarts the failed pod or another replica continues the job.


# Expected Deliverables for project

- Source code for all microservices
- Dockerfiles for each service
- Docker Compose configuration
- Kubernetes manifests or Helm charts
- Kafka topic and consumer-group configuration
- API documentation
- System architecture diagram
- Test cases and load-testing results
- Monitoring and tracing dashboard
- Failure-recovery demonstration
- Final project report and presentation
