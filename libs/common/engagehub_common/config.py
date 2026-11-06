"""Environment-driven configuration shared by all services."""

from __future__ import annotations

import socket

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    service_name: str = "engagehub"
    environment: str = "development"
    pod_name: str = Field(default_factory=socket.gethostname)
    log_level: str = "INFO"

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_ttl_seconds: int = 8 * 3600

    kafka_bootstrap_servers: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    redis_mode: str = "cluster"  # "cluster" or "standalone"
    redis_nodes: str = "redis-1:6379,redis-2:6379,redis-3:6379"
    database_url: str = ""

    otel_enabled: bool = True
    otel_endpoint: str = "http://jaeger:4318/v1/traces"

    seed_demo_data: bool = True
    session_ttl_seconds: int = 24 * 3600
    consumer_max_attempts: int = 4
    consumer_retry_base_seconds: float = 0.2

    session_grpc_target: str = "session-service:50051"
    product_service_url: str = "http://product-service:8000"
    order_service_url: str = "http://order-service:8000"
    customer_service_url: str = "http://customer-service:8000"
    session_service_url: str = "http://session-service:8000"
    tenant_service_url: str = "http://tenant-service:8000"
    demo_password: str = "engagehub-demo"

    agent_type: str = ""  # virtual-agent-service: which specialised agent this worker runs
    llm_provider: str = "rules"
    worker_heartbeat_ttl_seconds: int = 15
    offer_timeout_seconds: int = 30
    average_handle_seconds: int = 240

    @field_validator("jwt_secret")
    @classmethod
    def _secret_is_strong(cls, value: str) -> str:
        if len(value) < 32:
            raise ValueError("JWT_SECRET must be at least 32 characters")
        return value
