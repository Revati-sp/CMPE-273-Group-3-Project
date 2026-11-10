"""Structured JSON logging with correlation context pulled from contextvars."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime

from opentelemetry import trace

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)
tenant_id_var: ContextVar[str | None] = ContextVar("tenant_id", default=None)
conversation_id_var: ContextVar[str | None] = ContextVar("conversation_id", default=None)

_RESERVED = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, pod: str) -> None:
        super().__init__()
        self.service = service
        self.pod = pod

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "pod": self.pod,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, var in (
            ("correlation_id", correlation_id_var),
            ("tenant_id", tenant_id_var),
            ("conversation_id", conversation_id_var),
        ):
            if (value := var.get()) is not None:
                entry[key] = value
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            entry["trace_id"] = format(span_context.trace_id, "032x")
        entry.update({k: v for k, v in record.__dict__.items() if k not in _RESERVED})
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(service: str, pod: str, level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service, pod))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for noisy in ("aiokafka", "httpx", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def bind_event_context(*, correlation_id=None, tenant_id=None, conversation_id=None) -> None:
    correlation_id_var.set(correlation_id)
    tenant_id_var.set(tenant_id)
    conversation_id_var.set(conversation_id)
