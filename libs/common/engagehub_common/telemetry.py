"""OpenTelemetry setup and trace propagation across Kafka headers."""

from __future__ import annotations

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_configured = False


def configure_telemetry(app, service: str, pod: str, endpoint: str, enabled: bool) -> None:
    global _configured
    if not enabled:
        return
    if not _configured:
        provider = TracerProvider(resource=Resource.create({"service.name": service, "service.instance.id": pod}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        trace.set_tracer_provider(provider)
        HTTPXClientInstrumentor().instrument()
        _configured = True
    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz,metrics")


def tracer():
    return trace.get_tracer("engagehub")


def inject_headers() -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(k, v.encode()) for k, v in carrier.items()]


def extract_context(headers) -> object:
    carrier = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in (headers or [])}
    return propagate.extract(carrier)
