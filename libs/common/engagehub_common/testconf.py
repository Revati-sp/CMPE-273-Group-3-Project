"""Environment defaults for service test suites (imported first by each conftest)."""

import os

os.environ.setdefault("JWT_SECRET", "test-secret-test-secret-test-secret-123")
os.environ.setdefault("OTEL_ENABLED", "false")
