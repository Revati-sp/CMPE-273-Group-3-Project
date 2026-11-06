# Build from the repository root: docker build -f services/audit-service/Dockerfile .
# The layers before "COPY services/..." are identical across services, so they are shared.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_ROOT_USER_ACTION=ignore
WORKDIR /srv
COPY requirements/base.txt /tmp/base.txt
RUN pip install -r /tmp/base.txt && useradd --uid 10001 --no-create-home engagehub
COPY libs/common /tmp/common
RUN pip install --no-deps /tmp/common && rm -rf /tmp/common
COPY services/audit-service/app /srv/app
USER 10001
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8000/healthz', timeout=2)"
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "15", "--no-access-log"]
