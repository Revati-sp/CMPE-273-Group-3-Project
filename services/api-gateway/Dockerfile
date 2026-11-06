FROM nginx:1.27-alpine
COPY services/api-gateway/nginx.conf.template /etc/nginx/templates/default.conf.template
ENV DNS_RESOLVER=127.0.0.11
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null
