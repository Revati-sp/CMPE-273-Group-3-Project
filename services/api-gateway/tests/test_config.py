"""Static checks on the gateway routes. CI also runs `nginx -t` on the rendered config."""

import pathlib
import re

CONFIG = (pathlib.Path(__file__).parent.parent / "nginx.conf.template").read_text()
SERVICES = pathlib.Path(__file__).parents[2]


def test_every_route_points_at_an_existing_service():
    block = re.search(r"map \$api_name \$api_upstream \{(.*?)\n\}", CONFIG, re.S).group(1)
    routes = dict(re.findall(r"^\s+([a-z]+)\s+([a-z-]+);", block, re.M))
    assert len(routes) == 12
    for service in routes.values():
        assert (SERVICES / service / "Dockerfile").exists(), service


def test_websocket_route_upgrades_the_connection():
    block = CONFIG[CONFIG.index("location = /ws") :]
    assert "proxy_set_header Upgrade $http_upgrade" in block
    assert "proxy_set_header Connection $connection_upgrade" in block


def test_operational_endpoints_are_blocked_before_api_routes():
    assert CONFIG.index("(metrics|health|healthz|ready|readyz") < CONFIG.index("location ~ ^/api/(?<api_name>")


def test_customer_reply_stream_socket_is_proxied_with_upgrade():
    block = CONFIG[CONFIG.index("location ^~ /ws/customer/") :]
    block = block[: block.index("}")]
    assert "proxy_set_header Upgrade $http_upgrade" in block and "chat-service" in block


def test_correlation_id_is_generated_and_forwarded():
    assert "proxy_set_header X-Correlation-ID $correlation_id" in CONFIG
    assert re.search(r"''\s+\$request_id;", CONFIG)


def test_upstreams_resolve_at_request_time():
    assert "resolver ${DNS_RESOLVER}" in CONFIG
    assert re.findall(r"proxy_pass\s+http://[a-z]", CONFIG) == []
