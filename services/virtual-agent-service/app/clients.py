"""REST clients for domain services, with timeouts and circuit breakers."""

from __future__ import annotations

import httpx
from engagehub_common.auth import service_token
from engagehub_common.config import ServiceSettings
from engagehub_common.resilience import CircuitBreaker


class DependencyUnavailable(Exception):
    """A backing service failed. The agent degrades and hands the customer to a human."""


class DomainClient:
    def __init__(self, settings: ServiceSettings, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.settings = settings
        self.products = httpx.AsyncClient(base_url=settings.product_service_url, timeout=3.0, transport=transport)
        self.orders = httpx.AsyncClient(base_url=settings.order_service_url, timeout=3.0, transport=transport)
        self._product_breaker = CircuitBreaker("product-service")
        self._order_breaker = CircuitBreaker("order-service")

    def _headers(self, tenant_id: str, customer_id: str, extra: dict | None = None) -> dict:
        token = service_token(self.settings.jwt_secret, self.settings.service_name, tenant_id, customer_id)
        return {"Authorization": f"Bearer {token}", **(extra or {})}

    async def _request(self, client, breaker, method, url, *, tenant_id, customer_id, headers=None, **kwargs):
        async def call() -> httpx.Response:
            response = await client.request(
                method, url, headers=self._headers(tenant_id, customer_id, headers), **kwargs
            )
            if response.status_code >= 500:
                response.raise_for_status()
            return response

        try:
            return await breaker.call(call)
        except Exception as exc:
            raise DependencyUnavailable(f"{breaker.name}: {exc!r}") from exc

    async def search_products(self, tenant_id, customer_id, *, query="", max_price=None) -> list[dict]:
        params = {"q": query}
        if max_price is not None:
            params["max_price"] = max_price
        response = await self._request(
            self.products, self._product_breaker, "GET", "/products",
            tenant_id=tenant_id, customer_id=customer_id, params=params,
        )  # fmt: skip
        return response.json()

    async def recommendations(self, tenant_id, customer_id, *, max_price=None) -> list[dict]:
        params = {} if max_price is None else {"max_price": max_price}
        response = await self._request(
            self.products, self._product_breaker, "GET", "/products/recommendations",
            tenant_id=tenant_id, customer_id=customer_id, params=params,
        )  # fmt: skip
        return response.json()

    async def get_order(self, tenant_id, customer_id, order_id) -> dict | None:
        response = await self._request(
            self.orders, self._order_breaker, "GET", f"/orders/{order_id}",
            tenant_id=tenant_id, customer_id=customer_id,
        )  # fmt: skip
        return response.json() if response.status_code == 200 else None

    async def list_orders(self, tenant_id, customer_id) -> list[dict]:
        response = await self._request(
            self.orders, self._order_breaker, "GET", "/orders",
            tenant_id=tenant_id, customer_id=customer_id, params={"customer_id": customer_id},
        )  # fmt: skip
        return response.json() if response.status_code == 200 else []

    async def create_return(self, tenant_id, customer_id, order_id, reason, idempotency_key) -> dict | None:
        response = await self._request(
            self.orders, self._order_breaker, "POST", f"/orders/{order_id}/returns",
            tenant_id=tenant_id, customer_id=customer_id,
            headers={"Idempotency-Key": idempotency_key}, json={"reason": reason},
        )  # fmt: skip
        return response.json() if response.status_code in (200, 201) else None
