"""JWT verification and tenant-scoped authorization.

The tenant always comes from the signed token, never from client-supplied
input, so a caller cannot widen their scope by changing a URL or body.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, Request, status

ROLE_CUSTOMER = "customer"
ROLE_AGENT = "agent"
ROLE_ADMIN = "admin"
ROLE_SERVICE = "service"


@dataclass(frozen=True)
class Principal:
    subject: str
    tenant_id: str
    role: str
    name: str = ""
    customer_id: str | None = None

    @property
    def is_staff(self) -> bool:
        return self.role in (ROLE_AGENT, ROLE_ADMIN, ROLE_SERVICE)


class TokenError(Exception):
    pass


def issue_token(
    secret: str,
    *,
    subject: str,
    tenant_id: str,
    role: str,
    name: str = "",
    customer_id: str | None = None,
    ttl_seconds: int = 3600,
    algorithm: str = "HS256",
) -> str:
    now = int(time.time())
    claims = {
        "sub": subject,
        "tenant_id": tenant_id,
        "role": role,
        "name": name,
        "iat": now,
        "exp": now + ttl_seconds,
    }
    if customer_id:
        claims["customer_id"] = customer_id
    return jwt.encode(claims, secret, algorithm=algorithm)


def service_token(secret: str, service: str, tenant_id: str, customer_id: str | None = None) -> str:
    """Short-lived token a service uses to call a peer on behalf of one tenant."""
    return issue_token(
        secret,
        subject=f"service:{service}",
        tenant_id=tenant_id,
        role=ROLE_SERVICE,
        name=service,
        customer_id=customer_id,
        ttl_seconds=120,
    )


def verify_token(secret: str, token: str, algorithm: str = "HS256") -> Principal:
    try:
        claims = jwt.decode(
            token, secret, algorithms=[algorithm], options={"require": ["exp", "sub", "tenant_id", "role"]}
        )
    except jwt.PyJWTError as exc:
        raise TokenError(str(exc)) from exc
    return Principal(
        subject=claims["sub"],
        tenant_id=claims["tenant_id"],
        role=claims["role"],
        name=claims.get("name", ""),
        customer_id=claims.get("customer_id"),
    )


def get_principal(request: Request) -> Principal:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    settings = request.app.state.settings
    try:
        principal = verify_token(settings.jwt_secret, token, settings.jwt_algorithm)
    except TokenError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token") from exc
    request.state.principal = principal
    return principal


def require_roles(*roles: str):
    def dependency(principal: Principal = Depends(get_principal)) -> Principal:
        if principal.role not in roles:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "insufficient role")
        return principal

    return dependency


def ensure_customer_access(principal: Principal, customer_id: str) -> None:
    """Customers reach only their own data. Staff stay inside their tenant by query scope."""
    if not principal.is_staff and principal.subject != customer_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "not found")
