"""Customer service: profiles, tier and language preference."""

from datetime import datetime
from typing import Literal

from engagehub_common.auth import (
    ROLE_ADMIN,
    ROLE_AGENT,
    ROLE_CUSTOMER,
    Principal,
    ensure_customer_access,
    get_principal,
    require_roles,
)
from engagehub_common.config import ServiceSettings
from engagehub_common.events import utcnow
from engagehub_common.service import Runtime, create_service_app, get_runtime
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import DateTime, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

router = APIRouter()

DEMO_CUSTOMERS = (
    ("cust-acme-dana", "acme", "Dana Whitfield", "standard", "en"),
    ("cust-acme-vic", "acme", "Vic Sandoval", "vip", "en"),
    ("cust-globex-sam", "globex", "Sam Lindqvist", "standard", "en"),
    ("cust-globex-val", "globex", "Val Moreno", "vip", "es"),
)


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(128), default="")
    tier: Mapped[str] = mapped_column(String(16), default="standard")
    language: Mapped[str] = mapped_column(String(8), default="en")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProfileUpdate(BaseModel):
    name: str | None = None
    language: Literal["en", "es"] | None = None


def _public(c: Customer) -> dict:
    return {"id": c.id, "tenant_id": c.tenant_id, "name": c.name, "tier": c.tier, "language": c.language}


async def _own_profile(session, principal: Principal) -> Customer:
    """Registered customers get a profile on first use, built from their signed token."""
    customer = await session.get(Customer, principal.subject)
    if customer is None:
        customer = Customer(id=principal.subject, tenant_id=principal.tenant_id, name=principal.name)
        session.add(customer)
        await session.flush()
    return customer


@router.get("/customers/me")
async def get_me(
    principal: Principal = Depends(require_roles(ROLE_CUSTOMER)), rt: Runtime = Depends(get_runtime)
) -> dict:
    async with rt.db.session() as session:
        return _public(await _own_profile(session, principal))


@router.patch("/customers/me")
async def update_me(
    body: ProfileUpdate,
    principal: Principal = Depends(require_roles(ROLE_CUSTOMER)),
    rt: Runtime = Depends(get_runtime),
) -> dict:
    # Tier is deliberately not editable here: customers cannot make themselves VIP.
    async with rt.db.session() as session:
        customer = await _own_profile(session, principal)
        for name, value in body.model_dump(exclude_none=True).items():
            setattr(customer, name, value)
        return _public(customer)


@router.get("/customers")
async def list_customers(
    principal: Principal = Depends(require_roles(ROLE_AGENT, ROLE_ADMIN)), rt: Runtime = Depends(get_runtime)
) -> list[dict]:
    async with rt.db.session() as session:
        rows = await session.scalars(select(Customer).where(Customer.tenant_id == principal.tenant_id))
        return [_public(c) for c in rows]


@router.get("/customers/{customer_id}")
async def get_customer(
    customer_id: str, principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)
) -> dict:
    ensure_customer_access(principal, customer_id)
    async with rt.db.session() as session:
        customer = await session.get(Customer, customer_id)
        if customer is None or customer.tenant_id != principal.tenant_id:
            raise HTTPException(404, "customer not found")
        return _public(customer)


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="customer-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        if settings.seed_demo_data:
            async with rt.db.session() as session:
                for customer_id, tenant, name, tier, language in DEMO_CUSTOMERS:
                    await rt.db.insert_ignore(
                        session,
                        Customer,
                        {
                            "id": customer_id,
                            "tenant_id": tenant,
                            "name": name,
                            "tier": tier,
                            "language": language,
                            "created_at": utcnow(),
                        },
                    )

    app = create_service_app("EngageHub Customer Service", settings, runtime=runtime, db=True, setup=setup)
    app.include_router(router)
    return app
