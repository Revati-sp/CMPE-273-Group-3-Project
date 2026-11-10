"""Tenant service: the businesses hosted on the platform."""

from engagehub_common.config import ServiceSettings
from engagehub_common.service import Runtime, create_service_app, get_runtime
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from sqlalchemy import String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

router = APIRouter()

DEMO_TENANTS = (
    ("acme", "Acme Outdoors", "Gear for camping, hiking and the trail", "#2f6f4f"),
    ("globex", "Globex Electronics", "Consumer electronics and smart devices", "#2b5fa8"),
)


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(String(256), default="")
    brand_color: Mapped[str] = mapped_column(String(16), default="#333333")


def _public(t: Tenant) -> dict:
    return {"id": t.id, "name": t.name, "description": t.description, "brand_color": t.brand_color}


@router.get("/tenants")
async def list_tenants(rt: Runtime = Depends(get_runtime)) -> list[dict]:
    """Public: the login page needs this before anyone has a token."""
    async with rt.db.session() as session:
        return [_public(t) for t in await session.scalars(select(Tenant).order_by(Tenant.name))]


@router.get("/tenants/{tenant_id}")
async def get_tenant(tenant_id: str, rt: Runtime = Depends(get_runtime)) -> dict:
    async with rt.db.session() as session:
        tenant = await session.get(Tenant, tenant_id)
        if tenant is None:
            raise HTTPException(404, "tenant not found")
        return _public(tenant)


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="tenant-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        if settings.seed_demo_data:
            async with rt.db.session() as session:
                for tenant_id, name, description, color in DEMO_TENANTS:
                    await rt.db.insert_ignore(
                        session,
                        Tenant,
                        {"id": tenant_id, "name": name, "description": description, "brand_color": color},
                    )

    app = create_service_app("EngageHub Tenant Service", settings, runtime=runtime, db=True, setup=setup)
    app.include_router(router)
    return app
