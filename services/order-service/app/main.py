"""Order service: orders and return requests, scoped to tenant and customer."""

from datetime import datetime

from engagehub_common.auth import ROLE_SERVICE, Principal, get_principal
from engagehub_common.config import ServiceSettings
from engagehub_common.events import new_id, utcnow
from engagehub_common.service import Runtime, create_service_app, get_runtime
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field
from sqlalchemy import JSON, DateTime, Float, String, UniqueConstraint, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

router = APIRouter()

DEMO_ORDERS = (
    ("ORD-1001", "acme", "cust-acme-dana", "delivered", 189.0, "paid", "TRK-ACME-1001", "2026-10-02",
     [{"sku": "acme-tent-2p", "name": "Trailhead 2-Person Tent", "qty": 1}]),
    ("ORD-1002", "acme", "cust-acme-dana", "in_transit", 149.0, "paid", "TRK-ACME-1002", "2026-11-12",
     [{"sku": "acme-boots", "name": "Summit Hiking Boots", "qty": 1}]),
    ("ORD-1003", "acme", "cust-acme-vic", "delivered", 658.0, "paid", "TRK-ACME-1003", "2026-09-20",
     [{"sku": "acme-tent-4p", "name": "Basecamp 4-Person Tent", "qty": 2}]),
    ("ORD-2001", "globex", "cust-globex-sam", "delivered", 249.0, "paid", "TRK-GLBX-2001", "2026-10-15",
     [{"sku": "globex-phones", "name": "Aurora Noise-Cancelling Headphones", "qty": 1}]),
    ("ORD-2002", "globex", "cust-globex-sam", "processing", 79.0, "pending", None, "2026-11-20",
     [{"sku": "globex-speaker", "name": "Pulse Bluetooth Speaker", "qty": 1}]),
    ("ORD-2003", "globex", "cust-globex-val", "delivered", 1099.0, "paid", "TRK-GLBX-2003", "2026-10-28",
     [{"sku": "globex-laptop", "name": "Vertex 14 Laptop", "qty": 1}]),
)  # fmt: skip


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    customer_id: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32))
    total: Mapped[float] = mapped_column(Float)
    payment_status: Mapped[str] = mapped_column(String(32))
    tracking_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    estimated_delivery: Mapped[str | None] = mapped_column(String(32), nullable=True)
    items: Mapped[list] = mapped_column(JSON, default=list)


class ReturnRequest(Base):
    __tablename__ = "return_requests"
    __table_args__ = (UniqueConstraint("order_id", "idempotency_key", name="uq_return_order_key"),)

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    order_id: Mapped[str] = mapped_column(String(64), index=True)
    customer_id: Mapped[str] = mapped_column(String(128))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    reason: Mapped[str] = mapped_column(String(512), default="")
    status: Mapped[str] = mapped_column(String(32), default="open")
    refund_amount: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ReturnBody(BaseModel):
    reason: str = Field(default="", max_length=512)


def _order(o: Order) -> dict:
    return {
        "id": o.id,
        "customer_id": o.customer_id,
        "status": o.status,
        "total": o.total,
        "payment_status": o.payment_status,
        "tracking_number": o.tracking_number,
        "estimated_delivery": o.estimated_delivery,
        "items": o.items,
    }


def _return(r: ReturnRequest) -> dict:
    return {
        "id": r.id,
        "order_id": r.order_id,
        "status": r.status,
        "reason": r.reason,
        "refund_amount": r.refund_amount,
        "created_at": r.created_at.isoformat(),
    }


def _may_access(principal: Principal, customer_id: str) -> bool:
    """A service token acts for one customer. Staff see their tenant; customers see themselves."""
    if principal.role == ROLE_SERVICE:
        return principal.customer_id == customer_id
    return principal.is_staff or principal.subject == customer_id


async def _load_order(session, order_id: str, principal: Principal) -> Order:
    order = await session.get(Order, order_id)
    if order is None or order.tenant_id != principal.tenant_id or not _may_access(principal, order.customer_id):
        raise HTTPException(404, "order not found")
    return order


@router.get("/orders")
async def list_orders(
    customer_id: str | None = None,
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> list[dict]:
    target = customer_id or (None if principal.is_staff else principal.subject)
    if target is not None and not _may_access(principal, target):
        raise HTTPException(404, "customer not found")
    query = select(Order).where(Order.tenant_id == principal.tenant_id)
    if target is not None:
        query = query.where(Order.customer_id == target)
    async with rt.db.session() as session:
        return [_order(o) for o in await session.scalars(query.order_by(Order.id))]


@router.get("/orders/{order_id}")
async def get_order(
    order_id: str, principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)
) -> dict:
    async with rt.db.session() as session:
        return _order(await _load_order(session, order_id, principal))


@router.post("/orders/{order_id}/returns", status_code=201)
async def create_return(
    order_id: str,
    body: ReturnBody,
    response: Response,
    idempotency_key: str = Header(min_length=1, max_length=128),
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> dict:
    """A retry with the same Idempotency-Key returns the original request (200, not 201)."""
    async with rt.db.session() as session:
        order = await _load_order(session, order_id, principal)
        if order.status != "delivered":
            raise HTTPException(409, "only delivered orders can be returned")
        request_id = f"RET-{new_id()[:8].upper()}"
        created = await rt.db.insert_ignore(
            session,
            ReturnRequest,
            {
                "id": request_id,
                "tenant_id": order.tenant_id,
                "order_id": order.id,
                "customer_id": order.customer_id,
                "idempotency_key": idempotency_key,
                "reason": body.reason,
                "status": "open",
                "refund_amount": order.total,
                "created_at": utcnow(),
            },
        )
        row = await session.scalar(
            select(ReturnRequest).where(
                ReturnRequest.order_id == order.id, ReturnRequest.idempotency_key == idempotency_key
            )
        )
        if not created:
            response.status_code = 200
        return _return(row)


@router.get("/returns")
async def list_returns(principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)) -> list[dict]:
    query = select(ReturnRequest).where(ReturnRequest.tenant_id == principal.tenant_id)
    if not principal.is_staff:
        query = query.where(ReturnRequest.customer_id == principal.subject)
    elif principal.role == ROLE_SERVICE:
        query = query.where(ReturnRequest.customer_id == principal.customer_id)
    async with rt.db.session() as session:
        return [_return(r) for r in await session.scalars(query.order_by(ReturnRequest.created_at.desc()))]


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="order-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        if settings.seed_demo_data:
            async with rt.db.session() as session:
                for oid, tenant, customer, status, total, payment, tracking, eta, items in DEMO_ORDERS:
                    await rt.db.insert_ignore(
                        session,
                        Order,
                        {
                            "id": oid, "tenant_id": tenant, "customer_id": customer, "status": status,
                            "total": total, "payment_status": payment, "tracking_number": tracking,
                            "estimated_delivery": eta, "items": items,
                        },
                    )  # fmt: skip

    app = create_service_app("EngageHub Order Service", settings, runtime=runtime, db=True, setup=setup)
    app.include_router(router)
    return app
