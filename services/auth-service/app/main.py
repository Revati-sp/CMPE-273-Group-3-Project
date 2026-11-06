"""Auth service: registration, login and JWT issuance."""

import asyncio
import logging

import httpx
from engagehub_common.auth import ROLE_ADMIN, ROLE_AGENT, ROLE_CUSTOMER, Principal, get_principal, issue_token
from engagehub_common.config import ServiceSettings
from engagehub_common.events import new_id, utcnow
from engagehub_common.messaging.bus import PublishError
from engagehub_common.service import Runtime, create_service_app, get_runtime
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.models import Base, User
from app.passwords import hash_password, verify_password

log = logging.getLogger(__name__)
router = APIRouter()

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
# Compared against when the email is unknown, so timing does not reveal which emails exist.
_DUMMY_HASH = hash_password("engagehub-timing-equaliser")

DEMO_USERS = (
    ("cust-acme-dana", "acme", "dana@acme.example", "Dana Whitfield", ROLE_CUSTOMER),
    ("cust-acme-vic", "acme", "vic@acme.example", "Vic Sandoval", ROLE_CUSTOMER),
    ("agent-acme-alice", "acme", "alice@acme.example", "Alice Morgan", ROLE_AGENT),
    ("agent-acme-bob", "acme", "bob@acme.example", "Bob Reyes", ROLE_AGENT),
    ("admin-acme", "acme", "admin@acme.example", "Acme Admin", ROLE_ADMIN),
    ("cust-globex-sam", "globex", "sam@globex.example", "Sam Lindqvist", ROLE_CUSTOMER),
    ("cust-globex-val", "globex", "val@globex.example", "Val Moreno", ROLE_CUSTOMER),
    ("agent-globex-carol", "globex", "carol@globex.example", "Carol Chen", ROLE_AGENT),
    ("agent-globex-dave", "globex", "dave@globex.example", "Dave Okafor", ROLE_AGENT),
    ("admin-globex", "globex", "admin@globex.example", "Globex Admin", ROLE_ADMIN),
)


class RegisterRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64)
    email: str = Field(pattern=EMAIL_PATTERN, max_length=254)
    name: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64)
    email: str = Field(max_length=254)
    password: str = Field(max_length=128)


def _token_response(user: User, settings: ServiceSettings) -> dict:
    token = issue_token(
        settings.jwt_secret,
        subject=user.id,
        tenant_id=user.tenant_id,
        role=user.role,
        name=user.name,
        customer_id=user.id if user.role == ROLE_CUSTOMER else None,
        ttl_seconds=settings.jwt_ttl_seconds,
        algorithm=settings.jwt_algorithm,
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user.id, "tenant_id": user.tenant_id, "email": user.email, "name": user.name, "role": user.role},
    }


async def _audit(rt: Runtime, principal: Principal, action: str) -> None:
    try:
        await rt.publish_audit(principal, action, resource=principal.subject)
    except PublishError:
        # Do not lock everyone out because Kafka is down. The failure itself is logged.
        log.warning("audit event not published", extra={"action": action})


@router.post("/register", status_code=201)
async def register(body: RegisterRequest, request: Request, rt: Runtime = Depends(get_runtime)) -> dict:
    """Self-registration only ever creates customers. Staff accounts are provisioned."""
    if not await request.app.state.tenant_exists(body.tenant_id):
        raise HTTPException(400, "unknown tenant")
    email = body.email.lower()
    password_hash = await asyncio.to_thread(hash_password, body.password)
    user = User(
        id=f"cust-{new_id()}",
        tenant_id=body.tenant_id,
        email=email,
        name=body.name,
        role=ROLE_CUSTOMER,
        password_hash=password_hash,
    )
    async with rt.db.session() as session:
        existing = await session.scalar(select(User).where(User.tenant_id == body.tenant_id, User.email == email))
        if existing is not None:
            raise HTTPException(409, "email already registered for this tenant")
        session.add(user)
    await _audit(rt, Principal(user.id, user.tenant_id, user.role, user.name, user.id), "user.registered")
    return _token_response(user, rt.settings)


@router.post("/login")
async def login(body: LoginRequest, rt: Runtime = Depends(get_runtime)) -> dict:
    async with rt.db.session() as session:
        user = await session.scalar(
            select(User).where(User.tenant_id == body.tenant_id, User.email == body.email.lower())
        )
    stored = user.password_hash if user else _DUMMY_HASH
    valid = await asyncio.to_thread(verify_password, body.password, stored)
    if user is None or not valid:
        raise HTTPException(401, "invalid credentials")
    principal = Principal(
        user.id, user.tenant_id, user.role, user.name, user.id if user.role == ROLE_CUSTOMER else None
    )
    await _audit(rt, principal, "user.login")
    return _token_response(user, rt.settings)


@router.get("/me")
async def me(principal: Principal = Depends(get_principal)) -> dict:
    return {
        "id": principal.subject,
        "tenant_id": principal.tenant_id,
        "role": principal.role,
        "name": principal.name,
    }


async def seed_users(rt: Runtime, password: str) -> None:
    password_hash = await asyncio.to_thread(hash_password, password)
    async with rt.db.session() as session:
        for user_id, tenant, email, name, role in DEMO_USERS:
            await rt.db.insert_ignore(
                session,
                User,
                {
                    "id": user_id,
                    "tenant_id": tenant,
                    "email": email,
                    "name": name,
                    "role": role,
                    "password_hash": password_hash,
                    "created_at": utcnow(),
                },
            )


def create_app(runtime: Runtime | None = None, tenant_exists=None) -> FastAPI:
    settings = ServiceSettings(service_name="auth-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        if settings.seed_demo_data:
            await seed_users(rt, settings.demo_password)
        http = httpx.AsyncClient(base_url=settings.tenant_service_url, timeout=3.0)

        async def check_tenant(tenant_id: str) -> bool:
            try:
                response = await http.get(f"/tenants/{tenant_id}")
            except httpx.HTTPError as exc:
                raise HTTPException(503, "tenant service unavailable") from exc
            return response.status_code == 200

        app.state.tenant_exists = tenant_exists or check_tenant

    app = create_service_app("EngageHub Auth Service", settings, runtime=runtime, kafka=True, db=True, setup=setup)
    app.include_router(router)
    return app
