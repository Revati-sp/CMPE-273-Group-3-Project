"""Product service: per-tenant catalogue, search and recommendations."""

from engagehub_common.auth import Principal, get_principal
from engagehub_common.config import ServiceSettings
from engagehub_common.service import Runtime, create_service_app, get_runtime
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from sqlalchemy import Float, Integer, String, or_, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

router = APIRouter()

DEMO_PRODUCTS = (
    ("acme-tent-2p", "acme", "Trailhead 2-Person Tent", "camping", 189.0, 14, 4.6, "Lightweight three-season tent."),
    ("acme-tent-4p", "acme", "Basecamp 4-Person Tent", "camping", 329.0, 6, 4.4, "Roomy tent for family trips."),
    ("acme-bag-20", "acme", "Ridgeline 20F Sleeping Bag", "camping", 129.0, 22, 4.7, "Warm down-blend sleeping bag."),
    ("acme-boots", "acme", "Summit Hiking Boots", "footwear", 149.0, 30, 4.8, "Waterproof boots with ankle support."),
    ("acme-pack-45", "acme", "Switchback 45L Backpack", "packs", 159.0, 0, 4.5, "Ventilated overnight backpack."),
    ("acme-jacket", "acme", "Stormline Rain Jacket", "apparel", 99.0, 40, 4.3, "Packable waterproof jacket."),
    ("globex-phones", "globex", "Aurora Noise-Cancelling Headphones", "audio", 249.0, 18, 4.7, "40-hour battery life."),
    ("globex-speaker", "globex", "Pulse Bluetooth Speaker", "audio", 79.0, 55, 4.4, "Waterproof portable speaker."),
    ("globex-laptop", "globex", "Vertex 14 Laptop", "computers", 1099.0, 9, 4.6, "14-inch ultralight laptop."),
    ("globex-camera", "globex", "Lumen Mirrorless Camera", "cameras", 849.0, 5, 4.5, "24MP camera with 4K video."),
    ("globex-phone", "globex", "Nova X Phone", "phones", 699.0, 25, 4.2, "6.5-inch OLED smartphone."),
    ("globex-hub", "globex", "Halo Smart Home Hub", "smart-home", 129.0, 33, 4.1, "Controls lights, locks and more."),
)


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(128))
    category: Mapped[str] = mapped_column(String(64))
    price: Mapped[float] = mapped_column(Float)
    stock: Mapped[int] = mapped_column(Integer)
    rating: Mapped[float] = mapped_column(Float)
    description: Mapped[str] = mapped_column(String(512))


def _public(p: Product) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "category": p.category,
        "price": p.price,
        "stock": p.stock,
        "rating": p.rating,
        "description": p.description,
    }


def _escape_like(term: str) -> str:
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@router.get("/products")
async def search_products(
    q: str = Query(default="", max_length=100),
    category: str | None = None,
    max_price: float | None = Query(default=None, ge=0),
    limit: int = Query(default=20, le=100),
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> list[dict]:
    query = select(Product).where(Product.tenant_id == principal.tenant_id)
    if q:
        pattern = f"%{_escape_like(q)}%"
        query = query.where(
            or_(
                Product.name.ilike(pattern, escape="\\"),
                Product.description.ilike(pattern, escape="\\"),
                Product.category.ilike(pattern, escape="\\"),
            )
        )
    if category:
        query = query.where(Product.category == category)
    if max_price is not None:
        query = query.where(Product.price <= max_price)
    async with rt.db.session() as session:
        rows = await session.scalars(query.order_by(Product.rating.desc()).limit(limit))
        return [_public(p) for p in rows]


@router.get("/products/recommendations")
async def recommendations(
    max_price: float | None = Query(default=None, ge=0),
    limit: int = Query(default=3, le=20),
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> list[dict]:
    """Top-rated items in stock."""
    query = select(Product).where(Product.tenant_id == principal.tenant_id, Product.stock > 0)
    if max_price is not None:
        query = query.where(Product.price <= max_price)
    async with rt.db.session() as session:
        rows = await session.scalars(query.order_by(Product.rating.desc()).limit(limit))
        return [_public(p) for p in rows]


@router.get("/products/{product_id}")
async def get_product(
    product_id: str, principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)
) -> dict:
    async with rt.db.session() as session:
        product = await session.get(Product, product_id)
        if product is None or product.tenant_id != principal.tenant_id:
            raise HTTPException(404, "product not found")
        return _public(product)


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="product-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        if settings.seed_demo_data:
            async with rt.db.session() as session:
                for pid, tenant, name, category, price, stock, rating, description in DEMO_PRODUCTS:
                    await rt.db.insert_ignore(
                        session,
                        Product,
                        {
                            "id": pid, "tenant_id": tenant, "name": name, "category": category, "price": price,
                            "stock": stock, "rating": rating, "description": description,
                        },
                    )  # fmt: skip

    app = create_service_app("EngageHub Product Service", settings, runtime=runtime, db=True, setup=setup)
    app.include_router(router)
    return app
