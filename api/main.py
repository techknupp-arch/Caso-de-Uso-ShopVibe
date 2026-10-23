"""Persist checkouts in PostgreSQL; Debezium is the only publisher of new orders."""
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
logger = logging.getLogger(__name__)
app = FastAPI(title="ShopVibe Checkout API", version="1.1.0",
              description="Checkout -> PostgreSQL -> Debezium CDC -> Kafka -> Bronze")


def connect_db():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "shopvibe_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        connect_timeout=5, row_factory=dict_row,
    )


class OrderItem(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    product_id: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(ge=0, max_digits=12, decimal_places=2)


class OrderPayload(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    items: List[OrderItem] = Field(min_length=1)
    total_amount: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    payment_method: str = Field(min_length=1)
    ip_address: Optional[str] = "127.0.0.1"
    device_id: Optional[str] = "web_browser"
    # Retain the existing status contract; payment authorization is a separate step.
    status: str = Field(default="APPROVED", min_length=1)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_total(self):
        if sum(item.price * item.quantity for item in self.items) != self.total_amount:
            raise ValueError("total_amount deve corresponder a soma dos itens")
        return self


def persist_order(order):
    # Decimal strings in JSONB preserve cents exactly and are accepted by the Silver contract.
    values = {
        "customer_id": order.customer_id,
        "items": [{"product_id": item.product_id, "quantity": item.quantity,
                   "price": format(item.price, ".2f")} for item in order.items],
        "total_amount": order.total_amount, "payment_method": order.payment_method,
        "status": order.status, "ip_address": order.ip_address, "device_id": order.device_id,
    }
    stamp = order.timestamp
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    with connect_db() as connection:
        products = {row["id"] for row in connection.execute(
            "SELECT id FROM shopvibe.products WHERE id = ANY(%s)",
            ([item.product_id for item in order.items],),
        )}
        if any(item.product_id not in products for item in order.items):
            raise HTTPException(status_code=422, detail="Pedido possui produto nao cadastrado")
        saved = connection.execute(
            """INSERT INTO shopvibe.orders
               (id, customer_id, items, total_amount, payment_method, status, ip_address, device_id, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO NOTHING RETURNING *""",
            (order.order_id, values["customer_id"], Jsonb(values["items"]), values["total_amount"],
             values["payment_method"], values["status"], values["ip_address"], values["device_id"], stamp),
        ).fetchone()
        created = saved is not None
        if not created:
            saved = connection.execute("SELECT * FROM shopvibe.orders WHERE id = %s", (order.order_id,)).fetchone()
            # A retry keeps the original creation time and never emits an UPDATE/second CDC event.
            if saved is None or any(saved[field] != value for field, value in values.items()):
                raise HTTPException(status_code=409, detail="order_id ja utilizado por outro pedido")
    # The transaction has committed before the caller receives a success response.
    return saved, created


@app.post("/api/v1/checkout", status_code=201)
def process_checkout(order: OrderPayload, response: Response):
    try:
        saved, created = persist_order(order)
    except psycopg.errors.ForeignKeyViolation:
        raise HTTPException(status_code=404, detail="Cliente nao cadastrado") from None
    except psycopg.Error:
        logger.exception("Falha ao persistir checkout")
        raise HTTPException(status_code=503, detail="Nao foi possivel confirmar a gravacao do pedido; tente novamente com o mesmo order_id") from None
    response.status_code = 201 if created else 200
    return {
        "message": "Pedido gravado no PostgreSQL; envio ao Kafka e Bronze ocorre pelo CDC",
        "order_id": saved["id"], "replayed": not created,
        "topic": "shopvibe.shopvibe.orders",
        "data": {"order_id": saved["id"], **{field: saved[field] for field in (
            "customer_id", "items", "payment_method", "status", "ip_address", "device_id")},
                 "total_amount": format(saved["total_amount"], ".2f"),
                 "timestamp": saved["created_at"].isoformat()},
    }


@app.get("/health")
def health_check():
    try:
        with connect_db() as connection:
            connection.execute("SELECT 1 FROM shopvibe.orders LIMIT 0")
    except psycopg.Error:
        raise HTTPException(status_code=503, detail="PostgreSQL indisponivel") from None
    return {"status": "healthy", "service": "shopvibe-checkout-api", "storage": "postgresql"}
