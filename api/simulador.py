import json
import os
import time
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg


codigo = uuid.uuid4().hex[:12]
cliente_id = f"CLI-DEMO-{codigo}"
produto_id = f"PROD-DEMO-{codigo}"
pedido_id = f"PED-DEMO-{codigo}"

itens = [
    {
        "product_id": produto_id,
        "quantity": 2,
        "price": 10.00
    }
]

# Prepare only the catalog. The order itself must enter through the checkout API.
with psycopg.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "shopvibe_db"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    connect_timeout=10,
) as conexao:

    conexao.execute(
        """
        INSERT INTO shopvibe.customers (id, name)
        VALUES (%s, %s)
        """,
        (cliente_id, "Cliente ficticio da demonstracao"),
    )

    conexao.execute(
        """
        INSERT INTO shopvibe.products (id, name, price, stock)
        VALUES (%s, %s, %s, %s)
        """,
        (produto_id, "Produto ficticio da demonstracao", 10.00, 10),
    )

print(f"Cliente e produto preparados: {cliente_id}, {produto_id}")

# One path: checkout API -> PostgreSQL -> Debezium -> Kafka -> MinIO Bronze.
pedido = {
    "order_id": pedido_id,
    "customer_id": cliente_id,
    "items": itens,
    "total_amount": 20.00,
    "payment_method": "pix",
    "status": "APPROVED",
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

for tentativa in range(30):
    try:
        requisicao = Request(
            "http://api:8000/api/v1/checkout",
            data=json.dumps(pedido).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urlopen(requisicao, timeout=30) as resposta:
            print(resposta.read().decode("utf-8"))

        break

    except (HTTPError, URLError, TimeoutError) as erro:
        if isinstance(erro, HTTPError) and erro.code < 500:
            raise
        if tentativa == 29:
            raise

        print(f"Aguardando API/PostgreSQL: {erro}")
        time.sleep(2)

print(f"Checkout gravado via API. Confira o CDC na Bronze: {pedido_id}")
print(f"Produto do teste CDC: {produto_id}")
