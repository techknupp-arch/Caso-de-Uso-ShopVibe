"""Publish validated Gold snapshots pinned to one immutable Silver snapshot."""
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import gold_model
from .silver import validate_snapshot as validate_silver
from .storage import client, digest, ensure_bucket, json_bytes

MODEL_VERSION = "1"
SOURCES = ("customers", "products", "orders", "order_items", "inventory")


def _snapshot_id(value):
    if not isinstance(value, str) or len(value) != 24 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("Snapshot invalido")
    return value


def _read_silver(s3, snapshot_id):
    import pyarrow.parquet as pq
    validate_silver(snapshot_id, s3)
    raw_manifest = s3.get_object(Bucket="silver", Key="snapshots/" + snapshot_id + "/manifest.json")["Body"].read()
    manifest = json.loads(raw_manifest)
    tables = {}
    for name in SOURCES:
        info = manifest["tables"][name]
        raw = s3.get_object(Bucket="silver", Key=info["key"])["Body"].read()
        if digest(raw) != info["sha256"]:
            raise ValueError("Silver alterada durante a leitura: " + name)
        tables[name] = pq.read_table(io.BytesIO(raw)).to_pylist()
    return tables, digest(raw_manifest)


def validate_snapshot(snapshot_id=None, s3=None):
    import pyarrow.parquet as pq
    s3 = s3 or client()
    if snapshot_id is None:
        snapshot_id = json.loads(s3.get_object(Bucket="gold", Key="LATEST.json")["Body"].read())["snapshot_id"]
    _snapshot_id(snapshot_id)
    prefix = "snapshots/" + snapshot_id + "/"
    manifest = json.loads(s3.get_object(Bucket="gold", Key=prefix + "manifest.json")["Body"].read())
    contracts = gold_model.schemas()
    if manifest["snapshot_id"] != snapshot_id or set(manifest["tables"]) != set(contracts):
        raise ValueError("Manifesto Gold incompleto")
    _snapshot_id(manifest["silver_snapshot_id"])
    source, silver_hash = _read_silver(s3, manifest["silver_snapshot_id"])
    if silver_hash != manifest["silver_manifest_sha256"]:
        raise ValueError("Manifesto Silver diverge da origem da Gold")
    tables = {}
    for name, schema in contracts.items():
        info = manifest["tables"][name]
        if info["key"] != prefix + name + "/part-00000.parquet":
            raise ValueError("Caminho Gold divergente: " + name)
        raw = s3.get_object(Bucket="gold", Key=info["key"])["Body"].read()
        if digest(raw) != info["sha256"]:
            raise ValueError("Checksum Gold divergente: " + name)
        table = pq.read_table(io.BytesIO(raw))
        if table.num_rows != info["rows"] or not table.schema.equals(schema):
            raise ValueError("Contagem/schema Gold divergente: " + name)
        tables[name] = table.to_pylist()
    quality = gold_model.validate(tables, manifest["business_timezone"])
    if quality != manifest["quality"]:
        raise ValueError("Qualidade Gold divergente do manifesto")
    # Reconcile order identities/amounts with Silver independently of item joins.
    orders = {row["order_id"]: row for row in source["orders"] if not row["is_deleted"]}
    facts = {row["pedido_id"]: row for row in tables["fato_pedido"]}
    if set(orders) != set(facts) or any(
        facts[key]["valor_pedido"] != row["total_amount"] or facts[key]["status"] != row["status"]
        or facts[key]["cliente_id"] != row["customer_id"] or facts[key]["moeda"] != row["currency"]
        or facts[key]["pedido_em"] != row["ordered_at"] or facts[key]["is_simulated"] != row["is_simulated"]
        for key, row in orders.items()
    ):
        raise ValueError("Pedidos Gold divergem da Silver")
    items = {(row["order_id"], row["line_number"]): row for row in source["order_items"]
             if not row["is_deleted"] and row["order_id"] in orders}
    item_facts = {(row["pedido_id"], row["numero_linha"]): row for row in tables["fato_item_pedido"]}
    if set(items) != set(item_facts) or any(
        any(item_facts[key][gold_field] != row[silver_field] for gold_field, silver_field in (
            ("produto_id", "product_id"), ("quantidade", "quantity"), ("valor_unitario", "unit_price"), ("valor_item", "line_amount")))
        for key, row in items.items()
    ):
        raise ValueError("Itens Gold divergem da Silver")
    stock = {(row["product_id"], row["warehouse_id"]): row for row in source["inventory"] if not row["is_deleted"]}
    stock_facts = {(row["produto_id"], row["deposito_id"]): row for row in tables["estoque_atual"]}
    if set(stock) != set(stock_facts) or any(
        any(stock_facts[key][gold_field] != row[silver_field] for gold_field, silver_field in (
            ("saldo_fisico", "physical_stock"), ("saldo_virtual", "virtual_stock"), ("observado_em", "stock_at")))
        for key, row in stock.items()
    ):
        raise ValueError("Estoque Gold diverge da Silver")
    return {"snapshot_id": snapshot_id, "silver_snapshot_id": manifest["silver_snapshot_id"],
            "business_timezone": manifest["business_timezone"],
            "tables": {name: len(rows) for name, rows in tables.items()}, "quality": quality, "passed": True}


def build_gold(silver_snapshot_id=None, s3=None, business_timezone=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.exceptions import ClientError
    s3 = s3 or client()
    ensure_bucket(s3, "gold")
    if silver_snapshot_id is None:
        silver_snapshot_id = json.loads(s3.get_object(Bucket="silver", Key="LATEST.json")["Body"].read())["snapshot_id"]
    _snapshot_id(silver_snapshot_id)
    zone = business_timezone or os.getenv("GOLD_TIMEZONE", "America/Sao_Paulo")
    code_hash = digest(Path(__file__).read_bytes() + Path(gold_model.__file__).read_bytes())
    source, silver_hash = _read_silver(s3, silver_snapshot_id)
    identity = {"model_version": MODEL_VERSION, "code_sha256": code_hash, "silver_snapshot_id": silver_snapshot_id,
                "silver_manifest_sha256": silver_hash, "business_timezone": zone}
    snapshot_id = digest(json_bytes(identity))[:24]
    prefix = "snapshots/" + snapshot_id + "/"
    try:
        tables = gold_model.materialize(source, zone)
        quality = gold_model.validate(tables, zone)
    except (ValueError, KeyError, TypeError, ArithmeticError) as error:
        s3.put_object(Bucket="gold", Key="quarantine/" + snapshot_id + "/rejections.json",
                      Body=json_bytes(dict(identity, reason=str(error))), ContentType="application/json")
        raise ValueError("Gold rejeitada; LATEST preservado: " + str(error)) from error
    try:
        s3.head_object(Bucket="gold", Key=prefix + "manifest.json")
    except ClientError as error:
        if str(error.response["Error"]["Code"]) not in ("404", "NoSuchKey", "NotFound"):
            raise
        exists = False
    else:
        exists = True
    if exists:
        result = validate_snapshot(snapshot_id, s3)
        reused = True
    else:
        manifest = dict(identity, snapshot_id=snapshot_id, created_at=datetime.now(timezone.utc).isoformat(),
                        quality=quality, tables={})
        for name, schema in gold_model.schemas().items():
            table = pa.Table.from_pylist(tables[name], schema=schema)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy", version="2.4")
            raw = buffer.getvalue()
            key = prefix + name + "/part-00000.parquet"
            s3.put_object(Bucket="gold", Key=key, Body=raw, ContentType="application/octet-stream")
            manifest["tables"][name] = {"key": key, "rows": table.num_rows, "sha256": digest(raw)}
        s3.put_object(Bucket="gold", Key=prefix + "manifest.json", Body=json_bytes(manifest), ContentType="application/json")
        result = validate_snapshot(snapshot_id, s3)
        reused = False
    s3.put_object(Bucket="gold", Key="LATEST.json", Body=json_bytes({"snapshot_id": snapshot_id,
                  "silver_snapshot_id": silver_snapshot_id, "manifest": prefix + "manifest.json"}), ContentType="application/json")
    return dict(result, reused=reused)
