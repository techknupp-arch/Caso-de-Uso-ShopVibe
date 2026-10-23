"""Silver contracts and normalization. Monetary/quantity values use Decimal."""
import base64
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from .storage import digest, json_bytes

ENTITIES = ("customers", "products", "orders", "order_items", "payments", "fraud_assessments", "inventory", "shipments")
KEYS = {"customers": ("customer_id",), "products": ("product_id",), "orders": ("order_id",),
        "order_items": ("order_id", "line_number"), "payments": ("payment_id",), "fraud_assessments": ("order_id",),
        "inventory": ("product_id", "warehouse_id"), "shipments": ("shipment_id",)}


def instant(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, timezone.utc)
    if not value:
        raise ValueError("Timestamp obrigatorio ausente")
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    # The checkout API's historical payload uses naive UTC via datetime.utcnow().
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)


def decimal_value(value, scale=2, binary=False):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("Booleano nao e valor numerico")
    if binary:
        value = Decimal(int.from_bytes(base64.b64decode(value, validate=True), "big", signed=True)).scaleb(-scale)
    number = Decimal(str(value))
    quantum = Decimal(1).scaleb(-scale)
    if not number.is_finite() or number != number.quantize(quantum):
        raise ValueError("Precisao numerica invalida: " + str(value))
    if len(number.as_tuple().digits) > 16:
        raise ValueError("Numero excede a precisao da Silver")
    return number.quantize(quantum)


def identifier(value):
    if value is None or isinstance(value, bool) or not str(value).strip():
        raise ValueError("Identificador obrigatorio ausente")
    return str(value).strip()


def quantity(value):
    number = decimal_value(value, 0)
    if number is None or number < 0:
        raise ValueError("Quantidade deve ser inteira nao negativa")
    return int(number)


def cdc_money(value, schema, field):
    for branch in schema.get("fields", []):
        if branch.get("field") == "after":
            for child in branch.get("fields", []):
                if child.get("field") == field and child.get("name") == "org.apache.kafka.connect.data.Decimal":
                    return decimal_value(value, int(child.get("parameters", {}).get("scale", 2)), binary=isinstance(value, str))
    return decimal_value(value)


def base_record(entity, row, stamp, deleted=False, schema=None):
    schema = schema or {}
    if entity == "customers":
        out = {"customer_id": identifier(row.get("id")), "name": row.get("name"), "email": row.get("email")}
    elif entity == "products":
        out = {"product_id": identifier(row.get("id")), "name": row.get("name"),
               "price": cdc_money(row.get("price"), schema, "price"),
               "stock": None if deleted else quantity(row.get("stock", 0))}
    elif entity == "orders":
        items = row.get("items", [])
        # PostgreSQL JSONB is serialized as a JSON string by Debezium's Json logical type.
        if isinstance(items, str):
            items = json.loads(items)
        out = {"order_id": identifier(row.get("id", row.get("order_id"))), "customer_id": row.get("customer_id"),
               "total_amount": cdc_money(row.get("total_amount"), schema, "total_amount"), "currency": "BRL",
               "payment_method": row.get("payment_method"), "status": str(row.get("status") or "UNKNOWN").upper(),
               "ordered_at": instant(row.get("created_at") or row.get("timestamp") or stamp),
               "_items": items, "ip_address": row.get("ip_address"), "device_id": row.get("device_id")}
    else:
        raise ValueError("Entidade nao suportada: " + entity)
    if not deleted:
        if entity in ("customers", "products") and not out.get("name"):
            raise ValueError("Nome obrigatorio ausente")
        if entity == "products" and (out["price"] is None or out["price"] < 0):
            raise ValueError("Preco invalido")
        if entity == "orders":
            out["customer_id"] = identifier(out["customer_id"])
            if out["total_amount"] is None or out["total_amount"] < 0 or not isinstance(out["_items"], list):
                raise ValueError("Total ou itens do pedido invalidos")
            for item in out["_items"]:
                identifier(item.get("product_id"))
                if quantity(item.get("quantity")) < 1 or decimal_value(item.get("price")) is None or decimal_value(item.get("price")) < 0:
                    raise ValueError("Item de pedido invalido")
    return out


def make_event(entity, row, source, event_id, stamp, key, sequence=None, deleted=False, simulated=False, priority=2):
    identity = json.dumps([row[field] for field in KEYS[entity]], separators=(",", ":"))
    return {"entity": entity, "entity_id": identity, "row": row, "source_system": source,
            "source_event_id": event_id, "event_at": instant(stamp), "source_object": key,
            "source_sequence": sequence, "is_deleted": deleted, "is_simulated": simulated, "priority": priority}


def kafka_event(record, key):
    topic = record["_kafka_topic"]
    offset = int(record["_kafka_offset"])
    event_id = "kafka:{}:{}:{}".format(topic, record["_kafka_partition"], offset)
    if topic == "shopvibe.orders.raw":
        stamp = record.get("timestamp") or record["_kafka_timestamp"]
        row = base_record("orders", record, stamp)
        return make_event("orders", row, "checkout_api", event_id, stamp, key, offset)
    entity = topic.split(".")[-1]
    if topic != "shopvibe.shopvibe." + entity or entity not in ("customers", "products", "orders"):
        raise ValueError("Topico fora do contrato Silver")
    payload = record["payload"]
    operation = payload["op"]
    if operation not in ("c", "u", "r", "d"):
        raise ValueError("Operacao CDC nao suportada: " + operation)
    deleted = operation == "d"
    raw = payload["before"] if deleted else payload["after"]
    stamp = payload.get("source", {}).get("ts_ms") or record.get("_kafka_timestamp") or payload.get("ts_ms")
    row = base_record(entity, raw, stamp, deleted, record.get("schema", {}))
    return make_event(entity, row, "postgresql", event_id, stamp, key, offset, deleted, priority=3)


def external_events(doc, key):
    if doc.get("contract_version") != 1 or doc.get("simulated") is not True:
        raise ValueError("Contrato de simulacao invalido")
    source, entity, body = doc["source_system"], doc["dataset"], doc["body"]
    stamp, refs = doc["extracted_at"], doc.get("references", {})
    if source == "shopvibe_demo" and entity in ("customers", "products", "orders"):
        rows = [base_record(entity, row, stamp) for row in body]
    elif source == "payment_api" and entity == "payments":
        rows = []
        for row in body["payments"]:
            amount = decimal_value(row["amount"])
            if amount is None or amount < 0:
                raise ValueError("Valor de pagamento invalido")
            rows.append({"payment_id": identifier(row["id"]), "order_id": identifier(row["order_id"]),
                         "status": identifier(row["status"]).upper(), "amount": amount,
                         "currency": identifier(row["currency"]), "method": identifier(row["method"]),
                         "processed_at": instant(row["processed_at"]) if row.get("processed_at") else None})
    elif source == "clearsale" and entity == "fraud_assessments":
        rows = [{"order_id": identifier(row["code"]), "status_code": identifier(row["status"]),
                 "score": decimal_value(row.get("score")), "observed_at": instant(stamp)} for row in body["orders"]]
    elif source == "bling" and entity == "inventory":
        rows = []
        for row in body["data"]:
            product = identifier(row["produto"]["id"])
            for warehouse in row["depositos"]:
                rows.append({"product_id": refs.get("products", {}).get(product, "bling:" + product),
                             "warehouse_id": identifier(warehouse["id"]),
                             "physical_stock": decimal_value(warehouse["saldoFisico"], 4),
                             "virtual_stock": decimal_value(warehouse["saldoVirtual"], 4), "stock_at": instant(stamp)})
    elif source == "melhor_envio" and entity == "shipments":
        rows = [{"shipment_id": identifier(row["id"]), "order_id": identifier(refs.get("orders", {}).get(row["id"])),
                 "status": identifier(row["status"]).upper(), "tracking_code": row.get("tracking"),
                 "carrier": row.get("company", {}).get("name"), "protocol": row.get("protocol"),
                 "posted_at": instant(row["posted_at"]) if row.get("posted_at") else None,
                 "delivered_at": instant(row["delivered_at"]) if row.get("delivered_at") else None} for row in body.values()]
    else:
        raise ValueError("Fonte/dataset nao suportado")
    identities = {}
    for row in rows:
        identity = tuple(row[field] for field in KEYS[entity])
        content = json_bytes(row)
        if identity in identities and identities[identity] != content:
            raise ValueError("Chave duplicada com estados divergentes na mesma exportacao: " + str(identity))
        identities[identity] = content
        event_id = "external:" + digest(json_bytes({"source": source, "entity": entity, "stamp": stamp, "row": row}))
        yield make_event(entity, row, source, event_id, stamp, key, simulated=True, priority=1 if source == "shopvibe_demo" else 2)


def materialize(events):
    current = {entity: {} for entity in ENTITIES}
    history, seen = [], {}
    # Kafka offsets determine order within each source; batch exports use their extraction timestamps.
    events = sorted(events, key=lambda e: (e["entity"], e["entity_id"], e["priority"], e["source_system"],
                    e["source_sequence"] if e["source_sequence"] is not None else 0,
                    e["event_at"], e["source_event_id"]))
    for event in events:
        fingerprint = digest(json_bytes({k: v for k, v in event.items() if k != "source_object"}))
        event_id = event["source_event_id"]
        if event_id in seen:
            if seen[event_id] != fingerprint:
                raise ValueError("Mesmo evento com conteudos divergentes: " + event_id)
            continue
        seen[event_id] = fingerprint
        entity, identity = event["entity"], event["entity_id"]
        metadata = {key: event[key] for key in ("source_system", "source_event_id", "event_at", "source_object", "source_sequence", "is_deleted", "is_simulated")}
        previous = current[entity].get(identity, {})
        if event["is_deleted"]:
            row = dict(previous or event["row"])
        else:
            row = dict(event["row"])
        row.update(metadata)
        row["quality_flags"] = []
        current[entity][identity] = row
        history.append(dict(metadata, entity=entity, entity_id=identity, record_json=json_bytes(event["row"]).decode()))
        if entity == "orders":
            items = row.get("_items", [])
            active = set()
            for number, item in enumerate(items, 1):
                key = json.dumps([row["order_id"], number], separators=(",", ":"))
                active.add(key)
                price, count = decimal_value(item["price"]), quantity(item["quantity"])
                current["order_items"][key] = dict(metadata, order_id=row["order_id"], line_number=number,
                    product_id=identifier(item["product_id"]), quantity=count, unit_price=price,
                    line_amount=price * count, quality_flags=[])
            for key, old in current["order_items"].items():
                if old["order_id"] == row["order_id"] and key not in active:
                    old.update(metadata, is_deleted=True)
    tables = {name: list(rows.values()) for name, rows in current.items()}
    customers = {row["customer_id"] for row in tables["customers"] if not row["is_deleted"]}
    products = {row["product_id"] for row in tables["products"] if not row["is_deleted"]}
    orders = {row["order_id"] for row in tables["orders"] if not row["is_deleted"]}
    for row in tables["orders"]:
        if not row["is_deleted"] and row["customer_id"] not in customers:
            row["quality_flags"].append("customer_not_found")
        if not row["is_deleted"] and row.get("_items"):
            total = sum((decimal_value(item["price"]) * quantity(item["quantity"]) for item in row["_items"]), Decimal(0))
            if total != row["total_amount"]:
                row["quality_flags"].append("item_total_mismatch")
    for entity in ("order_items", "inventory"):
        for row in tables[entity]:
            if not row["is_deleted"] and row["product_id"] not in products:
                row["quality_flags"].append("product_not_found")
    for entity in ("payments", "fraud_assessments", "shipments"):
        for row in tables[entity]:
            if row["order_id"] not in orders:
                row["quality_flags"].append("order_not_found")
    tables["event_history"] = history
    return tables


def schemas():
    import pyarrow as pa
    text, money, stamp = pa.string(), pa.decimal128(18, 2), pa.timestamp("us", tz="UTC")
    common = [("source_system", text), ("source_event_id", text), ("event_at", stamp), ("source_object", text),
              ("source_sequence", pa.int64()), ("is_deleted", pa.bool_()), ("is_simulated", pa.bool_())]
    fields = {
        "customers": [("customer_id", text), ("name", text), ("email", text)],
        "products": [("product_id", text), ("name", text), ("price", money), ("stock", pa.int64())],
        "orders": [("order_id", text), ("customer_id", text), ("total_amount", money), ("currency", text),
                   ("payment_method", text), ("status", text), ("ordered_at", stamp), ("ip_address", text), ("device_id", text)],
        "order_items": [("order_id", text), ("line_number", pa.int64()), ("product_id", text), ("quantity", pa.int64()), ("unit_price", money), ("line_amount", money)],
        "payments": [("payment_id", text), ("order_id", text), ("status", text), ("amount", money), ("currency", text), ("method", text), ("processed_at", stamp)],
        "fraud_assessments": [("order_id", text), ("status_code", text), ("score", money), ("observed_at", stamp)],
        "inventory": [("product_id", text), ("warehouse_id", text), ("physical_stock", pa.decimal128(18, 4)), ("virtual_stock", pa.decimal128(18, 4)), ("stock_at", stamp)],
        "shipments": [("shipment_id", text), ("order_id", text), ("status", text), ("tracking_code", text), ("carrier", text), ("protocol", text), ("posted_at", stamp), ("delivered_at", stamp)],
    }
    result = {name: pa.schema(items + common + [("quality_flags", pa.list_(text))]) for name, items in fields.items()}
    result["event_history"] = pa.schema([("entity", text), ("entity_id", text), ("record_json", text)] + common)
    return result
