"""Build immutable Parquet snapshots; publish LATEST only after read-back validation."""
import io
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

from . import model
from .storage import client, digest, ensure_bucket, json_bytes, objects

TOPICS = ("shopvibe.shopvibe.customers", "shopvibe.shopvibe.products", "shopvibe.shopvibe.orders", "shopvibe.orders.raw")
MODEL_VERSION = "1"


def airbyte_documents(raw, key):
    """Decode the native S3 JSONL output without stripping the Bronze metadata on disk."""
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    count = 0
    for line in raw.decode("utf-8-sig").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        doc = record.get("_airbyte_data")
        if isinstance(doc, str):
            doc = json.loads(doc)
        if not isinstance(doc, dict):
            raise ValueError("Registro Airbyte sem _airbyte_data valido")
        if (doc.get("source_system"), doc.get("dataset")) not in (("bling", "inventory"), ("melhor_envio", "shipments")):
            raise ValueError("Fonte Airbyte fora do contrato Silver")
        count += 1
        yield doc
    if not count:
        raise ValueError("Objeto Airbyte sem registros")


def validate_snapshot(snapshot_id=None, s3=None):
    import pyarrow.parquet as pq
    s3 = s3 or client()
    if snapshot_id is None:
        snapshot_id = json.loads(s3.get_object(Bucket="silver", Key="LATEST.json")["Body"].read())["snapshot_id"]
    if not isinstance(snapshot_id, str) or len(snapshot_id) != 24 or any(c not in "0123456789abcdef" for c in snapshot_id):
        raise ValueError("Snapshot invalido")
    manifest = json.loads(s3.get_object(Bucket="silver", Key="snapshots/" + snapshot_id + "/manifest.json")["Body"].read())
    contracts = model.schemas()
    if manifest["snapshot_id"] != snapshot_id or set(manifest["tables"]) != set(contracts):
        raise ValueError("Manifesto Silver incompleto")
    for entity, info in manifest["tables"].items():
        raw = s3.get_object(Bucket="silver", Key=info["key"])["Body"].read()
        if digest(raw) != info["sha256"]:
            raise ValueError("Checksum Silver divergente: " + entity)
        table = pq.read_table(io.BytesIO(raw))
        if table.num_rows != info["rows"] or not table.schema.equals(contracts[entity]):
            raise ValueError("Contagem/schema Silver divergente: " + entity)
        if entity in model.KEYS:
            keys = [tuple(row[field] for field in model.KEYS[entity]) for row in table.to_pylist()]
            if len(keys) != len(set(keys)) or any(None in key for key in keys):
                raise ValueError("Chave primaria Silver invalida: " + entity)
    return {"snapshot_id": snapshot_id, "tables": {name: info["rows"] for name, info in manifest["tables"].items()},
            "quality_warnings": manifest["quality_warnings"], "passed": True}


def build_silver(s3=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.exceptions import ClientError
    s3 = s3 or client()
    ensure_bucket(s3, "silver")
    keys = [key for key in objects(s3, "bronze", "external/") if key.endswith(".json")]
    keys.extend(key for key in objects(s3, "bronze", "airbyte/") if key.endswith((".jsonl", ".jsonl.gz")))
    for topic in TOPICS:
        keys.extend(key for key in objects(s3, "bronze", "topics/" + topic + "/") if key.endswith(".json"))
    events, inputs, rejects = [], [], []
    for key in sorted(keys):
        raw = s3.get_object(Bucket="bronze", Key=key)["Body"].read()
        inputs.append({"key": key, "sha256": digest(raw)})
        try:
            if key.startswith("external/"):
                events.extend(model.external_events(json.loads(raw), key))
            elif key.startswith("airbyte/"):
                for doc in airbyte_documents(raw, key):
                    events.extend(model.external_events(doc, key))
            else:
                for line in raw.decode("utf-8").splitlines():
                    events.append(model.kafka_event(json.loads(line), key))
        except (ValueError, KeyError, TypeError, OverflowError, ArithmeticError, OSError, EOFError) as error:
            rejects.append({"source_object": key, "reason": str(error)})
    code_hash = digest((Path(model.__file__).read_bytes() + Path(__file__).read_bytes()))
    snapshot_id = digest(json_bytes({"model_version": MODEL_VERSION, "code_sha256": code_hash, "inputs": inputs}))[:24]
    try:
        tables = model.materialize(events)
    except (ValueError, KeyError, TypeError, ArithmeticError) as error:
        rejects.append({"source_object": "multiple", "reason": str(error)})
    if rejects:
        s3.put_object(Bucket="silver", Key="quarantine/" + snapshot_id + "/rejections.json",
                      Body=json_bytes({"inputs": inputs, "rejections": rejects}), ContentType="application/json")
        raise ValueError("Registros em quarentena; LATEST preservado: " + snapshot_id)
    if not events:
        raise ValueError("Nenhum evento Bronze encontrado; Silver nao publicada")

    prefix = "snapshots/" + snapshot_id + "/"
    try:
        s3.head_object(Bucket="silver", Key=prefix + "manifest.json")
        result = validate_snapshot(snapshot_id, s3)
        # Recover a crash between validated manifest creation and pointer publication.
        s3.put_object(Bucket="silver", Key="LATEST.json", Body=json_bytes({"snapshot_id": snapshot_id,
                      "manifest": prefix + "manifest.json"}), ContentType="application/json")
        return dict(result, reused=True)
    except ClientError as error:
        if str(error.response["Error"]["Code"]) not in ("404", "NoSuchKey", "NotFound"):
            raise

    manifest = {"snapshot_id": snapshot_id, "model_version": MODEL_VERSION, "code_sha256": code_hash,
                "created_at": datetime.now(timezone.utc).isoformat(), "inputs": inputs, "tables": {},
                "quality_warnings": {name: sum(bool(row.get("quality_flags")) for row in rows) for name, rows in tables.items()}}
    for entity, rows in tables.items():
        table = pa.Table.from_pylist(rows, schema=model.schemas()[entity])
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy", version="2.4")
        raw = buffer.getvalue()
        key = prefix + entity + "/part-00000.parquet"
        s3.put_object(Bucket="silver", Key=key, Body=raw, ContentType="application/octet-stream")
        manifest["tables"][entity] = {"key": key, "rows": len(rows), "sha256": digest(raw)}
    s3.put_object(Bucket="silver", Key=prefix + "manifest.json", Body=json_bytes(manifest), ContentType="application/json")
    result = validate_snapshot(snapshot_id, s3)
    # Single pointer publication after every table has been downloaded and verified.
    s3.put_object(Bucket="silver", Key="LATEST.json", Body=json_bytes({"snapshot_id": snapshot_id,
                  "manifest": prefix + "manifest.json"}), ContentType="application/json")
    return dict(result, reused=False)
