import hashlib
import json
import os
from pathlib import Path

ROOT = Path(os.getenv("SHOPVIBE_PROJECT_ROOT", str(Path(__file__).resolve().parent.parent)))


def client():
    import boto3
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    return boto3.client("s3", endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
                        aws_access_key_id=os.getenv("MINIO_ROOT_USER") or "admin",
                        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin",
                        region_name="us-east-1")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")


def objects(s3, bucket, prefix):
    return sorted([item["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                   for item in page.get("Contents", [])])


def ensure_bucket(s3, bucket):
    from botocore.exceptions import ClientError
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        if str(error.response["Error"]["Code"]) not in ("404", "NoSuchBucket", "NotFound"):
            raise
        s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})


def ingest_sources(sources=None, s3=None):
    """Copy provider JSONs byte-for-byte into append-only, content-addressed Bronze keys."""
    # Bling and Melhor Envio are collected through Airbyte, never copied around it.
    if sources is None:
        sources = ["shopvibe_demo", "clearsale", "payment_api"]
    if set(sources) & {"bling", "melhor_envio"}:
        raise ValueError("Bling e Melhor Envio devem ser coletados pelo Airbyte. Execute a DAG shopvibe_servicos_4h.")
    s3 = s3 or client()
    ensure_bucket(s3, "bronze")
    existing = set(objects(s3, "bronze", "external/"))
    report = {"uploaded": 0, "unchanged": 0, "objects": []}
    for path in sorted((ROOT / "samples/services").rglob("*.json")):
        raw = path.read_bytes()
        doc = json.loads(raw)
        if doc["source_system"] not in sources:
            continue
        if doc.get("simulated") is not True or doc.get("contract_version") != 1:
            raise ValueError("Contrato de simulacao invalido: " + str(path))
        # Source/dataset are restricted to the implemented contracts, never arbitrary object paths.
        allowed = {("shopvibe_demo", "customers"), ("shopvibe_demo", "products"), ("shopvibe_demo", "orders"),
                   ("clearsale", "fraud_assessments"), ("payment_api", "payments"), ("bling", "inventory"), ("melhor_envio", "shipments")}
        if (doc["source_system"], doc["dataset"]) not in allowed:
            raise ValueError("Fonte/dataset nao suportado: " + str(path))
        key = "external/{}/{}/{}.json".format(doc["source_system"], doc["dataset"], digest(raw))
        if key in existing:
            # Detect external corruption rather than silently reusing an invalid object.
            if digest(s3.get_object(Bucket="bronze", Key=key)["Body"].read()) != digest(raw):
                raise RuntimeError("Objeto Bronze diverge do hash: " + key)
            report["unchanged"] += 1
        else:
            s3.put_object(Bucket="bronze", Key=key, Body=raw, ContentType="application/json")
            report["uploaded"] += 1
            existing.add(key)
        report["objects"].append(key)
    if not report["objects"]:
        raise ValueError("Nenhum arquivo encontrado para as fontes selecionadas")
    return report
