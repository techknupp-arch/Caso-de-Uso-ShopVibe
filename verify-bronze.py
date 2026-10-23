"""Read-only comparison of retained Kafka event payloads with MinIO Bronze."""
import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
METADATA = {"_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp"}


def kafka_snapshot(topics):
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        group_id=None, enable_auto_commit=False,
    )
    try:
        available = consumer.topics()
        partitions = [TopicPartition(topic, partition) for topic in topics if topic in available
                      for partition in consumer.partitions_for_topic(topic)]
        if not partitions:
            raise RuntimeError("Nenhum topico monitorado disponivel")
        consumer.assign(partitions)
        consumer.seek_to_beginning(*partitions)
        ends = consumer.end_offsets(partitions)
        events, tombstones = {}, 0
        deadline = time.monotonic() + 60
        while any(consumer.position(partition) < ends[partition] for partition in partitions):
            if time.monotonic() > deadline:
                raise RuntimeError("Leitura do Kafka nao terminou; auditoria incompleta")
            for partition, records in consumer.poll(timeout_ms=1000).items():
                for record in records:
                    if record.offset >= ends[partition]:
                        continue
                    if record.value is None:
                        tombstones += 1
                        continue
                    key = (record.topic, record.partition, record.offset)
                    events[key] = json.loads(record.value.decode("utf-8"))
        return events, tombstones, {f"{tp.topic}:{tp.partition}": end for tp, end in ends.items()}
    finally:
        consumer.close()


def bronze_snapshot(s3, topics):
    events, counts, objects = {}, Counter(), []
    paginator = s3.get_paginator("list_objects_v2")
    for topic in topics:
        for page in paginator.paginate(Bucket="bronze", Prefix=f"topics/{topic}/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                objects.append(key)
                body = s3.get_object(Bucket="bronze", Key=key)["Body"].read().decode("utf-8")
                for line in body.splitlines():
                    record = json.loads(line)
                    identity = (record["_kafka_topic"], record["_kafka_partition"], record["_kafka_offset"])
                    counts[identity] += 1
                    events[identity] = {k: v for k, v in record.items() if k not in METADATA}
    return events, counts, objects


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-seconds", type=int, default=90)
    parser.add_argument("--report", type=Path, default=ROOT / ".local-backup/bronze-validation.json")
    args = parser.parse_args()
    config = json.loads((ROOT / "conectores/connector-minio-bronze.json").read_text())["config"]
    topics = config["topics"].split(",")
    expected, tombstones, ends = kafka_snapshot(topics)
    s3 = boto3.client(
        "s3", endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER") or "admin",
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin",
        region_name="us-east-1",
    )
    deadline = time.monotonic() + args.wait_seconds
    while True:
        actual, counts, objects = bronze_snapshot(s3, topics)
        missing = sorted(set(expected) - set(actual))
        mismatches = sorted(key for key in expected.keys() & actual.keys() if expected[key] != actual[key])
        if (not missing and not mismatches) or time.monotonic() >= deadline:
            break
        time.sleep(2)
    duplicates = sorted(key for key, count in counts.items() if count > 1 and key in expected)
    report = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "bucket": "bronze", "expected_events": len(expected), "bronze_events": len(actual),
        "objects": len(objects), "tombstones_excluded": tombstones, "kafka_end_offsets": ends,
        "missing": missing, "payload_mismatches": mismatches, "duplicates": duplicates,
        "events_per_topic": dict(Counter(key[0] for key in expected)),
        "passed": bool(expected) and not missing and not mismatches and not duplicates,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
