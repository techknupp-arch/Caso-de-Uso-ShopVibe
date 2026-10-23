"""Opt-in real API -> PostgreSQL -> CDC -> Kafka -> Bronze test. Keeps its demo data."""
import json
import os
import time
import unittest
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
from kafka import KafkaConsumer, TopicPartition
import psycopg


@unittest.skipUnless(os.getenv("SHOPVIBE_INTEGRATION") == "1", "requires the local Docker stack")
class CheckoutIntegrationTests(unittest.TestCase):
    def test_checkout_is_committed_once_and_reaches_bronze_only_through_cdc(self):
        suffix = uuid.uuid4().hex[:12]
        customer, product, order = (prefix + suffix for prefix in ("CLI-CHECKOUT-", "PROD-CHECKOUT-", "PED-CHECKOUT-"))
        db_args = dict(host=os.getenv("POSTGRES_HOST", "postgres"), port=int(os.getenv("POSTGRES_PORT", "5432")),
                       dbname=os.getenv("POSTGRES_DB", "shopvibe_db"), user=os.environ["POSTGRES_USER"],
                       password=os.environ["POSTGRES_PASSWORD"], connect_timeout=5)
        with psycopg.connect(**db_args) as db:
            db.execute("INSERT INTO shopvibe.customers (id, name) VALUES (%s, %s)", (customer, "Cliente teste checkout CDC"))
            db.execute("INSERT INTO shopvibe.products (id, name, price, stock) VALUES (%s, %s, %s, %s)",
                       (product, "Produto teste checkout CDC", "10.00", 10))
        body = dict(order_id=order, customer_id=customer, payment_method="pix", total_amount="20.00",
                    items=[dict(product_id=product, quantity=2, price="10.00")])

        def checkout(data):
            request = Request(os.getenv("CHECKOUT_TEST_URL", "http://localhost:8000/api/v1/checkout"),
                              data=json.dumps(data).encode(), headers={"Content-Type": "application/json"})
            try:
                with urlopen(request, timeout=15) as response:
                    return response.status, json.load(response)
            except HTTPError as error:
                return error.code, json.load(error)

        kafka = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:29092"),
                              group_id=None, enable_auto_commit=False)
        self.addCleanup(kafka.close)
        raw_partitions = [TopicPartition("shopvibe.orders.raw", p)
                          for p in kafka.partitions_for_topic("shopvibe.orders.raw") or []]
        raw_before = kafka.end_offsets(raw_partitions) if raw_partitions else {}
        code, first = checkout(body)
        self.assertEqual(code, 201, first)
        self.assertFalse(first["replayed"])
        self.assertEqual(first["topic"], "shopvibe.shopvibe.orders")
        with psycopg.connect(**db_args) as db:
            row = db.execute("SELECT customer_id, items, total_amount, ip_address, device_id FROM shopvibe.orders WHERE id=%s", (order,)).fetchone()
            self.assertIsNotNone(row, "HTTP success must follow a committed database write")
            self.assertEqual(row[0], customer)
            self.assertEqual(row[1][0]["quantity"], 2)
            self.assertEqual(str(row[2]), "20.00")
            self.assertEqual(row[3:], ("127.0.0.1", "web_browser"))
        code, repeated = checkout({**body, "total_amount": 20, "items": [dict(product_id=product, quantity=2, price=10)]})
        self.assertEqual(code, 200, repeated)
        self.assertTrue(repeated["replayed"])
        self.assertEqual(first["data"], repeated["data"])
        self.assertEqual(checkout({**body, "total_amount": 30, "items": [dict(product_id=product, quantity=3, price=10)]})[0], 409)
        self.assertEqual(checkout({**body, "order_id": order + "-bad-customer", "customer_id": customer + "-missing"})[0], 404)
        self.assertEqual(checkout({**body, "order_id": order + "-bad-product", "items": [dict(product_id=product + "-missing", quantity=2, price=10)]})[0], 422)
        self.assertEqual(checkout({**body, "order_id": order + "-bad-total", "total_amount": 21})[0], 422)

        s3 = boto3.client("s3", endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                          aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                          aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"], region_name="us-east-1")
        matches = []
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            matches = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket="bronze", Prefix="topics/shopvibe.shopvibe.orders/"):
                for obj in page.get("Contents", []):
                    for line in s3.get_object(Bucket="bronze", Key=obj["Key"])["Body"].read().splitlines():
                        record = json.loads(line)
                        after = record.get("payload", {}).get("after") or {}
                        if after.get("id") == order:
                            matches.append((obj["Key"], record))
            if matches:
                break
            time.sleep(2)
        self.assertEqual(len(matches), 1, "One new checkout must produce one persisted CDC create event")
        key, event = matches[0]
        self.assertEqual(event["payload"]["op"], "c")
        self.assertEqual(event["payload"]["after"]["customer_id"], customer)
        self.assertEqual(kafka.end_offsets(raw_partitions) if raw_partitions else {}, raw_before,
                         "Checkout must not publish to the legacy raw topic")
        with psycopg.connect(**db_args) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM shopvibe.orders WHERE id = ANY(%s)",
                                        ([order, order + "-bad-customer", order + "-bad-product", order + "-bad-total"],)).fetchone()[0], 1)
        print(json.dumps({"order_id": order, "bronze_key": key, "cdc_create_events": 1,
                          "legacy_raw_unchanged": True, "passed": True}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
