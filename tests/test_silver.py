import copy
import io
import json
import unittest
from decimal import Decimal

from botocore.exceptions import ClientError
from pipelines import model
from pipelines.samples import documents
from pipelines.silver import build_silver, validate_snapshot
from pipelines.storage import json_bytes


class MemoryS3:
    def __init__(self):
        self.data = {"bronze": {}}
        self.fail_table = None

    def head_bucket(self, Bucket):
        if Bucket not in self.data:
            raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    def create_bucket(self, Bucket):
        self.data[Bucket] = {}

    def put_bucket_versioning(self, **kwargs):
        pass

    def head_object(self, Bucket, Key):
        if Key not in self.data[Bucket]:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.data[Bucket][Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail_table and self.fail_table in Key:
            raise OSError("Simulated write failure")
        self.data[Bucket][Key] = Body

    def get_paginator(self, operation):
        return self

    def paginate(self, Bucket, Prefix):
        return [{"Contents": [{"Key": key} for key in self.data[Bucket] if key.startswith(Prefix)]}]


def product(offset, price, deleted=False, stamp="2026-10-20T12:00:00Z"):
    row = model.base_record("products", {"id": "P1", "name": "Product", "price": price, "stock": 1}, stamp, deleted)
    return model.make_event("products", row, "postgresql", "event-" + str(offset), stamp, "test.json", offset, deleted, priority=3)


class SilverTests(unittest.TestCase):
    def test_cdc_decimal_and_offsets_override_clock_regression(self):
        schema = {"fields": [{"field": "after", "fields": [{"field": "price", "name": "org.apache.kafka.connect.data.Decimal", "parameters": {"scale": "2"}}]}]}
        self.assertEqual(model.cdc_money("A+g=", schema, "price"), Decimal("10.00"))
        events = [product(2, "20.00", stamp="2026-10-19T12:00:00Z"), product(1, "10.00")]
        self.assertEqual(model.materialize(events)["products"][0]["price"], Decimal("20.00"))

    def test_delete_preserves_previous_values_and_history(self):
        first = product(1, "10.00")
        deleted = product(2, None, deleted=True)
        tables = model.materialize([deleted, first])
        self.assertTrue(tables["products"][0]["is_deleted"])
        self.assertEqual(tables["products"][0]["price"], Decimal("10.00"))
        self.assertEqual(len(tables["event_history"]), 2)

    def test_postgres_jsonb_items_are_normalized_from_string(self):
        row = {"id": "O1", "customer_id": "C1", "total_amount": "10.00",
               "items": '[{"product_id":"P1","quantity":1,"price":10}]'}
        normalized = model.base_record("orders", row, "2026-10-20T12:00:00Z")
        self.assertEqual(normalized["_items"][0]["product_id"], "P1")

    def test_duplicate_event_is_idempotent_but_conflicts_fail(self):
        first = product(1, "10.00")
        duplicate = copy.deepcopy(first)
        duplicate["source_object"] = "another-object.json"
        self.assertEqual(len(model.materialize([first, duplicate])["event_history"]), 1)
        duplicate["row"]["price"] = Decimal("99.00")
        with self.assertRaises(ValueError):
            model.materialize([first, duplicate])

    def test_model_grains_money_and_relationships(self):
        events = [event for doc in documents() for event in model.external_events(doc, doc["dataset"] + ".json")]
        tables = model.materialize(events)
        self.assertEqual({name: len(rows) for name, rows in tables.items()}, {
            "customers": 2, "products": 3, "orders": 3, "order_items": 4,
            "payments": 3, "fraud_assessments": 3, "inventory": 6, "shipments": 2, "event_history": 22})
        self.assertFalse(any(row.get("quality_flags") for rows in tables.values() for row in rows))
        first_order = next(row for row in tables["orders"] if row["order_id"] == "ORD-DEMO-001")
        self.assertEqual(first_order["total_amount"], Decimal("379.70"))

    def test_conflicting_business_key_in_same_export_fails(self):
        doc = next(doc for doc in documents() if doc["dataset"] == "products")
        conflict = dict(doc["body"][0], price="999.00")
        doc["body"].append(conflict)
        with self.assertRaises(ValueError):
            list(model.external_events(doc, "conflict.json"))

    def test_removed_order_items_are_retained_as_deleted(self):
        order = next(doc for doc in documents() if doc["dataset"] == "orders")
        first = list(model.external_events(order, "old.json"))[0]
        changed = copy.deepcopy(first)
        changed["source_event_id"] = "new-order-event"
        changed["event_at"] = model.instant("2026-10-21T12:00:00Z")
        changed["row"]["_items"] = changed["row"]["_items"][:1]
        items = model.materialize([changed, first])["order_items"]
        self.assertEqual(len(items), 2)
        self.assertEqual([row["line_number"] for row in items if row["is_deleted"]], [2])

    def store(self):
        s3 = MemoryS3()
        for doc in documents():
            key = "external/{}/{}.json".format(doc["source_system"], doc["dataset"])
            s3.data["bronze"][key] = json_bytes(doc)
        return s3

    def test_snapshot_idempotency_schema_and_readback_integrity(self):
        s3 = self.store()
        bronze = copy.deepcopy(s3.data["bronze"])
        first = build_silver(s3)
        second = build_silver(s3)
        self.assertEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertTrue(second["reused"])
        del s3.data["silver"]["LATEST.json"]
        self.assertTrue(build_silver(s3)["reused"])
        self.assertIn("LATEST.json", s3.data["silver"])
        self.assertEqual(bronze, s3.data["bronze"])
        self.assertTrue(validate_snapshot(s3=s3)["passed"])
        key = "snapshots/" + first["snapshot_id"] + "/orders/part-00000.parquet"
        s3.data["silver"][key] = b"corrupted"
        with self.assertRaises(ValueError):
            validate_snapshot(s3=s3)

    def test_partial_write_and_bad_input_never_replace_latest(self):
        s3 = self.store()
        build_silver(s3)
        latest = s3.data["silver"]["LATEST.json"]
        changed = documents()[1]
        changed["body"][0]["price"] = "200.00"
        changed["extracted_at"] = "2026-10-21T12:00:00Z"
        s3.data["bronze"]["external/shopvibe_demo/new-products.json"] = json_bytes(changed)
        s3.fail_table = "/orders/"
        with self.assertRaises(OSError):
            build_silver(s3)
        self.assertEqual(latest, s3.data["silver"]["LATEST.json"])
        s3.fail_table = None
        s3.data["bronze"]["external/bad.json"] = b'{"contract_version": 99}'
        with self.assertRaises(ValueError):
            build_silver(s3)
        self.assertEqual(latest, s3.data["silver"]["LATEST.json"])
        self.assertTrue(any(key.startswith("quarantine/") for key in s3.data["silver"]))


if __name__ == "__main__":
    unittest.main()
