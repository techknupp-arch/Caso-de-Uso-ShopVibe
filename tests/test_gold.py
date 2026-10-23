import copy
import io
import json
import unittest
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines import gold_model, model
from pipelines.gold import build_gold, validate_snapshot
from pipelines.samples import documents
from pipelines.silver import build_silver
from pipelines.storage import digest, json_bytes
from test_silver import MemoryS3


def source_tables():
    return model.materialize([event for doc in documents() for event in model.external_events(doc, doc["dataset"] + ".json")])


class GoldTests(unittest.TestCase):
    def store(self):
        s3 = MemoryS3()
        for doc in documents():
            s3.data["bronze"]["external/{}/{}.json".format(doc["source_system"], doc["dataset"])] = json_bytes(doc)
        build_silver(s3)
        return s3

    def change_orders(self, s3, transform):
        doc = next(doc for doc in documents() if doc["dataset"] == "orders")
        transform(doc["body"])
        doc["extracted_at"] = "2026-10-21T12:00:00Z"
        s3.data["bronze"]["external/shopvibe_demo/new-orders.json"] = json_bytes(doc)
        return build_silver(s3)

    def test_grains_and_daily_totals_do_not_repeat_order_amount_per_item(self):
        tables = gold_model.materialize(source_tables())
        self.assertEqual({name: len(rows) for name, rows in tables.items()}, {
            "dim_cliente": 2, "dim_produto": 3, "dim_data": 2, "fato_pedido": 3,
            "fato_item_pedido": 4, "vendas_diarias": 2, "estoque_atual": 6})
        approved = next(row for row in tables["vendas_diarias"] if row["status"] == "APPROVED")
        self.assertEqual(approved["quantidade_pedidos"], 2)
        self.assertEqual(approved["quantidade_unidades"], 4)
        self.assertEqual(approved["valor_pedidos"], Decimal("679.60"))
        self.assertEqual(approved["ticket_medio"], Decimal("339.80"))
        self.assertEqual(sum(row["valor_pedidos"] for row in tables["vendas_diarias"]), Decimal("769.50"))
        self.assertEqual(sum(row["valor_item"] for row in tables["fato_item_pedido"]), Decimal("769.50"))
        self.assertFalse(gold_model.validate(tables, "America/Sao_Paulo")["orders_without_items"])
        self.assertNotIn("valor_pedido", tables["fato_item_pedido"][0])

    def test_business_day_uses_sao_paulo_and_keeps_currency_status_and_simulation_separate(self):
        source = source_tables()
        source["orders"][0]["ordered_at"] = model.instant("2026-10-20T01:30:00Z")
        source["orders"][1]["ordered_at"] = model.instant("2026-10-20T03:01:00Z")
        source["orders"][1]["currency"] = "USD"
        source["orders"][1]["is_simulated"] = False
        for row in source["order_items"]:
            if row["order_id"] == "ORD-DEMO-002":
                row["is_simulated"] = False
        source["orders"][2]["status"] = "CANCELED"
        tables = gold_model.materialize(source)
        facts = {row["pedido_id"]: row for row in tables["fato_pedido"]}
        self.assertEqual(facts["ORD-DEMO-001"]["data_id"], 20261019)
        self.assertEqual(facts["ORD-DEMO-002"]["data_id"], 20261020)
        self.assertEqual(len(tables["vendas_diarias"]), 3)
        canceled = next(row for row in tables["vendas_diarias"] if row["status"] == "CANCELED")
        self.assertEqual(canceled["valor_pedidos"], Decimal("89.90"))
        with self.assertRaisesRegex(ValueError, "Fuso"):
            gold_model.materialize(source, "Invalid/Timezone")

    def test_deleted_orders_remove_their_facts_and_missing_dimensions_do_not_drop_sales(self):
        source = source_tables()
        source["orders"][0]["is_deleted"] = True
        source["customers"][0]["is_deleted"] = True
        source["customers"] = [row for row in source["customers"] if row["customer_id"] != "CUS-DEMO-002"]
        source["products"] = [row for row in source["products"] if row["product_id"] != "SKU-DEMO-003"]
        tables = gold_model.materialize(source)
        self.assertEqual(len(tables["fato_pedido"]), 2)
        self.assertEqual(len(tables["fato_item_pedido"]), 2)
        self.assertEqual(sum(row["valor_pedidos"] for row in tables["vendas_diarias"]), Decimal("389.80"))
        quality = gold_model.validate(tables, "America/Sao_Paulo")
        self.assertEqual(quality["unknown_customers"], 1)
        self.assertEqual(quality["unknown_products"], 1)
        self.assertTrue(next(row for row in tables["dim_cliente"] if row["cliente_id"] == "CUS-DEMO-001")["is_deleted"])
        for row in source["orders"]:
            row["is_deleted"] = True
        empty = gold_model.materialize(source)
        self.assertEqual(empty["fato_pedido"], [])
        self.assertEqual(empty["fato_item_pedido"], [])
        self.assertEqual(empty["vendas_diarias"], [])

    def test_missing_item_detail_is_visible_but_partial_inconsistent_detail_is_rejected(self):
        source = source_tables()
        source["order_items"] = [row for row in source["order_items"] if row["order_id"] != "ORD-DEMO-001"]
        tables = gold_model.materialize(source)
        self.assertEqual(len(tables["fato_pedido"]), 3)
        self.assertEqual(gold_model.validate(tables, "America/Sao_Paulo")["orders_without_items"], 1)
        source = source_tables()
        source["order_items"].pop(0)
        with self.assertRaisesRegex(ValueError, "Total do pedido"):
            gold_model.materialize(source)

    def test_pinned_snapshots_idempotency_and_source_layers_are_preserved(self):
        s3 = self.store()
        before = copy.deepcopy(s3.data)
        first = build_gold(s3=s3)
        self.assertTrue(validate_snapshot(s3=s3)["passed"])
        self.assertTrue(build_gold(s3=s3)["reused"])
        self.assertEqual(s3.data["bronze"], before["bronze"])
        self.assertEqual(s3.data["silver"], before["silver"])
        del s3.data["gold"]["LATEST.json"]
        self.assertTrue(build_gold(s3=s3)["reused"])
        self.change_orders(s3, lambda rows: rows[0].update(status="CANCELED"))
        pinned = build_gold(first["silver_snapshot_id"], s3=s3)
        self.assertEqual(pinned["snapshot_id"], first["snapshot_id"])
        newer = build_gold(s3=s3)
        self.assertNotEqual(newer["snapshot_id"], first["snapshot_id"])
        utc = build_gold(s3=s3, business_timezone="UTC")
        self.assertNotEqual(utc["snapshot_id"], newer["snapshot_id"])
        self.assertTrue(validate_snapshot(first["snapshot_id"], s3)["passed"])

    def test_partial_write_and_rejected_input_preserve_previous_gold(self):
        s3 = self.store()
        build_gold(s3=s3)
        latest = s3.data["gold"]["LATEST.json"]
        self.change_orders(s3, lambda rows: rows[0].update(status="CANCELED"))
        s3.fail_table = "/fato_item_pedido/"
        with self.assertRaises(OSError):
            build_gold(s3=s3)
        self.assertEqual(s3.data["gold"]["LATEST.json"], latest)
        s3.fail_table = None
        self.change_orders(s3, lambda rows: rows[0].update(total_amount="999.00"))
        with self.assertRaisesRegex(ValueError, "LATEST preservado"):
            build_gold(s3=s3)
        self.assertEqual(s3.data["gold"]["LATEST.json"], latest)
        self.assertTrue(any(key.startswith("quarantine/") for key in s3.data["gold"]))

    def test_corruption_is_detected_and_does_not_rewrite_a_completed_snapshot(self):
        s3 = self.store()
        built = build_gold(s3=s3)
        latest = s3.data["gold"]["LATEST.json"]
        key = "snapshots/" + built["snapshot_id"] + "/fato_pedido/part-00000.parquet"
        original = s3.data["gold"][key]
        s3.data["gold"][key] = b"corrupted"
        with self.assertRaisesRegex(ValueError, "Checksum Gold"):
            validate_snapshot(s3=s3)
        with self.assertRaises(ValueError):
            build_gold(s3=s3)
        self.assertEqual(s3.data["gold"][key], b"corrupted")
        self.assertEqual(s3.data["gold"]["LATEST.json"], latest)
        s3.data["gold"][key] = original
        silver_key = "snapshots/" + built["silver_snapshot_id"] + "/orders/part-00000.parquet"
        s3.data["silver"][silver_key] = b"corrupted"
        with self.assertRaisesRegex(ValueError, "Checksum Silver"):
            build_gold(s3=s3)
        self.assertEqual(s3.data["gold"]["LATEST.json"], latest)

    def test_semantic_reconciliation_catches_wrong_daily_counts_even_with_matching_checksum(self):
        s3 = self.store()
        built = build_gold(s3=s3)
        prefix = "snapshots/" + built["snapshot_id"] + "/"
        key = prefix + "vendas_diarias/part-00000.parquet"
        rows = pq.read_table(io.BytesIO(s3.data["gold"][key])).to_pylist()
        rows[0]["quantidade_pedidos"] += 1
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=gold_model.schemas()["vendas_diarias"]), buffer)
        s3.data["gold"][key] = buffer.getvalue()
        manifest = json.loads(s3.data["gold"][prefix + "manifest.json"])
        manifest["tables"]["vendas_diarias"]["sha256"] = digest(buffer.getvalue())
        s3.data["gold"][prefix + "manifest.json"] = json_bytes(manifest)
        with self.assertRaisesRegex(ValueError, "Agregacao diaria"):
            validate_snapshot(s3=s3)


if __name__ == "__main__":
    unittest.main()
