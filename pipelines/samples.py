"""Synthetic exports: provider-like payloads plus explicit integration metadata."""
import argparse
import json
from pathlib import Path


def documents():
    stamp = "2026-10-20T12:00:00Z"
    def wrap(source, dataset, body, references=None):
        return {"contract_version": 1, "source_system": source, "dataset": dataset,
                "simulated": True, "extracted_at": stamp, "references": references or {}, "body": body}
    customers = [{"id": "CUS-DEMO-001", "name": "Cliente demonstracao A", "email": "cliente-a@example.invalid"},
                 {"id": "CUS-DEMO-002", "name": "Cliente demonstracao B", "email": "cliente-b@example.invalid"}]
    products = [{"id": "SKU-DEMO-001", "name": "Teclado demonstracao", "price": "199.90", "stock": 20},
                {"id": "SKU-DEMO-002", "name": "Mouse demonstracao", "price": "89.90", "stock": 50},
                {"id": "SKU-DEMO-003", "name": "Headset demonstracao", "price": "299.90", "stock": 10}]
    orders = [{"id": "ORD-DEMO-001", "customer_id": "CUS-DEMO-001", "total_amount": "379.70",
               "payment_method": "credit_card", "status": "APPROVED", "created_at": "2026-10-19T08:00:00Z",
               "items": [{"product_id": "SKU-DEMO-001", "quantity": 1, "price": "199.90"},
                         {"product_id": "SKU-DEMO-002", "quantity": 2, "price": "89.90"}]},
              {"id": "ORD-DEMO-002", "customer_id": "CUS-DEMO-002", "total_amount": "299.90",
               "payment_method": "pix", "status": "APPROVED", "created_at": "2026-10-19T08:00:00Z",
               "items": [{"product_id": "SKU-DEMO-003", "quantity": 1, "price": "299.90"}]},
              {"id": "ORD-DEMO-003", "customer_id": "CUS-DEMO-001", "total_amount": "89.90",
               "payment_method": "credit_card", "status": "PENDING", "created_at": "2026-10-19T08:00:00Z",
               "items": [{"product_id": "SKU-DEMO-002", "quantity": 1, "price": "89.90"}]}]
    assessments = {"packageID": "PACKAGE-DEMO-001", "orders": [
        {"code": "ORD-DEMO-001", "status": "APA", "score": 12.50},
        {"code": "ORD-DEMO-002", "status": "APA", "score": 4.10},
        {"code": "ORD-DEMO-003", "status": "NVO", "score": None}]}
    payments = {"payments": [
        {"id": "PAY-DEMO-001", "order_id": "ORD-DEMO-001", "status": "PAID", "amount": "379.70",
         "currency": "BRL", "method": "credit_card", "processed_at": "2026-10-19T08:01:00Z"},
        {"id": "PAY-DEMO-002", "order_id": "ORD-DEMO-002", "status": "PAID", "amount": "299.90",
         "currency": "BRL", "method": "pix", "processed_at": "2026-10-19T08:02:00Z"},
        {"id": "PAY-DEMO-003", "order_id": "ORD-DEMO-003", "status": "PENDING", "amount": "89.90",
         "currency": "BRL", "method": "credit_card", "processed_at": None}]}
    balances = {"data": [{"produto": {"id": 1001 + index}, "depositos": [
        {"id": 1, "saldoFisico": product["stock"], "saldoVirtual": product["stock"] - 1},
        {"id": 2, "saldoFisico": 5, "saldoVirtual": 5}]} for index, product in enumerate(products)]}
    shipments = {
        "SHIP-DEMO-001": {"id": "SHIP-DEMO-001", "protocol": "PROTO-DEMO-001", "status": "posted",
                          "tracking": "TRACK-DEMO-001", "company": {"id": 1, "name": "Transportadora ficticia"},
                          "posted_at": "2026-10-20T01:00:00Z", "delivered_at": None},
        "SHIP-DEMO-002": {"id": "SHIP-DEMO-002", "protocol": "PROTO-DEMO-002", "status": "delivered",
                          "tracking": "TRACK-DEMO-002", "company": {"id": 1, "name": "Transportadora ficticia"},
                          "posted_at": "2026-10-19T12:00:00Z", "delivered_at": "2026-10-19T18:00:00Z"}}
    return [wrap("shopvibe_demo", "customers", customers), wrap("shopvibe_demo", "products", products),
            wrap("shopvibe_demo", "orders", orders), wrap("clearsale", "fraud_assessments", assessments),
            wrap("payment_api", "payments", payments),
            wrap("bling", "inventory", balances, {"products": {str(1001+i): p["id"] for i,p in enumerate(products)}}),
            wrap("melhor_envio", "shipments", shipments,
                 {"orders": {"SHIP-DEMO-001": "ORD-DEMO-001", "SHIP-DEMO-002": "ORD-DEMO-002"}})]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("samples/services"))
    args = parser.parse_args()
    for doc in documents():
        path = args.output / doc["source_system"] / (doc["dataset"] + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(path)
