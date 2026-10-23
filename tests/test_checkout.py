"""Checkout validation and unavailable-database behavior, without Docker."""
import unittest
from unittest.mock import patch

from fastapi import HTTPException, Response
from pydantic import ValidationError
import psycopg

from api import main


def payload(**changes):
    return {"order_id": "ORD-UNIT", "customer_id": "CLI-UNIT", "payment_method": "pix",
            "items": [{"product_id": "PROD-UNIT", "quantity": 2, "price": "0.10"}],
            "total_amount": "0.20", **changes}


class CheckoutTests(unittest.TestCase):
    def test_invalid_totals_quantities_and_money_do_not_enter_checkout(self):
        main.OrderPayload(**payload())
        for changes in (
            {"total_amount": "0.21"}, {"items": []},
            {"items": [{"product_id": "P", "quantity": 0, "price": 1}]},
            {"items": [{"product_id": "P", "quantity": 1.5, "price": 1}]},
            {"items": [{"product_id": "P", "quantity": 1, "price": "0.201"}]},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                main.OrderPayload(**payload(**changes))

    def test_unconfirmed_database_write_returns_retryable_error(self):
        with patch.object(main, "connect_db", side_effect=psycopg.OperationalError("database unavailable")):
            with self.assertLogs(main.logger, level="ERROR"), self.assertRaises(HTTPException) as caught:
                main.process_checkout(main.OrderPayload(**payload()), Response())
        self.assertEqual(caught.exception.status_code, 503)
        self.assertIn("mesmo order_id", caught.exception.detail)


if __name__ == "__main__":
    unittest.main()
