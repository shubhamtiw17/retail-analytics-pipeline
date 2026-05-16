"""Unit tests for the event generator."""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generators.event_generator import (
    make_order,
    make_payment,
    make_customer_event,
    make_inventory_update,
    make_product_click,
)


def test_order_has_required_fields():
    order = make_order()
    assert order["event_id"]
    assert order["event_type"] == "order"
    assert order["customer_id"].startswith("CUST-")
    assert order["order_id"]
    assert order["total_amount"] > 0
    assert len(order["items"]) >= 1


def test_order_total_matches_items():
    order = make_order()
    calculated = sum(i["quantity"] * i["unit_price"] for i in order["items"])
    assert abs(order["total_amount"] - round(calculated, 2)) < 0.01


def test_payment_has_valid_status():
    for _ in range(20):
        payment = make_payment()
        assert payment["status"] in ("success", "failed", "pending")
        assert payment["amount"] > 0


def test_customer_event_has_valid_type():
    valid_types = {"signup", "login", "logout", "profile_update"}
    for _ in range(20):
        event = make_customer_event()
        assert event["event_type"] in valid_types


def test_inventory_update_structure():
    update = make_inventory_update()
    assert update["warehouse"] in ("WH-EAST", "WH-WEST", "WH-CENTRAL")
    assert update["product_id"].startswith("PROD-")
    assert isinstance(update["delta"], int)


def test_product_click_has_valid_referrer():
    valid_referrers = {"search", "homepage", "email", "social", "direct"}
    for _ in range(20):
        click = make_product_click()
        assert click["referrer"] in valid_referrers
        assert click["duration_ms"] > 0


def test_event_ids_are_unique():
    orders = [make_order() for _ in range(100)]
    ids = [o["event_id"] for o in orders]
    assert len(set(ids)) == 100