import json
import random
import time
import uuid
import argparse
import logging
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

# ── Kafka producer ────────────────────────────────────────────────────────────

def make_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=10,
    )


# ── Reference data ────────────────────────────────────────────────────────────

PRODUCT_IDS     = [f"PROD-{i:04d}" for i in range(1, 201)]
CUSTOMER_IDS    = [f"CUST-{i:06d}" for i in range(1, 10001)]
CATEGORIES      = ["electronics", "clothing", "home", "sports", "beauty", "food"]
DEVICES         = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
WAREHOUSES      = ["WH-EAST", "WH-WEST", "WH-CENTRAL"]
REFERRERS       = ["search", "homepage", "email", "social", "direct"]


# ── Event factories ───────────────────────────────────────────────────────────

def make_order() -> dict:
    items = [
        {
            "product_id": random.choice(PRODUCT_IDS),
            "quantity":   random.randint(1, 5),
            "unit_price": round(random.uniform(5.0, 500.0), 2),
        }
        for _ in range(random.randint(1, 4))
    ]
    return {
        "event_id":     str(uuid.uuid4()),
        "event_type":   "order",
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "customer_id":  random.choice(CUSTOMER_IDS),
        "order_id":     str(uuid.uuid4()),
        "items":        items,
        "total_amount": round(sum(i["quantity"] * i["unit_price"] for i in items), 2),
        "currency":     "USD",
        "status":       random.choice(["pending", "confirmed"]),
        "device":       random.choice(DEVICES),
        "region":       fake.state_abbr(),
    }


def make_payment() -> dict:
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "payment",
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "payment_id":  str(uuid.uuid4()),
        "order_id":    str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMER_IDS),
        "amount":      round(random.uniform(5.0, 2000.0), 2),
        "currency":    "USD",
        "method":      random.choice(PAYMENT_METHODS),
        "status":      random.choices(
                           ["success", "failed", "pending"],
                           weights=[80, 10, 10]
                       )[0],
        "gateway_ref": fake.bothify("GW-####-????"),
    }


def make_customer_event() -> dict:
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  random.choice(["signup", "login", "logout", "profile_update"]),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "customer_id": random.choice(CUSTOMER_IDS),
        "device":      random.choice(DEVICES),
        "ip_address":  fake.ipv4(),
        "country":     fake.country_code(),
        "session_id":  str(uuid.uuid4()),
    }


def make_inventory_update() -> dict:
    delta = random.choice([-50, -20, -10, -5, 50, 100, 200])
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "inventory_update",
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "product_id":  random.choice(PRODUCT_IDS),
        "warehouse":   random.choice(WAREHOUSES),
        "delta":       delta,
        "stock_after": max(0, random.randint(0, 1000) + delta),
        "reason":      random.choice(["sale", "restock", "return", "adjustment"]),
    }


def make_product_click() -> dict:
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "product_click",
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "customer_id": random.choice(CUSTOMER_IDS),
        "product_id":  random.choice(PRODUCT_IDS),
        "category":    random.choice(CATEGORIES),
        "device":      random.choice(DEVICES),
        "session_id":  str(uuid.uuid4()),
        "referrer":    random.choice(REFERRERS),
        "duration_ms": random.randint(500, 30000),
    }


# ── Topic routing ─────────────────────────────────────────────────────────────
# (topic, factory, weight) — clicks are 10x more frequent than orders

TOPIC_PROFILE = [
    ("orders",            make_order,            1),
    ("payments",          make_payment,          1),
    ("customer_events",   make_customer_event,   2),
    ("inventory_updates", make_inventory_update, 1),
    ("product_clicks",    make_product_click,    10),
]

TOPICS   = [t for t, _, _ in TOPIC_PROFILE]
MAKERS   = [m for _, m, _ in TOPIC_PROFILE]
WEIGHTS  = [w for _, _, w in TOPIC_PROFILE]

counters = {t: 0 for t in TOPICS}


# ── Producer send ─────────────────────────────────────────────────────────────

def send(producer: KafkaProducer, topic: str, event: dict) -> None:
    key = event.get("customer_id") or event.get("product_id") or event["event_id"]
    try:
        producer.send(topic, key=key, value=event)
        counters[topic] += 1
    except KafkaError as e:
        logger.error(f"Failed to produce to {topic}: {e}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(broker: str, tps: int, scenario: str | None) -> None:
    logger.info(f"Connecting to Kafka at {broker} ...")
    producer = make_producer(broker)
    logger.info(f"Ready — {tps} events/sec | scenario={scenario or 'normal'}")

    interval     = 1.0 / tps
    report_every = 10
    last_report  = time.time()
    flash_end    = None

    try:
        while True:
            loop_start = time.time()

            # ── Flash-sale: 10x volume for 30 seconds ────────────────────
            effective_tps = tps
            if scenario == "flash_sale":
                now = time.time()
                if flash_end is None:
                    flash_end = now + 30
                    logger.info("FLASH SALE START — 10x volume for 30s")
                if now < flash_end:
                    effective_tps = tps * 10
                else:
                    scenario  = None
                    flash_end = None
                    logger.info("FLASH SALE END — back to normal volume")

            # ── Pick topic by weight and produce ─────────────────────────
            idx   = random.choices(range(len(TOPICS)), weights=WEIGHTS, k=1)[0]
            topic = TOPICS[idx]
            send(producer, topic, MAKERS[idx]())

            # ── Summary log every 10 seconds ─────────────────────────────
            if time.time() - last_report >= report_every:
                total = sum(counters.values())
                parts = " | ".join(f"{t}={counters[t]}" for t in TOPICS)
                logger.info(f"[{report_every}s] total={total} | {parts}")
                counters.update({t: 0 for t in TOPICS})
                last_report = time.time()

            # ── Rate limiting ─────────────────────────────────────────────
            elapsed = time.time() - loop_start
            time.sleep(max(0, (1.0 / effective_tps) - elapsed))

    except KeyboardInterrupt:
        logger.info("Shutting down ...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed cleanly.")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail e-commerce event generator")
    parser.add_argument("--broker",   default="localhost:9092", help="Kafka broker")
    parser.add_argument("--tps",      type=int, default=50,     help="Events per second")
    parser.add_argument("--scenario", choices=["flash_sale"],   help="Load scenario")
    args = parser.parse_args()
    run(args.broker, args.tps, args.scenario)