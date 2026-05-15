from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    TimestampType, ArrayType, LongType
)

# ── Order item (nested inside order) ─────────────────────────────────────────
order_item_schema = StructType([
    StructField("product_id", StringType(),  True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
])

# ── Orders ────────────────────────────────────────────────────────────────────
order_schema = StructType([
    StructField("event_id",     StringType(),               False),
    StructField("event_type",   StringType(),               True),
    StructField("timestamp",    StringType(),               True),
    StructField("customer_id",  StringType(),               True),
    StructField("order_id",     StringType(),               True),
    StructField("items",        ArrayType(order_item_schema), True),
    StructField("total_amount", DoubleType(),               True),
    StructField("currency",     StringType(),               True),
    StructField("status",       StringType(),               True),
    StructField("device",       StringType(),               True),
    StructField("region",       StringType(),               True),
])

# ── Payments ──────────────────────────────────────────────────────────────────
payment_schema = StructType([
    StructField("event_id",    StringType(), False),
    StructField("event_type",  StringType(), True),
    StructField("timestamp",   StringType(), True),
    StructField("payment_id",  StringType(), True),
    StructField("order_id",    StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount",      DoubleType(), True),
    StructField("currency",    StringType(), True),
    StructField("method",      StringType(), True),
    StructField("status",      StringType(), True),
    StructField("gateway_ref", StringType(), True),
])

# ── Customer events ───────────────────────────────────────────────────────────
customer_event_schema = StructType([
    StructField("event_id",    StringType(), False),
    StructField("event_type",  StringType(), True),
    StructField("timestamp",   StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("device",      StringType(), True),
    StructField("ip_address",  StringType(), True),
    StructField("country",     StringType(), True),
    StructField("session_id",  StringType(), True),
])

# ── Inventory updates ─────────────────────────────────────────────────────────
inventory_schema = StructType([
    StructField("event_id",    StringType(),  False),
    StructField("event_type",  StringType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("warehouse",   StringType(),  True),
    StructField("delta",       IntegerType(), True),
    StructField("stock_after", IntegerType(), True),
    StructField("reason",      StringType(),  True),
])

# ── Product clicks ────────────────────────────────────────────────────────────
click_schema = StructType([
    StructField("event_id",    StringType(),  False),
    StructField("event_type",  StringType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("customer_id", StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("device",      StringType(),  True),
    StructField("session_id",  StringType(),  True),
    StructField("referrer",    StringType(),  True),
    StructField("duration_ms", LongType(),    True),
])

# ── Schema registry ───────────────────────────────────────────────────────────
SCHEMAS = {
    "orders":            order_schema,
    "payments":          payment_schema,
    "customer_events":   customer_event_schema,
    "inventory_updates": inventory_schema,
    "product_clicks":    click_schema,
}