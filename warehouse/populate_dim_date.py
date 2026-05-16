"""
Populates dim_date with every date from 2024-01-01 to 2027-12-31.
Run once after schema creation.
"""
import psycopg2
from datetime import date, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "retail_warehouse"),
    user=os.getenv("POSTGRES_USER", "retail"),
    password=os.getenv("POSTGRES_PASSWORD", "retail123"),
)

start = date(2024, 1, 1)
end   = date(2027, 12, 31)
delta = timedelta(days=1)

rows = []
current = start
while current <= end:
    rows.append((
        current,
        current.year,
        current.month,
        current.day,
        (current.month - 1) // 3 + 1,
        current.weekday(),
        current.weekday() >= 5,
    ))
    current += delta

with conn.cursor() as cur:
    cur.executemany("""
        INSERT INTO dim_date (date_id, year, month, day, quarter, day_of_week, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
    """, rows)
conn.commit()
conn.close()
print(f"✅ dim_date populated with {len(rows)} rows")