import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import create_engine, MetaData, select

# ==========================
# Настройки
# ==========================
DB_URL = "postgresql+psycopg2://demo:demo_pass@localhost:5432/oltp"

NUM_CUSTOMERS = 5_000
NUM_PRODUCTS = 500
NUM_ORDERS = 30_000
ORDER_ITEMS_TOTAL = 120_000
BATCH_SIZE = 5_000

fake = Faker()

engine = create_engine(
    DB_URL,
    pool_size=10,
    max_overflow=20,
)

metadata = MetaData()

# грузим только нужные таблицы
metadata.reflect(
    bind=engine,
    only=["customers", "products", "orders", "order_items"]
)

customers_tbl = metadata.tables["customers"]
products_tbl = metadata.tables["products"]
orders_tbl = metadata.tables["orders"]
order_items_tbl = metadata.tables["order_items"]


# ==========================
# Helper — batch insert
# ==========================
def batch_insert(conn, table, data, batch_size=BATCH_SIZE):
    for i in range(0, len(data), batch_size):
        conn.execute(table.insert(), data[i:i+batch_size])


# ==========================
# TRUNCATE
# ==========================
print("Очистка таблиц...")

with engine.begin() as conn:
    conn.exec_driver_sql("""
        TRUNCATE TABLE order_items, orders, products, customers
        RESTART IDENTITY CASCADE;
    """)
    conn.exec_driver_sql("ANALYZE;")

print("✅ Таблицы очищены")


# ==========================
# Customers
# ==========================
print("Генерация customers...")

customers = [
    {
        "full_name": fake.name(),
        "email": f"user{i}@example.com",   # deterministic = нет дублей
        "created_at": fake.date_time_between("-2y", "now"),
    }
    for i in range(NUM_CUSTOMERS)
]

with engine.begin() as conn:
    batch_insert(conn, customers_tbl, customers)

# быстро получаем id
with engine.connect() as conn:
    customer_ids = list(
        conn.execute(
            select(customers_tbl.c.customer_id)
        ).scalars()
    )


# ==========================
# Products
# ==========================
print("Генерация products...")

categories = ["Electronics", "Clothing", "Books", "Home", "Toys", "Sports"]

products = [
    {
        "name": f"{fake.word().title()}-{i}",  # тоже deterministic
        "category": random.choice(categories),
        "price": round(random.uniform(5, 500), 2),
    }
    for i in range(NUM_PRODUCTS)
]

with engine.begin() as conn:
    batch_insert(conn, products_tbl, products)

with engine.connect() as conn:
    result = conn.execute(
        select(
            products_tbl.c.product_id,
            products_tbl.c.price,
        )
    )

    product_prices = {row[0]: row[1] for row in result}
    product_ids = list(product_prices.keys())


# ==========================
# Orders
# ==========================
print("Генерация orders...")

status_choices = ["paid"] * 70 + ["new"] * 20 + ["cancelled"] * 10
start_date = datetime.now() - timedelta(days=180)

orders = [
    {
        "customer_id": random.choice(customer_ids),
        "order_ts": start_date + timedelta(
            days=random.randint(0, 180),
            seconds=random.randint(0, 86400),
        ),
        "status": random.choice(status_choices),
    }
    for _ in range(NUM_ORDERS)
]

with engine.begin() as conn:
    batch_insert(conn, orders_tbl, orders)

with engine.connect() as conn:
    order_ids = list(
        conn.execute(
            select(orders_tbl.c.order_id)
        ).scalars()
    )


# ==========================
# Order Items (120k)
# ==========================
print("Генерация order_items...")

remaining = ORDER_ITEMS_TOTAL

with engine.begin() as conn:

    batch = []

    while remaining > 0:
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        qty = random.randint(1, 5)

        batch.append({
            "order_id": order_id,
            "product_id": product_id,
            "qty": qty,
            "item_price": product_prices[product_id],
        })

        remaining -= 1

        if len(batch) >= BATCH_SIZE:
            conn.execute(order_items_tbl.insert(), batch)
            batch.clear()

    if batch:
        conn.execute(order_items_tbl.insert(), batch)

print("🚀 Данные успешно сгенерированы!")
