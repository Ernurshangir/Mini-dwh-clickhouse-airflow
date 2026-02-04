CREATE TABLE customers (
  customer_id SERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE products (
  product_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  price NUMERIC(12,2) NOT NULL CHECK (price >= 0)
);

CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id),
  order_ts TIMESTAMP NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('new','paid','cancelled'))
);

CREATE TABLE order_items (
  order_id INT NOT NULL REFERENCES orders(order_id),
  product_id INT NOT NULL REFERENCES products(product_id),
  qty INT NOT NULL CHECK (qty > 0),
  item_price NUMERIC(12,2) NOT NULL CHECK (item_price >= 0)
);

CREATE INDEX idx_orders_order_ts ON orders(order_ts);
