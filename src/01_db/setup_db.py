#!/usr/bin/env python3
"""
Creates the SALES sample schema used by all Chapter 3 examples.
Run once against your Oracle container before using the demos.
"""

import oracledb
from dotenv import load_dotenv
import os

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

DDL = [
    """CREATE TABLE customers (
        customer_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        customer_name VARCHAR2(100) NOT NULL,
        country       VARCHAR2(50),
        created_date  DATE DEFAULT SYSDATE
    )""",
    """CREATE TABLE orders (
        order_id     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        customer_id  NUMBER NOT NULL REFERENCES customers(customer_id),
        order_date   DATE DEFAULT SYSDATE
    )""",
    """CREATE TABLE products (
        product_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_name VARCHAR2(100) NOT NULL,
        category     VARCHAR2(50),
        unit_price   NUMBER(10,2)
    )""",
    """CREATE TABLE order_items (
        item_id    NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        order_id   NUMBER NOT NULL REFERENCES orders(order_id),
        product_id NUMBER NOT NULL REFERENCES products(product_id),
        quantity   NUMBER NOT NULL,
        unit_price NUMBER(10,2) NOT NULL
    )""",
]

SEED = [
    "INSERT INTO customers (customer_name, country, created_date) VALUES ('Acme GmbH', 'DE', DATE '2023-01-15')",
    "INSERT INTO customers (customer_name, country, created_date) VALUES ('Globex Corp', 'US', DATE '2023-03-22')",
    "INSERT INTO customers (customer_name, country, created_date) VALUES ('Initech AG', 'DE', DATE '2023-06-10')",
    "INSERT INTO customers (customer_name, country, created_date) VALUES ('Umbrella Ltd', 'GB', DATE '2024-01-05')",
    "INSERT INTO customers (customer_name, country, created_date) VALUES ('Stark Industries', 'US', DATE '2024-02-14')",

    "INSERT INTO products (product_name, category, unit_price) VALUES ('Widget A', 'Widgets', 29.99)",
    "INSERT INTO products (product_name, category, unit_price) VALUES ('Widget B', 'Widgets', 49.99)",
    "INSERT INTO products (product_name, category, unit_price) VALUES ('Gadget X', 'Gadgets', 149.99)",
    "INSERT INTO products (product_name, category, unit_price) VALUES ('Gadget Y', 'Gadgets', 199.99)",

    "INSERT INTO orders (customer_id, order_date) VALUES (1, DATE '2024-01-10')",
    "INSERT INTO orders (customer_id, order_date) VALUES (2, DATE '2024-02-15')",
    "INSERT INTO orders (customer_id, order_date) VALUES (1, DATE '2024-03-20')",
    "INSERT INTO orders (customer_id, order_date) VALUES (3, DATE '2024-04-05')",
    "INSERT INTO orders (customer_id, order_date) VALUES (4, DATE '2024-05-12')",
    "INSERT INTO orders (customer_id, order_date) VALUES (5, DATE '2024-06-18')",
    "INSERT INTO orders (customer_id, order_date) VALUES (2, DATE '2024-07-22')",
    "INSERT INTO orders (customer_id, order_date) VALUES (1, DATE '2024-08-30')",

    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (1, 10, 1, 29.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (2, 5, 3, 149.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (3, 1, 3, 149.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (4, 3, 4, 199.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (5, 5, 4, 199.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (6, 3, 3, 149.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (7, 1, 4, 199.99)",
    "INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (8, 2, 2, 49.99)",
]


def main():
    conn = oracledb.connect(
        user=os.getenv("ORA_USER"),
        password=os.getenv("ORA_PASSWORD"),
        dsn=os.getenv("ORA_DSN"),
    )
    cursor = conn.cursor()

    for stmt in DDL:
        table_name = stmt.split("TABLE")[1].split("(")[0].strip()
        try:
            cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
            print(f"Dropped {table_name}")
        except oracledb.DatabaseError:
            pass
        cursor.execute(stmt)
        print(f"Created {table_name}")

    for stmt in SEED:
        cursor.execute(stmt)

    conn.commit()
    print(f"\nSeeded {len(SEED)} rows. Done.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
