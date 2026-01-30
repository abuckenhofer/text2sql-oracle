-- ============================================================
-- Sample SALES schema: tables + seed data
-- Run against your Oracle instance (Free container or ADB)
-- ============================================================

-- Drop existing tables (ignore errors on first run)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE order_items CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- Tables
CREATE TABLE customers (
    customer_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name VARCHAR2(100) NOT NULL,
    country       VARCHAR2(50),
    created_date  DATE DEFAULT SYSDATE
);

CREATE TABLE orders (
    order_id     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  NUMBER NOT NULL REFERENCES customers(customer_id),
    order_date   DATE DEFAULT SYSDATE
);

CREATE TABLE products (
    product_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name VARCHAR2(100) NOT NULL,
    category     VARCHAR2(50),
    unit_price   NUMBER(10,2)
);

CREATE TABLE order_items (
    item_id    NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id   NUMBER NOT NULL REFERENCES orders(order_id),
    product_id NUMBER NOT NULL REFERENCES products(product_id),
    quantity   NUMBER NOT NULL,
    unit_price NUMBER(10,2) NOT NULL
);

-- Seed data
INSERT INTO customers (customer_name, country, created_date) VALUES ('Acme GmbH', 'DE', DATE '2023-01-15');
INSERT INTO customers (customer_name, country, created_date) VALUES ('Globex Corp', 'US', DATE '2023-03-22');
INSERT INTO customers (customer_name, country, created_date) VALUES ('Initech AG', 'DE', DATE '2023-06-10');
INSERT INTO customers (customer_name, country, created_date) VALUES ('Umbrella Ltd', 'GB', DATE '2024-01-05');
INSERT INTO customers (customer_name, country, created_date) VALUES ('Stark Industries', 'US', DATE '2024-02-14');

INSERT INTO products (product_name, category, unit_price) VALUES ('Widget A', 'Widgets', 29.99);
INSERT INTO products (product_name, category, unit_price) VALUES ('Widget B', 'Widgets', 49.99);
INSERT INTO products (product_name, category, unit_price) VALUES ('Gadget X', 'Gadgets', 149.99);
INSERT INTO products (product_name, category, unit_price) VALUES ('Gadget Y', 'Gadgets', 199.99);

INSERT INTO orders (customer_id, order_date) VALUES (1, DATE '2024-01-10');
INSERT INTO orders (customer_id, order_date) VALUES (2, DATE '2024-02-15');
INSERT INTO orders (customer_id, order_date) VALUES (1, DATE '2024-03-20');
INSERT INTO orders (customer_id, order_date) VALUES (3, DATE '2024-04-05');
INSERT INTO orders (customer_id, order_date) VALUES (4, DATE '2024-05-12');
INSERT INTO orders (customer_id, order_date) VALUES (5, DATE '2024-06-18');
INSERT INTO orders (customer_id, order_date) VALUES (2, DATE '2024-07-22');
INSERT INTO orders (customer_id, order_date) VALUES (1, DATE '2024-08-30');

INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (1, 10, 1, 29.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (2, 5, 3, 149.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (3, 1, 3, 149.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (4, 3, 4, 199.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (5, 5, 4, 199.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (6, 3, 3, 149.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (7, 1, 4, 199.99);
INSERT INTO order_items (order_id, quantity, product_id, unit_price) VALUES (8, 2, 2, 49.99);

COMMIT;
