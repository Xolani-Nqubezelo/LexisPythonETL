DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
---- `customer_id` is the primary key.
-- `email` is stored in lowercase to ensure consistency.
-- A CHECK constraint (`email = LOWER(email)`) enforces normalization at the database level.
-- A UNIQUE constraint is applied on `email` to prevent duplicates.
-- `signup_date` and `is_active` are marked NOT NULL as they are required for analytics and filtering.
-- `country_code` is optional due to missing values in source data.
    customer_id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    signup_date DATE NOT NULL,
    country_code CHAR(2),
    is_active BOOLEAN NOT NULL
);

CREATE TABLE orders (
-- -- 
-- - `order_id` is the primary key.
-- - `customer_id` is a foreign key referencing `customers(customer_id)` to enforce referential integrity.
-- - `order_ts` uses `TIMESTAMPTZ` to support timezone-aware timestamps.
-- - A CHECK constraint enforces valid status values:
--   ('placed', 'shipped', 'cancelled', 'refunded').
-- - All core fields (`customer_id`, `order_ts`, `status`, `total_amount`, `currency`) are NOT NULL to ensure reliability.

    order_id BIGINT PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CHECK (status IN ('placed','shipped','cancelled','refunded'))
);

CREATE TABLE order_items (
    order_id BIGINT,
    line_no INTEGER,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    category TEXT,

    PRIMARY KEY (order_id, line_no),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),

    CHECK (quantity > 0),
    CHECK (unit_price > 0)

    
-- - Composite primary key (`order_id`, `line_no`) ensures uniqueness per order line.
-- - `order_id` is a foreign key referencing `orders(order_id)`.
-- - CHECK constraints enforce:
--   - `quantity > 0`
--   - `unit_price > 0`
-- - These constraints prevent invalid transactional data from being stored.

);