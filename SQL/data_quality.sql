
CREATE OR REPLACE VIEW duplicate_customers AS
SELECT
    LOWER(email) AS email_lower,
    ARRAY_AGG(customer_id) AS customer_ids,
    COUNT(*) AS count_duplicates
FROM customers
GROUP BY LOWER(email)
HAVING COUNT(*) > 1;

CREATE OR REPLACE VIEW orders_missing_customers AS
SELECT o.order_id, o.customer_id, o.order_ts, o.total_amount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


CREATE OR REPLACE VIEW invalid_order_items AS
SELECT *
FROM order_items
WHERE quantity <= 0 OR unit_price <= 0;




CREATE OR REPLACE VIEW invalid_order_items AS
SELECT *
FROM order_items
WHERE quantity <= 0 OR unit_price <= 0;



SELECT * FROM daily_metrics;
SELECT * FROM top_customers;
SELECT * FROM top_skus;
SELECT * FROM duplicate_customers;
SELECT * FROM orders_missing_customers;
SELECT * FROM invalid_order_items;
SELECT * FROM invalid_order_status;
