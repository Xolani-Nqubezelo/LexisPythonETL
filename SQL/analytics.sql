CREATE OR REPLACE VIEW daily_metrics AS
SELECT
    DATE(o.order_ts) AS order_date,
    COUNT(DISTINCT o.order_id) AS orders_count,
    SUM(o.total_amount) AS total_revenue,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) = 0 THEN 0
        ELSE SUM(o.total_amount)::NUMERIC / COUNT(DISTINCT o.order_id)
    END AS average_order_value
FROM orders o
GROUP BY DATE(o.order_ts)
ORDER BY order_date;

CREATE OR REPLACE VIEW top_customers AS
SELECT
    c.customer_id,
    c.email,
    c.full_name,
    SUM(o.total_amount) AS lifetime_spend
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.email, c.full_name
ORDER BY lifetime_spend DESC
LIMIT 10;

CREATE OR REPLACE VIEW top_skus AS
SELECT
    oi.sku,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
GROUP BY oi.sku
ORDER BY revenue DESC
LIMIT 10;


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
