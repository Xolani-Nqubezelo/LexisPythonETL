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

