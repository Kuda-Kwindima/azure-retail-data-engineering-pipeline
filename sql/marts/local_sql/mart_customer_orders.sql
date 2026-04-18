CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.mart_customer_orders;

CREATE TABLE marts.mart_customer_orders AS
SELECT
    user_id,
    COUNT(order_id) AS total_orders,
    AVG(order_number::numeric) AS avg_order_number,
    AVG(days_since_prior_order::numeric) AS avg_days_between_orders
FROM warehouse.dim_orders
GROUP BY user_id;

SELECT COUNT(*) FROM marts.mart_customer_orders;