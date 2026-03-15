CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.mart_product_reorders;

CREATE TABLE marts.mart_product_reorders AS
SELECT
    p.product_id,
    p.product_name,
    p.aisle,
    p.department,
    COUNT(*) AS total_order_lines,
    SUM(f.reordered) AS total_reorders,
    ROUND(AVG(f.reordered::numeric), 4) AS reorder_rate
FROM warehouse.fact_order_items f
LEFT JOIN warehouse.dim_products p
    ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.aisle,
    p.department;

SELECT COUNT(*) FROM marts.mart_product_reorders;
