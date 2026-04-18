CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.mart_department_trends;

CREATE TABLE marts.mart_department_trends AS
SELECT
    p.department,
    COUNT(*) AS total_order_lines,
    SUM(f.reordered) AS total_reorders,
    ROUND(AVG(f.reordered::numeric), 4) AS reorder_rate
FROM warehouse.fact_order_items f
LEFT JOIN warehouse.dim_products p
    ON f.product_id = p.product_id
GROUP BY p.department;

SELECT COUNT(*) FROM marts.mart_department_trends;

