CREATE MATERIALIZED VIEW mv_sales_by_customer_segment AS
SELECT
    c.segment,
    SUM(s.amount) AS total_sales,
    COUNT(s.id) AS total_orders
FROM
    sales s
JOIN
    customers c ON s.customer_id = c.id
GROUP BY
    c.segment;

--| Customer Segment | Total Sales | Total Orders |
--|------------------|-------------|--------------|
--| Individual       | 5000        | 20           |
--| Corporate        | 3800        | 17           |



CREATE MATERIALIZED VIEW mv_sales_by_month AS
SELECT
    DATE_TRUNC('month', s.date) AS month,
    SUM(s.amount) AS total_sales,
    COUNT(s.id) AS total_orders
FROM
    sales s
GROUP BY
    DATE_TRUNC('month', s.date)
ORDER BY
    DATE_TRUNC('month', s.date) DESC;

--
--| Month       | Total Sales | Total Orders |
--|-------------|-------------|--------------|
--| January '24 | 4000        | 20           |
--| December '23| 3500        | 15           |



CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    product_id INT,
    amount DECIMAL(10, 2),
    date DATE
);


CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    segment VARCHAR(50)
);
