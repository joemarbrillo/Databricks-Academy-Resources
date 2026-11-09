SELECT
  c.first_name || ' ' || c.last_name AS customer_name,
  ROUND(SUM(o.quantity * o.unit_price), 2) AS total_spend
FROM
  data.raw_orders o
    JOIN data.raw_customers c
      ON o.customer_id = c.customer_id
GROUP BY
  customer_name
ORDER BY
  ROUND(SUM(o.quantity * o.unit_price), 2) DESC
LIMIT 5;

SELECT
  p.category,
  ROUND(AVG(f.rating), 2) as avg_rating,
  COUNT(*) as review_count
FROM
  data.customer_feedback f
    JOIN data.raw_orders o
      ON o.order_id = f.order_id
    JOIN data.raw_products p
      ON p.product_id = o.product_id
GROUP BY
  p.category
ORDER BY
  avg_rating DESC;

SELECT
  c.state,
  ROUND(SUM(o.quantity * o.unit_price), 2) AS total_revenue,
  COUNT(DISTINCT c.customer_id) AS unique_customers
FROM
  data.raw_orders o
    JOIN data.raw_customers c
      ON o.customer_id = c.customer_id
GROUP BY
  c.state
ORDER BY
  total_revenue DESC
LIMIT 10;


select current_catalog();

DROP MATERIALIZED VIEW IF EXISTS IDENTIFIER('learn_etl_pipelines_sql_joemar_brillo' || '.default.lab_top_orders');


