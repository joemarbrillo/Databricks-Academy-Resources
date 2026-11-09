DROP MATERIALIZED VIEW IF EXISTS silver.orders_enriched;
CREATE OR REFRESH MATERIALIZED VIEW silver.orders_enriched
REFRESH POLICY INCREMENTAL
AS
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  CAST(o.order_date AS DATE)                          AS order_date,
  CAST(o.quantity AS INT)                              AS quantity,
  CAST(o.unit_price AS DECIMAL(10,2))                  AS unit_price,
  CAST(o.quantity * o.unit_price AS DECIMAL(12,2))     AS line_total,
  COALESCE(o.discount_pct, 0)                          AS discount_pct,
  COALESCE(o.shipping_cost, 0.00)                      AS shipping_cost,
  o.order_status,
  o.payment_method,
  COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS customer_name,
  c.customer_segment,
  c.city                                                AS customer_city,
  c.state                                               AS customer_state,
  COALESCE(p.product_name, 'Unknown Product')           AS product_name,
  p.category                                            AS product_category,
  p.brand                                               AS product_brand,
  CAST(p.list_price AS DECIMAL(10,2))                   AS product_list_price
FROM bronze.raw_orders o
LEFT JOIN data.raw_customers c
  ON o.customer_id = c.customer_id
LEFT JOIN bronze.raw_products p
  ON o.product_id = p.product_id;