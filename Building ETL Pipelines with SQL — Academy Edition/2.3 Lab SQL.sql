DROP TABLE IF EXISTS bronze.raw_orders;

CREATE OR REFRESH STREAMING TABLE bronze.raw_orders
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.enableDeletionVectors' = 'true'
  ) AS
SELECT
  *
FROM
  STREAM read_files(
    '/Volumes/learn_etl_pipelines_sql_joemar_brillo/data/course_data/raw_orders',
    format => 'csv',
    header => 'true',
    inferColumnTypes => 'true',
    schemaEvolutionMode => 'rescue',
    partitionColumns => ''
  );

DROP TABLE IF EXISTS bronze.raw_products;

CREATE OR REFRESH STREAMING TABLE bronze.raw_products
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.enableDeletionVectors' = 'true'
  ) AS
SELECT
  *
FROM
  STREAM read_files(
    '/Volumes/learn_etl_pipelines_sql_joemar_brillo/data/course_data/raw_products',
    format => 'csv',
    header => 'true',
    inferColumnTypes => 'true',
    rescuedDataColumn => '_rescued_data',
    schemaEvolutionMode => 'rescue',
    partitionColumns => ''
  );

DROP TABLE IF EXISTS bronze.customer_feedback;

CREATE OR REFRESH STREAMING TABLE bronze.customer_feedback
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.enableDeletionVectors' = 'true'
  ) AS
SELECT
  *
FROM
  STREAM read_files(
    '/Volumes/learn_etl_pipelines_sql_joemar_brillo/data/course_data/customer_feedback',
    format => 'csv',
    header => 'true',
    inferColumnTypes => 'true',
    rescuedDataColumn => '_rescued_data',
    schemaEvolutionMode => 'rescue',
    partitionColumns => ''
  );

select
  *
from
  bronze.customer_feedback
limit 10;

select
  *
from
  bronze.raw_products
limit 10;

select
  *
from
  bronze.raw_orders
limit 10;

select
  *
from
  data.raw_customers
limit 10;

-- TODO
-- 1. Create a table called `silver.customer_feedback_enriched` that joins the `bronze.customer_feedback` table with the `bronze.
CREATE OR REFRESH MATERIALIZED VIEW silver.customer_feedback_enriched AS
SELECT
  f.feedback_id,
  f.customer_id,
  f.order_id,
  f.feedback_date,
  f.rating,
  f.feedback_text,
  f.channel,
  --   CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
  COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') as customer_name,
  c.customer_segment AS customer_segment,
  o.order_date,
  o.order_status,
  CAST(o.quantity * o.unit_price AS DECIMAL(12, 2)) AS order_value,
  p.product_name,
  p.category AS product_category,
  p.brand AS product_brand
FROM
  bronze.customer_feedback f
    LEFT JOIN bronze.raw_orders o
      ON f.order_id = o.order_id
    LEFT JOIN bronze.raw_products p
      ON o.product_id = p.product_id
    LEFT JOIN data.raw_customers c
      ON c.customer_id = f.customer_id;

CREATE OR REFRESH MATERIALIZED VIEW gold.feedback_by_category AS
SELECT
  f.product_category AS product_category,
  AVG(f.rating) AS avg_rating,
  COUNT(*) AS feedback_count,
  ROUND(AVG(f.order_value), 2) AS avg_order_value
FROM
  silver.customer_feedback_enriched f
WHERE
  f.product_category IS NOT NULL
GROUP BY
  product_category;



