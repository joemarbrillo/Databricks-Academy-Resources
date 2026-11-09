DROP TABLE IF EXISTS bronze.raw_orders;

DECLARE OR REPLACE my_catalog STRING;
SET VAR my_catalog = 'learn_etl_pipelines_sql_joemar_brillo';

CREATE OR REFRESH STREAMING TABLE bronze.raw_orders
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
)
AS SELECT * FROM STREAM read_files(
  '/Volumes/learn_etl_pipelines_sql_joemar_brillo/data/course_data/raw_orders',
  format => 'csv',
  header => 'true',
  inferColumnTypes => 'true',
  schemaEvolutionMode => 'rescue'
);

-- select my_catalog;


DROP TABLE IF EXISTS bronze.raw_products;



CREATE OR REFRESH STREAMING TABLE bronze.raw_products
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
)
AS SELECT * FROM STREAM read_files(
  '/Volumes/learn_etl_pipelines_sql_joemar_brillo/data/course_data/raw_products',
  format => 'csv',
  header => 'true',
  inferColumnTypes => 'true',
  schemaEvolutionMode => 'rescue'
);

-- Enable row tracking on the customers source table
-- (required by the incremental materialized view silver.orders_enriched)
ALTER TABLE data.raw_customers
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');


