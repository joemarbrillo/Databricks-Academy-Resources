# Databricks notebook source
import os

_classroom_setup_common_nb_path = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

# Under %run this is the calling lesson, which sits at the imported course root. Import can rename
# that folder, so data paths must not repeat the course folder name.
_COURSE_ROOT_DIR = "/Workspace" + os.path.dirname(_classroom_setup_common_nb_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create safe function for catalog name
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION safe_uc_name(value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   COALESCE(
# MAGIC     NULLIF(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(
# MAGIC           LOWER(TRIM(value)),
# MAGIC           '[^a-z0-9_]',
# MAGIC           '_'
# MAGIC         ),
# MAGIC         '_+',
# MAGIC         '_'
# MAGIC       ),
# MAGIC       ''
# MAGIC     ),
# MAGIC     'user'
# MAGIC   );
# MAGIC
# MAGIC -- Create temp view of all available catalogs
# MAGIC CREATE OR REPLACE TEMPORARY VIEW list_of_catalogs AS
# MAGIC SELECT LOWER(catalog_name) AS catalog_names
# MAGIC FROM system.information_schema.catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC BEGIN
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 1. Declare variables
# MAGIC -- =========================================
# MAGIC   DECLARE user_email STRING;
# MAGIC   DECLARE user_name STRING;
# MAGIC   DECLARE safe_user_name STRING;
# MAGIC   DECLARE vocareum_catalog_name STRING DEFAULT 'NA';
# MAGIC   DECLARE catalog_name STRING DEFAULT 'NA';
# MAGIC   DECLARE catalog_already_exists BOOLEAN DEFAULT FALSE;
# MAGIC   DECLARE create_catalog_statement STRING DEFAULT '';
# MAGIC   DECLARE create_view_with_catalog_name STRING DEFAULT '';
# MAGIC   DECLARE test STRING DEFAULT '';
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 2. Capture current user
# MAGIC -- =========================================
# MAGIC   SET user_email = current_user();
# MAGIC   SET user_name = SPLIT(user_email, '@')[0];
# MAGIC   SET safe_user_name = (SELECT safe_uc_name(SPLIT(current_user(), '@')[0]));
# MAGIC
# MAGIC -- ==================================================================================
# MAGIC -- 3. Determine Vocareum or Non-Vocareum workspace and set/create catalog
# MAGIC -- ==================================================================================
# MAGIC   IF (LOWER(user_email) LIKE '%@vocareum.com')
# MAGIC     THEN
# MAGIC       SET vocareum_catalog_name = safe_user_name;
# MAGIC
# MAGIC       IF EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM list_of_catalogs
# MAGIC         WHERE catalog_names = LOWER(vocareum_catalog_name)
# MAGIC       ) THEN
# MAGIC         SET catalog_already_exists = TRUE;
# MAGIC         SET create_view_with_catalog_name = CONCAT(
# MAGIC               "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
# MAGIC               "SELECT '", vocareum_catalog_name, "' AS catalog_name"
# MAGIC             );
# MAGIC       END IF;
# MAGIC
# MAGIC   ELSE
# MAGIC     SET catalog_name = CONCAT('learn_get_started_da_', safe_user_name);
# MAGIC     SET create_view_with_catalog_name = CONCAT(
# MAGIC       "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
# MAGIC       "SELECT '", catalog_name, "' AS catalog_name"
# MAGIC     );
# MAGIC
# MAGIC     IF EXISTS (
# MAGIC       SELECT 1
# MAGIC       FROM list_of_catalogs
# MAGIC       WHERE catalog_names = LOWER(catalog_name)
# MAGIC     )
# MAGIC       THEN
# MAGIC         SET catalog_already_exists = TRUE;
# MAGIC         SET test = 'catalog exists';
# MAGIC     ELSE
# MAGIC       SET create_catalog_statement = CONCAT(
# MAGIC         'CREATE CATALOG IF NOT EXISTS ',
# MAGIC         catalog_name
# MAGIC       );
# MAGIC       SET test = 'catalog does not exist';
# MAGIC       EXECUTE IMMEDIATE create_catalog_statement;
# MAGIC     END IF;
# MAGIC
# MAGIC   END IF;
# MAGIC
# MAGIC   EXECUTE IMMEDIATE create_view_with_catalog_name;
# MAGIC
# MAGIC -- ===================================================================
# MAGIC -- 4. Return final results
# MAGIC -- ===================================================================
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN vocareum_catalog_name <> 'NA' THEN 'Vocareum Workspace'
# MAGIC       ELSE 'Learner Workspace'
# MAGIC     END AS Workspace,
# MAGIC     CASE
# MAGIC       WHEN vocareum_catalog_name <> 'NA' THEN vocareum_catalog_name
# MAGIC       ELSE catalog_name
# MAGIC     END AS `User Catalog Name`,
# MAGIC     CASE
# MAGIC       WHEN catalog_already_exists = TRUE THEN 'Catalog Already Exists'
# MAGIC       ELSE 'Catalog Created'
# MAGIC     END AS `Catalog Status`;
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Declare and set the catalog variable from the temp view
# MAGIC DECLARE OR REPLACE my_catalog STRING;
# MAGIC SET VAR my_catalog = (SELECT catalog_name FROM catalog_name_vw);
# MAGIC
# MAGIC -- Set the active catalog
# MAGIC USE CATALOG IDENTIFIER(my_catalog);

# COMMAND ----------

my_catalog = spark.sql("SELECT current_catalog()").first()[0]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail;
# MAGIC USE SCHEMA retail;

# COMMAND ----------

SCHEMA_NAME_RETAIL = "retail"
VOLUME_NAME_RETAIL = "data"

## Create volume inside the schema
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{my_catalog}`.`{SCHEMA_NAME_RETAIL}`.`{VOLUME_NAME_RETAIL}`")