-- Databricks notebook source
-- Create safe function for catalog name
CREATE OR REPLACE TEMPORARY FUNCTION safe_uc_name(value STRING)
RETURNS STRING
RETURN
  COALESCE(
    NULLIF(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(TRIM(value)),
          '[^a-z0-9_]',
          '_'
        ),
        '_+',
        '_'
      ),
      ''
    ),
    'user'
  );

-- COMMAND ----------

-- Create temp view of all available catalogs
CREATE OR REPLACE TEMPORARY VIEW list_of_catalogs AS
SELECT LOWER(catalog_name) AS catalog_names
FROM system.information_schema.catalogs;

-- COMMAND ----------

-- Start SQL scripting block to create and or set catalog based on Workspace
BEGIN

-- =========================================
-- 1. Declare variables
-- =========================================
  DECLARE user_email STRING;
  DECLARE user_name STRING;
  DECLARE safe_user_name STRING;
  DECLARE vocareum_catalog_name STRING DEFAULT 'NA';
  DECLARE catalog_name STRING DEFAULT 'NA';
  DECLARE catalog_already_exists BOOLEAN DEFAULT FALSE;
  DECLARE create_catalog_statement STRING DEFAULT '';
  DECLARE create_view_with_catalog_name STRING DEFAULT '';
  DECLARE test STRING DEFAULT '';


-- =========================================
-- 2. Capture current user
-- =========================================
  SET user_email = current_user();
  SET user_name = SPLIT(user_email, '@')[0];
  SET safe_user_name = (SELECT safe_uc_name(SPLIT(current_user(), '@')[0]));


-- ==================================================================================
-- 3. Determine Vocareum or Non Vocareum Workspace and set and/or create catalog
-- ==================================================================================
  IF (LOWER(user_email) LIKE '%@vocareum.com')
    THEN
      
      SET vocareum_catalog_name = safe_user_name;
      
      IF EXISTS (
        SELECT 1
        FROM list_of_catalogs
        WHERE catalog_names = LOWER(vocareum_catalog_name)
      ) THEN  

        SET catalog_already_exists = TRUE;

        SET create_view_with_catalog_name = CONCAT(
              "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
              "SELECT '", vocareum_catalog_name, "' AS catalog_name"
            );

      END IF; 

  ELSE

    SET catalog_name = CONCAT('learn_get_started_da_', safe_user_name);

    SET create_view_with_catalog_name = CONCAT(
      "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
      "SELECT '", catalog_name, "' AS catalog_name"
    );

    IF EXISTS (
      SELECT 1
      FROM list_of_catalogs
      WHERE catalog_names = LOWER(catalog_name)
    ) 
      THEN 
      
        SET catalog_already_exists = TRUE;
        SET test = 'catalog exists';
    
    ELSE 

      SET create_catalog_statement = CONCAT(
        'CREATE CATALOG IF NOT EXISTS ',
        catalog_name
      );

      SET test = 'catalog does not exist';

      EXECUTE IMMEDIATE create_catalog_statement;
    END IF;

  END IF;

  EXECUTE IMMEDIATE create_view_with_catalog_name;


-- ===================================================================
-- 4. Return Final Script Results
-- ===================================================================
  SELECT
    CASE
      WHEN vocareum_catalog_name <> 'NA' THEN 'Vocareum Workspace'
      ELSE 'Learner Workspace'
    END AS Workspace,
    CASE
      WHEN vocareum_catalog_name <> 'NA' THEN vocareum_catalog_name
      ELSE catalog_name
    END AS `User Catalog Name`,
    CASE
      WHEN catalog_already_exists = TRUE THEN 'Catalog Already Exists'
      ELSE 'Catalog Created'
    END AS `Catalog Status`;
END;

-- COMMAND ----------

-- Create the global SQL variable with the catalog name using the temp view
DECLARE OR REPLACE my_catalog STRING;
SET VAR my_catalog = (SELECT catalog_name FROM catalog_name_vw);

-- Set default catalog
USE CATALOG IDENTIFIER(my_catalog);

-- Confirm the default catalog
-- SELECT current_catalog();

-- COMMAND ----------

BEGIN
  DECLARE schema_exists BOOLEAN DEFAULT FALSE;

  SET schema_exists = EXISTS (
    SELECT 1
    FROM information_schema.schemata
    WHERE schema_name = 'retail'
  );

  IF schema_exists THEN
    USE SCHEMA retail;
    SELECT 'Schema retail set successfully.' AS Status;
  ELSE
    -- The 'retail' schema is missing: the learner skipped the Required Setup.
    -- This notebook runs on a SQL Warehouse (pure SQL, no HTML banners), so we
    -- surface a loud, multi-row, step-by-step message that is impossible to miss
    -- in the results grid. The column header itself shouts the required action.
    SELECT explode(array(
      '⚠⚠⚠  SETUP REQUIRED — STOP AND READ  ⚠⚠⚠',
      ' ',
      'The "retail" schema was not found in your catalog.',
      'The queries in this notebook WILL FAIL until you complete the steps below.',
      ' ',
      'STEP 1:  Open the "00 - Course Overview" notebook.',
      'STEP 2:  Run the "Required Setup" cell — it creates the retail schema, volume, and tables.',
      'STEP 3:  Come back to THIS notebook and re-run it from the top.'
    )) AS `⚠  ACTION REQUIRED — RUN THE REQUIRED SETUP FIRST  ⚠`;
  END IF;
END;