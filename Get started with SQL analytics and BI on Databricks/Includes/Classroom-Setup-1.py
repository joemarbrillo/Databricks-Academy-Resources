# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

import os
import shutil

def copy_file_to_volume(
    src_workspace_path: str,
    target_volume_path: str,
    overwrite: bool = False
):
    """
    Copy a file from a Databricks workspace path to a Unity Catalog volume.

    Parameters
    ----------
    src_workspace_path : str
        Full workspace path, for example
        /Workspace/Users/user@databricks.com/data/file.csv

    target_volume_path : str
        Full volume path, for example
        /Volumes/catalog/schema/volume/file.csv

    overwrite : bool, default False
        Whether to overwrite the file if it already exists
    """
    # Check if source file exists
    if not os.path.exists(src_workspace_path):
        raise FileNotFoundError(
            f'Source file does not exist: {src_workspace_path}'
        )

    # Check if target file exists
    if os.path.exists(target_volume_path) and not overwrite:
        print(f'File already exists at target location. Skipping copy: {target_volume_path}')
    else:
        shutil.copy(src_workspace_path, target_volume_path)

# COMMAND ----------

########################################
# Copy 3 CSV files and create tables
########################################

RETAIL_FILES = [
    "customers",
    "sales",
    "sales_orders"
]

retail_volume_base_path = f"/Volumes/{my_catalog}/{SCHEMA_NAME_RETAIL}/{VOLUME_NAME_RETAIL}"

## Copy each CSV file from the workspace into the retail volume
for table_name in RETAIL_FILES:
    src_path    = os.path.join(_COURSE_ROOT_DIR, "Includes", "data", f"{table_name}.csv")
    target_path = f"{retail_volume_base_path}/{table_name}.csv"

    #print(f"Source path  : {src_path}")
    #print(f"Target path  : {target_path}")

    copy_file_to_volume(
        src_workspace_path=src_path,
        target_volume_path=target_path,
        overwrite=False
    )

# COMMAND ----------

## Create a Delta table for each CSV file
print(f"Creating tables in Retail schema ...")

for table_name in RETAIL_FILES:
    retail_table_ref = f"`{my_catalog}`.`{SCHEMA_NAME_RETAIL}`.`{table_name}`"
    retail_csv_path  = f"{retail_volume_base_path}/{table_name}.csv"

    

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {retail_table_ref}
        AS
        SELECT * EXCEPT(_tmp_rescued)
        FROM read_files(
            '{retail_csv_path}',
            format => 'csv',
            header => 'true',
            inferSchema => 'true',
            rescuedDataColumn => '_tmp_rescued'
        )
    """)

print("Tables in Retail schema ready")