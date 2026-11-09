-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![Databricks Academy](./Includes/images/common/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 05 - Developing a Simple Pipeline
-- MAGIC
-- MAGIC In this demonstration, we will build a simple Apache Spark™ Declarative Pipeline **from scratch** using the new **Lakeflow Pipeline Editor** with declarative SQL. Rather than starting from pre-written pipeline files, you will create a new, empty ETL pipeline, configure its settings, and then **copy the SQL code for each layer of the medallion architecture directly into your pipeline file**.
-- MAGIC
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC - Create a new Apache Spark™ Declarative Pipeline in the Lakeflow Pipeline Editor.
-- MAGIC - Describe the SQL syntax used to create bronze, silver, and gold pipeline objects.
-- MAGIC - Add a configuration parameter to reference a raw data source volume.
-- MAGIC - Create, execute, and monitor a Spark Declarative Pipeline.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #f44336;
-- MAGIC   background: #ffebee;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC Before starting this notebook, select the required compute environment listed below.
-- MAGIC
-- MAGIC - **Serverless Compute, Version 5**  
-- MAGIC ![Serverless Select](./Includes/images/common/select-serverless.png)
-- MAGIC <br></br>
-- MAGIC   - How to select an environment version:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
-- MAGIC
-- MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this course.
-- MAGIC
-- MAGIC This cell will also reset your `/Volumes/learn_build_data_pipelines_sdp_YOUR_USERNAME/sdp_1_bronze/source` volume with the JSON files to the starting point, with one JSON file in each directory.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-REQUIRED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to view the value of the `source_volume_path` variable.
-- MAGIC
-- MAGIC    Confirm that the value references your **learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_1_bronze.source** volume. This volume contains the **orders**, **status**, and **customer** directories with the raw JSON files. You will use this path when configuring your pipeline.
-- MAGIC
-- MAGIC    **EXAMPLE PATH**: `/Volumes/learn_build_data_pipelines_sdp_YOUR_USERNAME/sdp_1_bronze/source`

-- COMMAND ----------

-- DBTITLE 1,Path to data source files
-- MAGIC %python
-- MAGIC print(source_volume_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Create the Spark Declarative Pipeline
-- MAGIC
-- MAGIC In this section, you will create a Spark Declarative Pipeline using the Lakeflow Pipeline Editor, configure its settings, and add a configuration parameter that points to your raw source volume.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Enable the Lakeflow Pipelines Editor
-- MAGIC ##### If you completed `02 Demo - Course Setup and Creating a Pipeline`, you have already enabled the required setting and can skip this subsection.
-- MAGIC
-- MAGIC
-- MAGIC 1. Complete the following steps to enable the **Lakeflow Pipelines Editor**:
-- MAGIC       - **NOTE:** This may already be enabled in your workspace.
-- MAGIC
-- MAGIC    a. In the top-right corner, select your user icon ![User Lab Icon](./Includes/images/common/user_lab_circle_icon.png).
-- MAGIC
-- MAGIC    b. Right-click on **Settings** and select **Open in New Tab**.
-- MAGIC
-- MAGIC    c. Select **Developer**.
-- MAGIC
-- MAGIC    d. Scroll to the bottom, enable **Lakeflow Pipelines Editor** if it is not already enabled, and select **Enable tabs for notebooks and files**.
-- MAGIC
-- MAGIC    ![Lakeflow Pipeline Editor](./Includes/images/creating-a-pipeline/lakeflow-pipeline-editor.png)
-- MAGIC
-- MAGIC    e. Refresh your browser to apply the changes.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Create a New ETL Pipeline
-- MAGIC
-- MAGIC Complete the following steps to create your Spark Declarative Pipeline:
-- MAGIC
-- MAGIC 1. In the main navigation pane, right-click **Jobs & Pipelines** and select **Open Link in New Tab**.
-- MAGIC
-- MAGIC 2. In the new tab, select **Create → ETL Pipeline**.
-- MAGIC
-- MAGIC    **NOTE:** If prompted to **Try the new Lakeflow Pipelines Editor**, choose **Enable Lakeflow Pipelines Editor**. This appears only if you did not complete the previous step.
-- MAGIC
-- MAGIC 3. At the top, complete the following:
-- MAGIC    - Name your pipeline `05 - Developing a Simple Pipeline-add-your-name`
-- MAGIC       
-- MAGIC        **NOTE:** If prompted to rename root folder, Please rename it as well
-- MAGIC    - Select your default **catalog** and **schema**:
-- MAGIC         - **Catalog:** Your **learn_build_data_pipelines_sdp_YOUR_USERNAME** catalog
-- MAGIC         - **Schema:** **sdp_1_bronze**
-- MAGIC
-- MAGIC       **NOTE:** Clear the selected schema using the cross icon to view all schemas.
-- MAGIC
-- MAGIC 4. Rename the **transformations** folder to `orders`.
-- MAGIC
-- MAGIC 5. Rename the **my_transformation.sql** file to `orders_pipeline.sql`.
-- MAGIC
-- MAGIC    **NOTE:** If your starter file is a Python file (`my_transformation.py`), delete it and create a new **SQL** file named `orders_pipeline.sql` instead. This demonstration focuses on SQL.
-- MAGIC
-- MAGIC 6. Leave the **Lakeflow Pipelines Editor** page open.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Explore and Modify Pipeline Settings
-- MAGIC
-- MAGIC 1. Select the **Settings** icon (gear icon) in your Pipeline Editor tab.
-- MAGIC     - A panel will open on the right.
-- MAGIC
-- MAGIC 2. **Pipeline Settings** - Review the pipeline details:
-- MAGIC     - Pipeline ID, type, name, mode, creator, owner, and run as.
-- MAGIC     - Leave these as is.
-- MAGIC
-- MAGIC 3. **Code Assets** - This section automatically references all included files in your project (your new `orders` folder and `orders_pipeline.sql` file).
-- MAGIC
-- MAGIC 4. **Default Location for Data Assets**
-- MAGIC
-- MAGIC     - Select **Edit the catalog and schema** and confirm the following:
-- MAGIC
-- MAGIC         | Field | What to Select |
-- MAGIC         |-------|---------------|
-- MAGIC         | **Default catalog** | Your **learn_build_data_pipelines_sdp_YOUR_USERNAME** catalog |
-- MAGIC         | **Default schema** | **sdp_1_bronze** |
-- MAGIC
-- MAGIC     - Select **Save** if you updated these.
-- MAGIC
-- MAGIC     > **NOTE:** With Apache Spark™ Declarative Pipelines you can publish streaming tables and materialized views to any catalog and schema. You are not restricted to the default catalog and schema.
-- MAGIC
-- MAGIC 5. **Compute** - Sets the pipeline compute
-- MAGIC
-- MAGIC     a. Select the **Edit** icon and confirm that **Serverless** is selected.
-- MAGIC
-- MAGIC     b. Deselect **Serverless** and review the available compute options, cluster policies, and tags.
-- MAGIC
-- MAGIC     c. **Reselect Serverless** and navigate back to the pipeline settings.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #ff9800;
-- MAGIC   background: #fff3e0;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
-- MAGIC     REQUIRED - Add a Configuration Parameter to Point to your Source Volume
-- MAGIC   </strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC 6. Complete the following steps to add a `source` configuration parameter pointing to your raw data volume. Your pipeline code will reference this parameter to locate the source files.
-- MAGIC
-- MAGIC    a. Run the cell below and copy the path to your source data volume.
-- MAGIC
-- MAGIC    b. In **Settings**, under **Configuration**, select **Add configuration**.
-- MAGIC
-- MAGIC    c. **Key** = `source`
-- MAGIC
-- MAGIC    d. **Value** = Paste the volume path of your `learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_1_bronze.source` volume.
-- MAGIC     - Example: `/Volumes/learn_build_data_pipelines_sdp_YOUR_USERNAME/sdp_1_bronze/source`
-- MAGIC
-- MAGIC    e. Select **Save**.
-- MAGIC
-- MAGIC **Use parameters with pipelines**:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/ldp/parameters) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/parameters) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/ldp/parameters)
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(source_volume_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. **Usage** - Review the **Usage** section. This is where you can assign a budget policy for Serverless compute. Tags are applied to compute activity and logged in your billing records.
-- MAGIC
-- MAGIC 8. **Notifications** - Enables you to add notifications on pipeline runs.
-- MAGIC
-- MAGIC 9. **Advanced Settings**
-- MAGIC
-- MAGIC      a. Expand **Advanced Settings** and select **Edit advanced settings**. Review the following options:
-- MAGIC
-- MAGIC     - **Channel** - controls the pipeline runtime version. Leave as **Current**.
-- MAGIC
-- MAGIC     - **Event Logs** - optionally write pipeline audit logs, data quality checks, and lineage to a table. Leave this deselected for now.
-- MAGIC 10. Select anywhere in the code editor to close the **Settings** panel.
-- MAGIC
-- MAGIC You now have an empty pipeline configured with a default catalog, default schema, and a `source` configuration parameter. Next, you will add the pipeline code.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Build the Pipeline Code
-- MAGIC
-- MAGIC In this section, you will build a simple medallion architecture by adding SQL to your `orders_pipeline.sql` file, one layer at a time:
-- MAGIC
-- MAGIC 1. **Bronze** - incrementally ingest the raw JSON orders files into a streaming table.
-- MAGIC 2. **Silver** - clean and type the bronze data in a second streaming table.
-- MAGIC 3. **Gold** - aggregate the silver data into a daily-orders materialized view.
-- MAGIC
-- MAGIC > **NOTE:** You can also build pipelines with Python. This demonstration focuses on SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Create the Bronze Streaming Table
-- MAGIC
-- MAGIC 1. Start by creating the **bronze streaming table** that incrementally ingests the raw JSON orders files from your source volume.
-- MAGIC
-- MAGIC    Copy the SQL code below and paste it into your `orders_pipeline.sql` file.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC --------------------------------------------------------------------------------------------------------------
-- MAGIC -- A. Create the bronze streaming table from the raw JSON files in your source volume
-- MAGIC --------------------------------------------------------------------------------------------------------------
-- MAGIC CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.orders_bronze_demo05
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   *,
-- MAGIC   current_timestamp() AS processing_time,   -- Records when each row was ingested
-- MAGIC   _metadata.file_name AS source_file        -- Captures the source file name for lineage
-- MAGIC FROM STREAM read_files(                      -- Performs incremental ingestion with checkpoints using Auto Loader
-- MAGIC     "${source}/orders",                      -- Uses the 'source' configuration parameter set in the pipeline settings
-- MAGIC     format => 'JSON'
-- MAGIC );
-- MAGIC </code></pre>
-- MAGIC
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC
-- MAGIC   const text = el.innerText;
-- MAGIC
-- MAGIC   // Preferred modern API
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     console.error("Fallback copy failed:", err);
-- MAGIC     alert("Could not copy to clipboard. Please copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Review the code**
-- MAGIC - `CREATE OR REFRESH STREAMING TABLE` creates the **sdp_1_bronze.orders_bronze_demo05** streaming table, the landing zone for the raw orders data.
-- MAGIC - `FROM STREAM read_files("${source}/orders", ...)` uses **Auto Loader** for incremental ingestion and references the `source` configuration parameter you set in the pipeline settings.
-- MAGIC - The `processing_time` and `source_file` columns are added for auditing and lineage.
-- MAGIC - Because this is a streaming table, the pipeline **incrementally processes only new files** on each run.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Create the Silver Streaming Table
-- MAGIC
-- MAGIC 1. Next, create the **silver streaming table** that reads incrementally from the bronze table, then selects and cleans the relevant columns.
-- MAGIC
-- MAGIC    Copy the SQL code below and paste it into your `orders_pipeline.sql` file, below the bronze code.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC --------------------------------------------------------------------------------------------------------------
-- MAGIC -- B. Create the silver streaming table from the bronze streaming table
-- MAGIC --------------------------------------------------------------------------------------------------------------
-- MAGIC CREATE OR REFRESH STREAMING TABLE sdp_2_silver.orders_silver_demo05
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   order_id,
-- MAGIC   timestamp(order_timestamp) AS order_timestamp,   -- Casts the order_timestamp to a TIMESTAMP type
-- MAGIC   customer_id,
-- MAGIC   notifications
-- MAGIC FROM STREAM sdp_1_bronze.orders_bronze_demo05;      -- Incrementally reads from the bronze streaming table
-- MAGIC </code></pre>
-- MAGIC
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC
-- MAGIC   const text = el.innerText;
-- MAGIC
-- MAGIC   // Preferred modern API
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     console.error("Fallback copy failed:", err);
-- MAGIC     alert("Could not copy to clipboard. Please copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Review the code**
-- MAGIC - `CREATE OR REFRESH STREAMING TABLE` creates the **sdp_2_silver.orders_silver_demo05** streaming table.
-- MAGIC - `FROM STREAM sdp_1_bronze.orders_bronze_demo05` reads **incrementally** from the bronze streaming table, so only new bronze rows flow through on each run.
-- MAGIC - The query selects only the relevant columns and casts `order_timestamp` to a proper `TIMESTAMP` type.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Create the Gold Materialized View
-- MAGIC
-- MAGIC 1. Finally, create the **gold materialized view** that aggregates the silver data into daily order counts.
-- MAGIC
-- MAGIC    Copy the SQL code below and paste it into your `orders_pipeline.sql` file, below the silver code.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC --------------------------------------------------------------------------------------------------------------
-- MAGIC -- C. Create the gold materialized view aggregating the silver table by order date
-- MAGIC --------------------------------------------------------------------------------------------------------------
-- MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.gold_orders_by_date_demo05
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   date(order_timestamp) AS order_date,
-- MAGIC   count(*) AS total_daily_orders
-- MAGIC FROM sdp_2_silver.orders_silver_demo05   -- Aggregates the full silver table with optimizations where applicable
-- MAGIC GROUP BY date(order_timestamp);
-- MAGIC </code></pre>
-- MAGIC
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC
-- MAGIC   const text = el.innerText;
-- MAGIC
-- MAGIC   // Preferred modern API
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     console.error("Fallback copy failed:", err);
-- MAGIC     alert("Could not copy to clipboard. Please copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Review the code**
-- MAGIC - `CREATE OR REFRESH MATERIALIZED VIEW` creates the **sdp_3_gold.gold_orders_by_date_demo05** materialized view.
-- MAGIC - It aggregates the silver table into **daily order counts** (`GROUP BY date(order_timestamp)`).
-- MAGIC - A `MATERIALIZED VIEW` is used here instead of a streaming table because `GROUP BY` aggregations can't be incrementally computed as a streaming table. Databricks still optimizes recomputation where possible.
-- MAGIC - **Incremental refresh for materialized views**:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/optimizations/incremental-refresh) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/optimizations/incremental-refresh)
-- MAGIC
-- MAGIC **Key Differences ST/MV**:
-- MAGIC   - Bronze and Silver use `STREAMING TABLE` for incremental processing.
-- MAGIC   - Gold uses `MATERIALIZED VIEW` because `GROUP BY` aggregations can't be incrementally computed as a streaming table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Run the Spark Declarative Pipeline

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### D1. Dry Run the Pipeline
-- MAGIC Using a dry run, you can **check for problems in a pipeline's source code** without waiting for tables to be created or updated.
-- MAGIC
-- MAGIC This feature is useful when developing or testing pipelines because it lets you quickly find and fix errors in your pipeline, such as incorrect table or column names.
-- MAGIC
-- MAGIC
-- MAGIC 1. <span style="background-color: #fff3e0; padding: 2px 6px; border-radius: 4px;">Select **Dry Run** from the top navigation bar (if your screen is small, you might have to select the drop-down next to the **Run pipeline** button in the top navigation bar).</span>
-- MAGIC
-- MAGIC     - Notice that the pipeline switches to **DRY RUN** mode in the left window and begins processing each step (this takes about ~1 minute).
-- MAGIC
-- MAGIC     - After the dry run completes, you should see three items in the **Pipeline graph** (Select the graph icon in the right navigation bar if necessary):
-- MAGIC
-- MAGIC       ![Graph](./Includes/images/developing-a-simple-pipeline/dry-run-pipeline-graph.png)
-- MAGIC
-- MAGIC 2. Explore the bottom window and observe the following columns:
-- MAGIC
-- MAGIC     - **Catalog** - the catalog each object will write to
-- MAGIC
-- MAGIC     - **Schema** - the schema each object will write to
-- MAGIC
-- MAGIC     - **Type** - the object type
-- MAGIC
-- MAGIC       ![Window](./Includes/images/developing-a-simple-pipeline/pipeline-dry-run-window.png)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #ff9800;
-- MAGIC   background: #fff3e0;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
-- MAGIC     Troubleshooting a Pipeline Error
-- MAGIC   </strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC If your pipeline returns an error, verify the following in your <strong>Pipeline Settings</strong>:
-- MAGIC
-- MAGIC   - Default catalog is set to your <strong>learn_build_data_pipelines_sdp_YOUR_USERNAME</strong> catalog
-- MAGIC
-- MAGIC   - Default schema is set to <strong>sdp_1_bronze</strong>
-- MAGIC
-- MAGIC   - The `source` configuration variable references your volume path: `/Volumes/learn_build_data_pipelines_sdp_YOUR_USERNAME/sdp_1_bronze/source`
-- MAGIC
-- MAGIC   - All three SQL statements were pasted into your `orders_pipeline.sql` file
-- MAGIC
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Run the Spark Declarative Pipeline
-- MAGIC
-- MAGIC 1. Select the drop-down next to the **Run pipeline** button in the top navigation bar.
-- MAGIC
-- MAGIC You will see two options:
-- MAGIC
-- MAGIC | Update Type | Materialized View | Streaming Table |
-- MAGIC |-------------|------------------|-----------------|
-- MAGIC | **Run pipeline** - Refresh | Updates results to reflect the current results for the defining query. Will examine the costs, and perform an incremental refresh if it is more cost-efficient. | Processes new records through logic defined in streaming tables and flows. |
-- MAGIC | **Run pipeline with full table refresh** - Full refresh of the pipeline | Updates results to reflect the current results for the defining query. | Clears data from streaming tables, clears state information (checkpoints) from flows, and reprocesses all records from the data source. |
-- MAGIC
-- MAGIC 2. Select **Run pipeline** and monitor each stage in the right window.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #1976d2;
-- MAGIC   background: #e3f2fd;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
-- MAGIC     Information
-- MAGIC   </strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC - A full refresh deletes all objects and checkpoints. Be careful when using.
-- MAGIC - To prevent a full refresh from being triggered, set the table property: `pipelines.reset.allowed = false`
-- MAGIC - **Pipeline refresh semantics**:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/ldp/updates#pipeline-refresh-semantics) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/updates#pipeline-refresh-semantics) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/ldp/updates#pipeline-refresh-semantics)
-- MAGIC
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D3. Monitor the Pipeline Run
-- MAGIC
-- MAGIC 1. While the pipeline is running (~1 minute), observe the **Pipeline graph** on the right.
-- MAGIC
-- MAGIC     As each object is created, a data flow appears and row counts update as processing completes.
-- MAGIC
-- MAGIC     Expected results:
-- MAGIC     - **174 rows** ingested from Raw JSON → Bronze → Silver
-- MAGIC     - **7 rows** in the materialized view aggregation

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### D4. Explore the Completed Pipeline
-- MAGIC
-- MAGIC After the pipeline completes, explore the results:
-- MAGIC
-- MAGIC 1. Review the **Pipeline graph**. The graph visualizes the full data flow.
-- MAGIC
-- MAGIC 2. Review the bottom window for details on tables and materialized views created, including locations, durations, and row counts.
-- MAGIC
-- MAGIC    a. Select the **Performance** tab to view performance metrics, then return to **Tables**.
-- MAGIC
-- MAGIC    b. Select the **orders_bronze_demo05** streaming table to view its **data**, **Table metrics**, and **Performance**.
-- MAGIC
-- MAGIC    c. Select the arrow to the left of **All tables** in the bottom window to return to the full pipeline objects list.
-- MAGIC
-- MAGIC    d. Select the **gold_orders_by_date_demo05** materialized view and confirm it summarizes data by date as expected.
-- MAGIC       - Use the navigation bar in the bottom window to explore the **Data**, **Columns**, **Table metrics**, and **Performance** tabs for the materialized view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D5. Explore the DAG (Pipeline Graph)
-- MAGIC
-- MAGIC 1. You can also select objects directly within the DAG (Pipeline Graph) to view the details in the bottom window.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D6. Run the Pipeline Again
-- MAGIC
-- MAGIC 1. Select **Run pipeline** again and explore the updated results while it runs.
-- MAGIC
-- MAGIC 2. After the second run completes, note that no new rows were added to the streaming tables.
-- MAGIC
-- MAGIC **This is expected since no new files were added to the data source and the checkpoints know the current data was already ingested**.
-- MAGIC
-- MAGIC ![Run 2](./Includes/images/developing-a-simple-pipeline/run-2-no-changes.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Add a New File to Cloud Storage
-- MAGIC
-- MAGIC 1. Run the cell below to add a new JSON file (**01.json**) to your volume at:  `/Volumes/learn_build_data_pipelines_sdp_YOUR_USERNAME/sdp_1_bronze/source/orders`.
-- MAGIC
-- MAGIC       This will simulate files being added to cloud storage.

-- COMMAND ----------

-- DBTITLE 1,Add a new JSON file to the data source
-- MAGIC %python
-- MAGIC ## Use the course seed data bundled in Includes/data
-- MAGIC course_data_path = get_course_data_path()
-- MAGIC
-- MAGIC ## Land another JSON file to your orders volume
-- MAGIC copy_workspace_files_to_volume(
-- MAGIC     src_workspace_folder=f'{course_data_path}/orders',
-- MAGIC     target_volume_path=f'{source_volume_path}/orders',
-- MAGIC     n=2
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to view the new file in your volume:
-- MAGIC
-- MAGIC    a. Select the **Catalog** icon ![Catalog Icon](./Includes/images/common/catalog_icon.png) from the left navigation pane.
-- MAGIC
-- MAGIC    b. Expand your **learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_1_bronze.source** volume.
-- MAGIC
-- MAGIC    c. Expand the **orders** directory.
-- MAGIC
-- MAGIC    d. You should see two files in your volume: **00.json** and **01.json** (refresh if necessary).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell below to view the data in the new **/orders/01.json** file. Notice the following:
-- MAGIC
-- MAGIC    - The **01.json** file contains new orders.
-- MAGIC    - The **01.json** file has 25 rows.

-- COMMAND ----------

-- DBTITLE 1,Preview the 01.json file
-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC   SELECT *
-- MAGIC   FROM json.`{source_volume_path}/orders/01.json`
-- MAGIC ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Go back to the **orders_pipeline.sql** file and select **Run Pipeline** to execute your ETL pipeline again with the new file.
-- MAGIC
-- MAGIC    - Watch the pipeline run and notice only **25 rows** are added to the bronze and silver tables.
-- MAGIC
-- MAGIC    - This happens because:
-- MAGIC       - The pipeline has already processed the initial **00.json** file (174 rows)
-- MAGIC       - It now reads only the new **01.json** file (25 rows)
-- MAGIC       - New rows are appended to the **streaming tables**
-- MAGIC       - The materialized view is **incrementally recomputed** using the latest data (should have a total of **8 rows**)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Checkpoint
-- MAGIC ![Final](./Includes/images/developing-a-simple-pipeline/demo2-final.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Exploring Your Streaming Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F1. View the Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the new streaming tables and materialized view in your catalog. Complete the following:
-- MAGIC
-- MAGIC    a. Select the catalog icon ![Catalog Icon](./Includes/images/common/catalog_icon.png) in the left navigation pane.
-- MAGIC
-- MAGIC    b. Expand your **learn_build_data_pipelines_sdp_YOUR_USERNAME** catalog.
-- MAGIC
-- MAGIC    c. Expand the schemas **sdp_1_bronze**, **sdp_2_silver**, and **sdp_3_gold**.
-- MAGIC       - Notice that the two streaming tables and materialized view are correctly placed in your schemas.
-- MAGIC
-- MAGIC          - Streaming Bronze Table: **learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_1_bronze.orders_bronze_demo05**
-- MAGIC
-- MAGIC          - Streaming Silver Table: **learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_2_silver.orders_silver_demo05**
-- MAGIC
-- MAGIC          - Gold Materialized View: **learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_3_gold.gold_orders_by_date_demo05**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to view the data in the **learn_build_data_pipelines_sdp_YOUR_USERNAME.sdp_1_bronze.orders_bronze_demo05** table.
-- MAGIC
-- MAGIC    Before you run the cell, how many rows should this streaming table have?
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC       - The table contains 199 rows (**00.json** had 174 rows, and **01.json** had 25 rows).
-- MAGIC       - In the **source_file** column you can see the exact file the rows were ingested from.
-- MAGIC       - In the **processing_time** column you can see the exact time the rows were ingested.

-- COMMAND ----------

select current_schema(), current_catalog();

-- COMMAND ----------

select current_catalog(), current_schema();
show tables like '%bronze%'


-- COMMAND ----------

-- DBTITLE 1,View the streaming table
SELECT *
FROM learn_build_data_pipelines_sdp_joemar_brillo.sdp_1_bronze.orders_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F2. View the Table History
-- MAGIC
-- MAGIC 1. Run the code below to view the history of the **orders_bronze_demo05** streaming table.
-- MAGIC
-- MAGIC       Notice the following:
-- MAGIC
-- MAGIC       - In the **operation** column, the last two updates are **STREAMING UPDATE**, not `WRITE` or `MERGE`. This confirms the table is being written to incrementally by a streaming query.
-- MAGIC
-- MAGIC       - Expand the **operationParameters** values for the last two updates. Notice both use `"outputMode": "Append"` and each has an incrementing **epochId** (`0` for the initial load, `1` for the incremental update).
-- MAGIC
-- MAGIC       - Find the **operationMetrics** column. Expand the values for the last two updates. Observe the following:
-- MAGIC
-- MAGIC          - It displays various metrics for the streaming update: **numRemovedFiles, numOutputRows, numOutputBytes, and numAddedFiles**.
-- MAGIC
-- MAGIC          - In the `numOutputRows` values
-- MAGIC             - **174 rows** were added in the first update
-- MAGIC             -  **25 rows** in the second.
-- MAGIC             - If this were a batch pipeline doing a full re-read, the second run would show 199 rows. The fact that it shows only 25 proves that only new data was processed.

-- COMMAND ----------

DESCRIBE HISTORY sdp_1_bronze.orders_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Viewing Spark Declarative Pipelines with the Pipelines UI
-- MAGIC
-- MAGIC After building and running your pipeline using the **orders_pipeline.sql** file in the steps above, you can view the pipeline(s) you created in your workspace via the **Jobs and Pipelines** UI.
-- MAGIC
-- MAGIC 1. Complete the following steps to view the pipeline you created:
-- MAGIC
-- MAGIC    a. In the main applications navigation pane on the far left (you may need to expand it by selecting the ![Expand Navigation Pane](./Includes/images/developing-a-simple-pipeline/expand_main_navigation.png) icon at the top left of your workspace) right-click on **Jobs & Pipelines** and select **Open Link in a New Tab**.
-- MAGIC
-- MAGIC    b. This should take you to the pipelines you have created. You should see your **05 - Developing a Simple Pipeline-your name** pipeline.
-- MAGIC
-- MAGIC    c. Select your **05 - Developing a Simple Pipeline-your name**.
-- MAGIC
-- MAGIC    d. Under your pipeline name, select the drop-down with the timestamp. Here you can view the **Pipeline graph** and other metrics for each run of the pipeline.
-- MAGIC
-- MAGIC    e. Close the pipeline UI tab you opened.
-- MAGIC
-- MAGIC    ![Jobs & Pipelines](./Includes/images/developing-a-simple-pipeline/demo_2_view_in_jobs_pipelines.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - **Apache Spark™ Declarative Pipelines documentation**:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/dlt/) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt/)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
-- MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
-- MAGIC <script>
-- MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
-- MAGIC </script>
-- MAGIC