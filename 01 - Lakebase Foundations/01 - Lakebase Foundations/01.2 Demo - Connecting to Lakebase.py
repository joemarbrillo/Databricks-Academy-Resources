# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](../Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo - Connecting to Lakebase
# MAGIC
# MAGIC There are a few approaches to connecting with Lakebase, which we'll illustrate in this demo using live notebook code.
# MAGIC
# MAGIC In a subsequent demo, we'll apply these patterns to wire these connections into a Databricks app and handle end-to-end application integration.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Explain the various options for accessing Lakebase, and when each is applicable
# MAGIC - Access Lakebase from Python code using a PostgreSQL driver
# MAGIC - Perform DDL and CRUD operations from Python code

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %md-sandbox
# MAGIC ## A. Overview of Lakebase
# MAGIC
# MAGIC Lakebase is a fully managed PostgreSQL OLTP service integrated into Databricks, meant for low-latency transactional workloads (app state, feature serving, operational apps) that sit alongside your lakehouse analytics.
# MAGIC
# MAGIC Lakebase features:
# MAGIC
# MAGIC - Autoscaling compute based on demand
# MAGIC - Scale-to-zero when idle
# MAGIC - Organized resources as **projects** that support **branches** for isolation and restoring from any point in time
# MAGIC - Read replicas for scaling read-heavy workloads
# MAGIC - Can be configured to support high availability requirements
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 8px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Note
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC   The original version, <strong>Lakebase Provisioned</strong>, used fixed-size, manually scaled database instances. All Provisioned instances have since been migrated to the current platform.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Classroom Setup (REQUIRED)
# MAGIC
# MAGIC This section performs tasks required for the successful completion of this lab. Please run through all cells before proceeding.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Select Serverless Compute
# MAGIC
# MAGIC Use serverless compute (v6) to run the cells within this notebook. Serverless will ensure that you're using the software environment on which this material was tested.

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %md-sandbox
# MAGIC ### B2. Run Setup
# MAGIC
# MAGIC Run the following cell, which sets up your environment. This will:
# MAGIC - Configure variables used throughout the notebooks
# MAGIC - Create a Lakebase project for you to work with
# MAGIC - Output important configuration information below

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-01.2

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Understanding the Output
# MAGIC
# MAGIC The output from the setup cell above provides a link to a pre-created Lakebase project for use with this demo. We'll be referring back to this page later in the demo.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Connection Methods
# MAGIC
# MAGIC Lakebase supports two main connection methods.
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:20px;margin:20px 0;">
# MAGIC   <div style="background:#F9F7F4;border:1px solid #DCE0E2;border-radius:16px;box-shadow:0 2px 8px rgba(27,49,57,0.06);overflow:hidden;">
# MAGIC     <div style="height:8px;background:#FF5F46;"></div>
# MAGIC     <div style="padding:20px 22px;">
# MAGIC       <div style="font-size:24px;font-weight:700;line-height:1.2;color:#1B3139;margin-bottom:14px;">PostgreSQL Driver</div>
# MAGIC       <div style="font-size:15px;line-height:1.6;color:#303F47;margin-bottom:14px;">
# MAGIC       Provides native PostgreSQL access through an adapter like <code>psycopg3</code> or <code>SQLAlchemy</code>.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4;border:1px solid #DCE0E2;border-radius:16px;box-shadow:0 2px 8px rgba(27,49,57,0.06);overflow:hidden;">
# MAGIC     <div style="height:8px;background:#2272B4;"></div>
# MAGIC     <div style="padding:20px 22px;">
# MAGIC       <div style="font-size:24px;font-weight:700;line-height:1.2;color:#1B3139;margin-bottom:14px;">Lakebase Data API</div>
# MAGIC       <div style="font-size:15px;line-height:1.6;color:#303F47;margin-bottom:14px;">
# MAGIC         Provides HTTPS-based access to the database.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1100 500"
# MAGIC      font-family="system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif">
# MAGIC   <defs>
# MAGIC     <marker id="arrow" viewBox="0 0 12 12" refX="11" refY="6"
# MAGIC             markerWidth="10" markerHeight="10" orient="auto">
# MAGIC       <path d="M0,0 L12,6 L0,12 z" fill="#303F47"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <rect width="1100" height="500" fill="#FFFFFF"/>
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Nested Lakebase containers (right side, bottom layer first)   -->
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Lakebase Project (outermost — Yellow 600) -->
# MAGIC   <rect x="600" y="50" width="460" height="410" rx="14" fill="#FFAB00"/>
# MAGIC   <text x="830" y="80" text-anchor="middle"
# MAGIC         font-size="14" font-weight="800" fill="#1B3139">Lakebase Project</text>
# MAGIC   <!-- Branch (middle — Oat Medium) -->
# MAGIC   <rect x="630" y="110" width="400" height="320" rx="12"
# MAGIC         fill="#EEEDE9" stroke="#DCE0E2" stroke-width="1"/>
# MAGIC   <text x="830" y="140" text-anchor="middle"
# MAGIC         font-size="14" font-weight="800" fill="#1B3139">Branch</text>
# MAGIC   <!-- Lakebase Endpoint (inner — Oat Light) -->
# MAGIC   <rect x="660" y="165" width="340" height="240" rx="10"
# MAGIC         fill="#F9F7F4" stroke="#DCE0E2" stroke-width="1"/>
# MAGIC   <text x="830" y="195" text-anchor="middle"
# MAGIC         font-size="14" font-weight="800" fill="#1B3139">Compute</text>
# MAGIC   <!-- Database (innermost — white) -->
# MAGIC   <rect x="760" y="245" width="140" height="80" rx="8"
# MAGIC         fill="#FFFFFF" stroke="#DCE0E2" stroke-width="1.2"/>
# MAGIC   <text x="830" y="293" text-anchor="middle"
# MAGIC         font-size="16" font-weight="800" fill="#1B3139">Database</text>
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Left-side boxes                                               -->
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Python Clients -->
# MAGIC   <rect x="40" y="120" width="120" height="80" rx="12"
# MAGIC         fill="#F9F7F4" stroke="#DCE0E2" stroke-width="1.2"/>
# MAGIC   <text x="100" y="155" text-anchor="middle"
# MAGIC         font-size="14" font-weight="700" fill="#1B3139">Python-based</text>
# MAGIC   <text x="100" y="178" text-anchor="middle"
# MAGIC         font-size="14" font-weight="700" fill="#1B3139">clients</text>
# MAGIC   <!-- PostgreSQL Driver (Lava 600) -->
# MAGIC   <rect x="220" y="120" width="160" height="80" rx="12" fill="#FF3621"/>
# MAGIC   <text x="300" y="155" text-anchor="middle"
# MAGIC         font-size="15" font-weight="800" fill="#FFFFFF">PostgreSQL</text>
# MAGIC   <text x="300" y="178" text-anchor="middle"
# MAGIC         font-size="15" font-weight="800" fill="#FFFFFF">Driver</text>
# MAGIC   <!-- HTTP Clients -->
# MAGIC   <rect x="40" y="300" width="120" height="80" rx="12"
# MAGIC         fill="#F9F7F4" stroke="#DCE0E2" stroke-width="1.2"/>
# MAGIC   <text x="100" y="335" text-anchor="middle"
# MAGIC         font-size="14" font-weight="700" fill="#1B3139">HTTP</text>
# MAGIC   <text x="100" y="358" text-anchor="middle"
# MAGIC         font-size="14" font-weight="700" fill="#1B3139">clients</text>
# MAGIC   <!-- Lakebase Data API (Blue 600) -->
# MAGIC   <rect x="220" y="300" width="160" height="80" rx="12" fill="#2272B4"/>
# MAGIC   <text x="300" y="335" text-anchor="middle"
# MAGIC         font-size="15" font-weight="800" fill="#FFFFFF">Lakebase</text>
# MAGIC   <text x="300" y="358" text-anchor="middle"
# MAGIC         font-size="15" font-weight="800" fill="#FFFFFF">Data API</text>
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Arrows (on top of all boxes)                                  -->
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Python Clients → PostgreSQL Driver -->
# MAGIC   <line x1="160" y1="160" x2="220" y2="160" stroke="#303F47" stroke-width="2"
# MAGIC         marker-end="url(#arrow)"/>
# MAGIC   <!-- HTTP Clients → Lakebase Data API -->
# MAGIC   <line x1="160" y1="340" x2="220" y2="340" stroke="#303F47" stroke-width="2"
# MAGIC         marker-end="url(#arrow)"/>
# MAGIC   <!-- Python Clients → Lakebase Data API (NEW — Python clients can use either) -->
# MAGIC   <path d="M 130 200 C 130 250, 200 300, 220 320"
# MAGIC         fill="none" stroke="#303F47" stroke-width="2"
# MAGIC         marker-end="url(#arrow)"/>
# MAGIC   <!-- PostgreSQL Driver → Database (upper entry, now angled) -->
# MAGIC   <path d="M 380 160 L 600 160 C 680 160, 730 220, 760 250"
# MAGIC         fill="none" stroke="#303F47" stroke-width="2"
# MAGIC         marker-end="url(#arrow)"/>
# MAGIC   <!-- Lakebase Data API → Database (lower entry, now angled) -->
# MAGIC   <path d="M 380 340 L 600 340 C 680 340, 730 350, 760 320"
# MAGIC         fill="none" stroke="#303F47" stroke-width="2"
# MAGIC         marker-end="url(#arrow)"/>
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- Pill labels on top of arrows                                  -->
# MAGIC   <!-- =========================================================== -->
# MAGIC   <!-- PostgreSQL wire protocol -->
# MAGIC   <rect x="410" y="146" width="170" height="28" rx="10"
# MAGIC         fill="#EEEDE9" stroke="#DCE0E2" stroke-width="1"/>
# MAGIC   <text x="495" y="165" text-anchor="middle"
# MAGIC         font-size="12" font-weight="600" fill="#1B3139">PostgreSQL wire protocol</text>
# MAGIC   <!-- HTTPS -->
# MAGIC   <rect x="455" y="326" width="70" height="28" rx="10"
# MAGIC         fill="#EEEDE9" stroke="#DCE0E2" stroke-width="1"/>
# MAGIC   <text x="490" y="345" text-anchor="middle"
# MAGIC         font-size="12" font-weight="600" fill="#1B3139">HTTPS</text>
# MAGIC </svg>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. PostgreSQL Driver
# MAGIC
# MAGIC Drivers like [psycopg3](https://www.psycopg.org/) or [SQLAlchemy](https://www.sqlalchemy.org/) can be employed to directly connect to Lakebase in the same way as any other Postgres database. In this case, your code uses the standard PostgreSQL wire protocol to interact with the database server.
# MAGIC
# MAGIC This is generally the best fit for Databricks apps, or for any situation requiring:
# MAGIC - close-to-DB interactions
# MAGIC - access to the entirety of SQL (complex queries, multi-statement transactions, prepared statements, and more) and/or Postgres ecosystem
# MAGIC - other libraries that expect a DB-API driver
# MAGIC - high-throughput, low-latency calls

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %md
# MAGIC ### C2. Lakebase Data API
# MAGIC
# MAGIC Lakebase can also expose a [PostgREST](https://docs.postgrest.org/en/stable/references/api.html)-compatible REST API [[AWS](https://docs.databricks.com/aws/en/oltp/projects/data-api) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/data-api) | [GCP](https://docs.databricks.com/gcp/en/oltp/projects/data-api)] that sits in front of the database. Schemas, tables and views are exposed as HTTP resources, with CRUD operations via standard HTTP verbs (`GET`/`POST`/`PATCH`/`DELETE`) and rich query parameters for filtering, ordering, pagination, and joins.
# MAGIC
# MAGIC This is a good option for:
# MAGIC - any client where using a Python-based PostgreSQL driver is not suitable or possible
# MAGIC - serverless functions or edge runtimes that prefer stateless HTTP over maintaining connections
# MAGIC - simple CRUD and query patterns

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Comparison
# MAGIC
# MAGIC Here is a summary of how the two connectivity options compare:
# MAGIC
# MAGIC <table>
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th>Aspect</th>
# MAGIC       <th>Lakebase Data API</th>
# MAGIC       <th>PostgreSQL driver (psycopg3, SQLAlchemy, etc)</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>Protocol</td>
# MAGIC       <td>REST (HTTPS + JSON)</td>
# MAGIC       <td>PostgreSQL wire protocol</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Authorization model</td>
# MAGIC       <td>Databricks OAuth bearer token in Authorization header</td>
# MAGIC       <td>Short-lived DB credentials (token as password) or Postgres passwords</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>API surface</td>
# MAGIC       <td>Auto-generated CRUD endpoints; query via URL params</td>
# MAGIC       <td>Arbitrary SQL, transactions, stored procedures, driver features</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Shape of responses</td>
# MAGIC       <td>JSON documents</td>
# MAGIC       <td>Python DB-API rows / typed values</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Connection behavior</td>
# MAGIC       <td>Stateless, per-request HTTP</td>
# MAGIC       <td>Stateful connections, pooling, prepared statements</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Usage</td>
# MAGIC       <td>Web/mobile/serverless, third-party integrations, simple CRUD</td>
# MAGIC       <td>Backend services, ETL, complex queries, ORMs, high-throughput servers</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Language/tooling neutrality</td>
# MAGIC       <td>Very high (any HTTP client)</td>
# MAGIC       <td>Python-specific (but same pattern in other languages via other drivers)</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Performance/throughput</td>
# MAGIC       <td>Good</td>
# MAGIC       <td>Best</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <style>
# MAGIC table{
# MAGIC   border-collapse: separate;
# MAGIC   border-spacing: 0;
# MAGIC   width: auto;
# MAGIC   max-width: 100%;
# MAGIC   margin: 16px 0;
# MAGIC   float: none;
# MAGIC   clear: both;
# MAGIC   display: table;
# MAGIC   font-size: 0.95em;
# MAGIC   line-height: 1.35;
# MAGIC   background: #ffffff;
# MAGIC   border-radius: 8px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 0 0 1px #303F47;
# MAGIC   margin-left: 2px !important;
# MAGIC   margin-right: 2px !important;
# MAGIC   box-sizing: border-box !important;
# MAGIC }
# MAGIC thead th{
# MAGIC   background: #DADADA;
# MAGIC   color: #1B3139;
# MAGIC   font-weight: 600;
# MAGIC   text-align: left;
# MAGIC   padding: 10px 12px;
# MAGIC }
# MAGIC tbody td{
# MAGIC   padding: 10px 12px;
# MAGIC   border-top: 1px solid #DCE0E2;
# MAGIC }
# MAGIC tbody tr:nth-child(odd){
# MAGIC   background: #FFFFFF !important;
# MAGIC }
# MAGIC tbody tr:nth-child(even){
# MAGIC   background: #F5F3F2 !important;
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %md
# MAGIC ## D. Using a PostgreSQL Driver
# MAGIC
# MAGIC In this section, we'll see how to connect to Lakebase using a PostgreSQL driver. In this example we'll use [psycopg3](https://www.psycopg.org/psycopg3/docs/), a modern, widely-used PostgreSQL driver for Python. [SQLAlchemy](https://www.sqlalchemy.org/) is another good option.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. PostgreSQL Connection Overview
# MAGIC
# MAGIC In the simplest case, a Python program connects to Postgres by opening a single connection, running queries, and closing the connection when it is finished. This pattern works well for scripts or notebooks where a small amount of work is performed sequentially. However, web applications often execute many queries across multiple concurrent users or workers. In these environments, repeatedly opening and closing database connections becomes inefficient and can limit throughput.
# MAGIC
# MAGIC To solve this problem, we turn to connection pools. In the remainder of the section, we highlight a pattern to employ connection pools to connect to Lakebase.
# MAGIC
# MAGIC <div style="display:block; margin:18px auto; max-width:760px; width:100%; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; color:#1B3139;">
# MAGIC   <svg viewBox="0 0 760 380" width="100%" style="display:block;">
# MAGIC     <defs>
# MAGIC       <marker id="pool-arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
# MAGIC         <path d="M0,0 L0,6 L8,3 z" fill="#5B6B73"></path>
# MAGIC       </marker>
# MAGIC       <filter id="pool-shadow" x="-20%" y="-20%" width="140%" height="140%">
# MAGIC         <feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#1B3139" flood-opacity="0.08"></feDropShadow>
# MAGIC       </filter>
# MAGIC     </defs>
# MAGIC     <!-- Connection pool container -->
# MAGIC     <rect x="260" y="30" width="210" height="320" rx="16"
# MAGIC           fill="#F9F7F4" stroke="#5B6B73" stroke-width="1.6" filter="url(#pool-shadow)"></rect>
# MAGIC     <text x="365" y="58" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Connection pool</text>
# MAGIC     <!-- Connections (evenly spaced) -->
# MAGIC     <!-- centers: ~110, 190, 270 -->
# MAGIC     <rect x="286" y="80" width="158" height="60" rx="10"
# MAGIC           fill="#FFFFFF" stroke="#5B6B73" stroke-width="1.5"></rect>
# MAGIC     <text x="365" y="115" text-anchor="middle" font-size="16" font-weight="700">Connection</text>
# MAGIC     <rect x="286" y="160" width="158" height="60" rx="10"
# MAGIC           fill="#FFFFFF" stroke="#5B6B73" stroke-width="1.5"></rect>
# MAGIC     <text x="365" y="195" text-anchor="middle" font-size="16" font-weight="700">Connection</text>
# MAGIC     <rect x="286" y="240" width="158" height="60" rx="10"
# MAGIC           fill="#FFFFFF" stroke="#5B6B73" stroke-width="1.5" opacity="0.65"></rect>
# MAGIC     <text x="365" y="275" text-anchor="middle" font-size="15" fill="#5B6B73">Connection</text>
# MAGIC     <!-- Clients aligned to connections -->
# MAGIC     <rect x="36" y="80" width="110" height="60" rx="10"
# MAGIC           fill="#FF5F46" stroke="#98102A" filter="url(#pool-shadow)"></rect>
# MAGIC     <text x="91" y="115" text-anchor="middle" font-size="16" font-weight="800" fill="#FFFFFF">Client</text>
# MAGIC     <rect x="36" y="160" width="110" height="60" rx="10"
# MAGIC           fill="#FF5F46" stroke="#98102A" filter="url(#pool-shadow)"></rect>
# MAGIC     <text x="91" y="195" text-anchor="middle" font-size="16" font-weight="800" fill="#FFFFFF">Client</text>
# MAGIC     <!-- Database -->
# MAGIC     <rect x="610" y="130" width="120" height="100" rx="12"
# MAGIC           fill="#1F77B4" stroke="#1B3139" filter="url(#pool-shadow)"></rect>
# MAGIC     <text x="670" y="170" text-anchor="middle" font-size="16" font-weight="800" fill="#FFFFFF">Postgres</text>
# MAGIC     <text x="670" y="195" text-anchor="middle" font-size="16" font-weight="800" fill="#FFFFFF">Database</text>
# MAGIC     <!-- Checkout arrows (aligned cleanly) -->
# MAGIC     <line x1="146" y1="110" x2="286" y2="110"
# MAGIC           stroke="#5B6B73" stroke-width="2.2" marker-end="url(#pool-arrow)"></line>
# MAGIC     <rect x="165" y="94" width="76" height="28" rx="6" fill="#EEEDE9"></rect>
# MAGIC     <text x="203" y="113" text-anchor="middle" font-size="15">checkout</text>
# MAGIC     <line x1="146" y1="190" x2="286" y2="190"
# MAGIC           stroke="#5B6B73" stroke-width="2.2" marker-end="url(#pool-arrow)"></line>
# MAGIC     <rect x="165" y="174" width="76" height="28" rx="6" fill="#EEEDE9"></rect>
# MAGIC     <text x="203" y="193" text-anchor="middle" font-size="15">checkout</text>
# MAGIC     <!-- Transactions (still slightly curved for life) -->
# MAGIC     <path d="M444 110 C520 110, 550 130, 610 155"
# MAGIC           fill="none" stroke="#5B6B73" stroke-width="2.2" marker-end="url(#pool-arrow)"></path>
# MAGIC     <rect x="500" y="95" width="100" height="28" rx="6" fill="#EEEDE9"></rect>
# MAGIC     <text x="550" y="114" text-anchor="middle" font-size="15">transaction</text>
# MAGIC     <path d="M444 190 C520 190, 550 180, 610 175"
# MAGIC           fill="none" stroke="#5B6B73" stroke-width="2.2" marker-end="url(#pool-arrow)"></path>
# MAGIC     <rect x="500" y="170" width="100" height="28" rx="6" fill="#EEEDE9"></rect>
# MAGIC     <text x="550" y="189" text-anchor="middle" font-size="15">transaction</text>
# MAGIC   </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Establishing a Connection Pool
# MAGIC
# MAGIC A **connection pool** is a managed collection of open database connections that your code checks out, uses for a query or transaction, and then returns to the pool instead of opening and closing new connections each time. This avoids the overhead of repeatedly establishing Postgres connections, which is especially important for Databricks apps that serve many short requests or run concurrently across multiple workers.
# MAGIC
# MAGIC Pools are also a natural place to integrate credential rotation. In the example below, the call to `postgres.generate_database_credential()` ensures that each new pooled connection receives a fresh short-lived token.
# MAGIC
# MAGIC By centralizing connection lifecycle and token handling in the pool, your app code stays simple, you minimize latency per request, and you naturally scale to higher QPS without running into connection‑setup bottlenecks.
# MAGIC
# MAGIC <div style="display:block; margin:18px auto; max-width:980px; width:100%; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; color:#1B3139;">
# MAGIC   <svg viewBox="0 0 980 470" width="100%" style="display:block;">
# MAGIC     <defs>
# MAGIC       <marker id="seq-arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
# MAGIC         <path d="M0,0 L0,6 L8,3 z" fill="#5B6B73"></path>
# MAGIC       </marker>
# MAGIC       <filter id="seq-shadow" x="-20%" y="-20%" width="140%" height="140%">
# MAGIC         <feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#1B3139" flood-opacity="0.07"></feDropShadow>
# MAGIC       </filter>
# MAGIC     </defs>
# MAGIC     <!-- Participant cards -->
# MAGIC     <rect x="24" y="20" width="150" height="66" rx="10" fill="#F9F7F4" stroke="#DCE0E2" filter="url(#seq-shadow)"></rect>
# MAGIC     <text x="99" y="58" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Client</text>
# MAGIC     <rect x="265" y="20" width="180" height="66" rx="10" fill="#FFF4F2" stroke="#FFD2CB" filter="url(#seq-shadow)"></rect>
# MAGIC     <text x="355" y="49" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Connection Pool</text>
# MAGIC     <text x="355" y="68" text-anchor="middle" font-size="14" fill="#5B6B73">(psycopg3)</text>
# MAGIC     <rect x="585" y="20" width="150" height="66" rx="10" fill="#EEF6FF" stroke="#CFE4FF" filter="url(#seq-shadow)"></rect>
# MAGIC     <text x="660" y="58" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Databricks</text>
# MAGIC     <rect x="815" y="20" width="150" height="66" rx="10" fill="#F4FBF7" stroke="#CFE9D8" filter="url(#seq-shadow)"></rect>
# MAGIC     <text x="890" y="49" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Postgres</text>
# MAGIC     <text x="890" y="68" text-anchor="middle" font-size="14" fill="#5B6B73">(Lakebase)</text>
# MAGIC     <!-- Bottom participant cards -->
# MAGIC     <rect x="24" y="382" width="150" height="66" rx="10" fill="#F9F7F4" stroke="#DCE0E2"></rect>
# MAGIC     <text x="99" y="421" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Client</text>
# MAGIC     <rect x="265" y="382" width="180" height="66" rx="10" fill="#FFF4F2" stroke="#FFD2CB"></rect>
# MAGIC     <text x="355" y="411" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Connection Pool</text>
# MAGIC     <text x="355" y="430" text-anchor="middle" font-size="14" fill="#5B6B73">(psycopg3)</text>
# MAGIC     <rect x="585" y="382" width="150" height="66" rx="10" fill="#EEF6FF" stroke="#CFE4FF"></rect>
# MAGIC     <text x="660" y="421" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Databricks</text>
# MAGIC     <rect x="815" y="382" width="150" height="66" rx="10" fill="#F4FBF7" stroke="#CFE9D8"></rect>
# MAGIC     <text x="890" y="411" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Postgres</text>
# MAGIC     <text x="890" y="430" text-anchor="middle" font-size="14" fill="#5B6B73">(Lakebase)</text>
# MAGIC     <!-- Lifelines -->
# MAGIC     <line x1="99" y1="86" x2="99" y2="382" stroke="#D0D6DA" stroke-width="3"></line>
# MAGIC     <line x1="355" y1="86" x2="355" y2="382" stroke="#FFD2CB" stroke-width="2"></line>
# MAGIC     <line x1="660" y1="86" x2="660" y2="382" stroke="#CFE4FF" stroke-width="2"></line>
# MAGIC     <line x1="890" y1="86" x2="890" y2="382" stroke="#CFE9D8" stroke-width="2"></line>
# MAGIC     <!-- Activations -->
# MAGIC     <line x1="355" y1="130" x2="355" y2="178" stroke="#FFD2CB" stroke-width="6"></line>
# MAGIC     <line x1="355" y1="226" x2="355" y2="274" stroke="#FFD2CB" stroke-width="6"></line>
# MAGIC     <line x1="355" y1="322" x2="355" y2="358" stroke="#FFD2CB" stroke-width="6"></line>
# MAGIC     <line x1="660" y1="178" x2="660" y2="226" stroke="#CFE4FF" stroke-width="6"></line>
# MAGIC     <line x1="890" y1="274" x2="890" y2="322" stroke="#CFE9D8" stroke-width="6"></line>
# MAGIC     <!-- Messages -->
# MAGIC     <!-- Client -> Pool -->
# MAGIC     <line x1="99" y1="130" x2="355" y2="130" stroke="#5B6B73" stroke-width="2.4" marker-end="url(#seq-arrow)"></line>
# MAGIC     <text x="225" y="116" text-anchor="middle" font-size="15" fill="#1B3139">Request connection</text>
# MAGIC     <!-- Pool -> Databricks -->
# MAGIC     <line x1="355" y1="178" x2="660" y2="178" stroke="#5B6B73" stroke-width="2.4" marker-end="url(#seq-arrow)"></line>
# MAGIC     <text x="507" y="163" text-anchor="middle" font-size="15" fill="#1B3139">generate_database_credential()</text>
# MAGIC     <!-- Databricks -> Pool token -->
# MAGIC     <line x1="660" y1="226" x2="355" y2="226" stroke="#5B6B73" stroke-width="2.2" stroke-dasharray="4 4" marker-end="url(#seq-arrow)"></line>
# MAGIC     <text x="508" y="212" text-anchor="middle" font-size="15" fill="#1B3139">Token</text>
# MAGIC     <!-- Pool -> Postgres -->
# MAGIC     <line x1="355" y1="274" x2="890" y2="274" stroke="#5B6B73" stroke-width="2.4" marker-end="url(#seq-arrow)"></line>
# MAGIC     <text x="620" y="259" text-anchor="middle" font-size="15" fill="#1B3139">connect(password=token)</text>
# MAGIC     <!-- Postgres -> Pool -->
# MAGIC     <line x1="890" y1="322" x2="355" y2="322" stroke="#5B6B73" stroke-width="2.2" stroke-dasharray="4 4" marker-end="url(#seq-arrow)"></line>
# MAGIC     <text x="620" y="307" text-anchor="middle" font-size="15" fill="#1B3139">Connection established</text>
# MAGIC     <!-- Pool -> Client -->
# MAGIC     <line x1="355" y1="358" x2="99" y2="358" stroke="#5B6B73" stroke-width="2.2" stroke-dasharray="4 4" marker-end="url(#seq-arrow)"></line>
# MAGIC     <text x="225" y="344" text-anchor="middle" font-size="15" fill="#1B3139">New pooled connection</text>
# MAGIC   </svg>
# MAGIC </div>
# MAGIC
# MAGIC Run the following cell, which defines a `Connection` subclass that injects authentication token management. This pattern is considered best practice when using **psycopg3** to connect to Lakebase.

# COMMAND ----------

# DBTITLE 1,Cell 17
import os

os.environ["PSYCOPG_IMPL"] = "python"

import psycopg
from psycopg_pool import ConnectionPool

class LakebaseConnection(psycopg.Connection):

    @classmethod
    def connect(
        cls,
        conninfo: str="",
        *,
        workspace=None,
        endpoint=None,
        **kwargs
    ):
        # fetch new Lakebase token
        cred = workspace.postgres.generate_database_credential(endpoint=endpoint)

        # inject token into the connect() call
        kwargs["password"] = cred.token
        conn = super().connect(conninfo, **kwargs)

        return conn

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Instantiating the Pool
# MAGIC
# MAGIC Next, we instantiate a pool using the connection class above, and a connection string we construct from:
# MAGIC - the database name (in this example, we'll assume `databricks_postgres`)
# MAGIC - the username (in this example, we'll assume the user running this notebook)
# MAGIC - the host, which we obtain from the project that was set up at the beginning of this notebook, using the `postgres.get_endpoint()` API

# COMMAND ----------

# DBTITLE 1,Cell 19
# assume by default
database = 'databricks_postgres'

# use currently authenticated user
username = workspace.current_user.me().user_name

# obtain host from Lakebase compute (coming from setup cell above)
host = workspace.postgres.get_endpoint(name=my_lakebase_compute).status.hosts.host

# initialize connection pool using the Lakebase connection handler
pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} port=5432 sslmode=require",
    connection_class=LakebaseConnection,
    kwargs= {
        # pass workspace connection and Lakebase compute to the connect() method
        "workspace": workspace,
        "endpoint": my_lakebase_compute
    },
    min_size=1,
    max_size=10,
    open=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Executing SQL
# MAGIC
# MAGIC With a connection pool ready, we can now illustrate how to grab a connection and run SQL statements that:
# MAGIC 1. Create a schema
# MAGIC 1. Create a table in that schema

# COMMAND ----------

with pool.connection() as conn:
    with conn.cursor() as cur:
        # create a schema named "test"
        cur.execute("CREATE SCHEMA IF NOT EXISTS test")

        # create a small table named "hello"
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test.hello AS
            SELECT
                1 AS id,
                'Hello Lakebase' AS message,
                current_user AS created_by,
                current_timestamp AS created_at
        """)

    conn.commit()

# COMMAND ----------

# MAGIC %md
# MAGIC ### D5. Validating Results
# MAGIC
# MAGIC Let's explore how to validate the current state of the database using SQL and the Databricks UI.

# COMMAND ----------

# MAGIC %md
# MAGIC #### D5.1 Executing a Read Operation in SQL
# MAGIC
# MAGIC The code below validates the work we did above by displaying the contents of the table. It also illustrates the pattern for table reading using the PostgreSQL driver:

# COMMAND ----------

with pool.connection() as conn:
    with conn.cursor() as cur:
        # read contents of table "hello"
        cur.execute("SELECT * FROM test.hello")
        print(cur.fetchall())

# COMMAND ----------

# MAGIC %md
# MAGIC #### D5.2 Inspecting the Database in the UI
# MAGIC
# MAGIC We can also inspect tables in the UI. In the Lakebase project page (which we opened in the first section of this notebook):
# MAGIC 1. Select **Tables** in the left sidebar
# MAGIC 1. Use the schema dropdown to select `test`
# MAGIC 1. Select the **hello** table and note the contents

# COMMAND ----------

# DBTITLE 1,Cell 26
# MAGIC %md-sandbox
# MAGIC ### D6. Recap
# MAGIC
# MAGIC In this section, we connected to Lakebase using the **psycopg3** driver. This driver provides modern capabilities for doing complex, high-bandwidth interactions with Postgres. Furthermore, the patterns we saw align with best practices for maintaining fresh tokens in a connection pool to ensure performant behavior.
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 8px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Important Note
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC   These connections worked out-of-the-box because we connected using the same identity that was used to create the project; so we automatically had a role enabled that allowed us to connect to and create schemas.
# MAGIC   
# MAGIC   When accessing from a Databricks app, we will have to perform some additional configuration to allow the app service principal to access the database. We will cover this in a separate lab.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Cell 27
# MAGIC %md
# MAGIC ## E. Cleanup
# MAGIC
# MAGIC Please run the following cell to clean up resources before proceeding.

# COMMAND ----------

cleanup()

# COMMAND ----------

# DBTITLE 1,Cell 29
# MAGIC %md
# MAGIC ## F. Conclusion
# MAGIC In this demo, we:
# MAGIC - **Covered what Lakebase provides**: elastic compute with scale-to-zero, organized as projects with branches and read replicas.
# MAGIC - **Connected using a Python PostgreSQL driver**: we used `psycopg3` with connection pools to ensure high performance with fresh, short-lived tokens that are automatically rotated, following the recommended pattern for production apps.
# MAGIC - **Performed and verified transactions**: from Python, we executed statements to:
# MAGIC    - create a schema
# MAGIC    - create a table
# MAGIC    - insert data into a table
# MAGIC    - read data from a table

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>