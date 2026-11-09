# Databricks notebook source
# MAGIC %md
# MAGIC ![databricks_academy_logo.png](../Includes/images/databricks_academy_logo.png "databricks_academy_logo.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Engineer Responsibilities
# MAGIC First, let’s take a quick look at the key responsibilities of a data engineer, who is essential in managing and optimizing an organization's data.
# MAGIC
# MAGIC ![get-started-de-de responsibility.png](../Includes/images/get-started-de-de responsibility.png "get-started-de-de responsibility.png")
# MAGIC
# MAGIC One of the primary tasks of a data engineer is to **transform raw data** into a format that is clean and reliable. This involves a variety of processes including:
# MAGIC - data extraction from diverse sources like databases, data lakes, flat files and more 
# MAGIC - data cleansing to remove errors and inconsistencies
# MAGIC - data transformation to convert it into a structured and usable format for consumers
# MAGIC
# MAGIC Data engineers are also responsible for **ensuring the quality and integrity** of an organization’s data. This means implementing processes to monitor and maintain data accuracy, consistency, and reliability. This keeps the data trustworthy and dependable.
# MAGIC
# MAGIC Another critical role of a data engineer is **designing, building, and maintaining data pipelines**. 
# MAGIC
# MAGIC These pipelines are the pathways through which data flows from various sources to our storage systems and analytical tools. Data engineers create, optimize and automate these pipelines to ensure efficient data movement, proper data integration, and seamless access. 
# MAGIC
# MAGIC Now that you know what a data engineer does, let’s see how these responsibilities fit into the bigger picture of data architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Engineering Architecture
# MAGIC Let’s review a high-level step-by-step overview of a traditional data engineering architecture.
# MAGIC
# MAGIC ![get-started-de-de architecture.png](../Includes/images/get-started-de-de architecture.png "get-started-de-de architecture.png")
# MAGIC
# MAGIC **Data sources**: First, you start by understanding your data sources, which can include databases, cloud storage, network logs, a variety of files types and more.
# MAGIC
# MAGIC **Data ingestion**: Next, data is ingested into the enterprise storage systems. Typically, this involves using data warehouses and data lakes to manage the large volumes of data.
# MAGIC
# MAGIC **Data processing**: Once ingested, data must be processed and cleaned using various tools to ensure accuracy and completeness for business use.
# MAGIC
# MAGIC **Making data available**: Processed data is then stored and provided to consumers like business analysts, data scientists, and machine learning teams for insights, models, and applications.
# MAGIC
# MAGIC **Data orchestration**: As a data engineer, it’s crucial to orchestrate workloads effectively by building, automating and maintaining data pipelines for reusability.
# MAGIC
# MAGIC **Data governance**: Lastly, doing this while managing data governance, access and security to maintain the integrity and protection of the data.
# MAGIC
# MAGIC While this architecture provides a strong foundation, it also introduces several challenges for organizations. Why? Well, organizations typically use a variety of technology stacks to manage the data engineering process, which introduces several challenges.
# MAGIC
# MAGIC Let’s explore some of the most common challenges data engineers face in this environment.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Challenges Faced by Data Engineers
# MAGIC
# MAGIC - Complex data ingestion methods. Managing streaming ingestion can be difficult, requiring always-running streaming platforms or manual tracking of files that have not been ingested yet, along with managing the time-consuming and error-prone tasks.
# MAGIC - Supporting key data engineering principles, such as agile development methods, CI/CD (continuous integration and continuous deployment), version control, and isolated development and production environments.
# MAGIC - Using third-party orchestration tools, which can increase operational overhead and system complexity, typically requiring an advanced skill set. As well as platform inconsistencies. Differences between various data warehouses and data lake providers can create challenges in integrating multiple products due to differing limitations, workloads, languages, and governance models.
# MAGIC
# MAGIC To address these challenges, many organizations are turning to unified platforms like Databricks.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simplify Data Engineering Using the Databricks Data Intelligence Platform
# MAGIC
# MAGIC ![get-started-de-databricks overview.png](../Includes/images/get-started-de-databricks overview.png "get-started-de-databricks overview.png")
# MAGIC
# MAGIC With the Databricks Data Intelligent Platform, organizations can simplify their data engineering architecture and do it in one place, one platform.
# MAGIC
# MAGIC - From Lakeflow Connect using batch or streaming processing into Delta Lake, enhancing data reliability and performance by centralizing storage and avoiding duplication.
# MAGIC - Using Spark and/or Photon to optimize data processing and transformation, supporting efficient storage in Delta Lake for your lakehouse.
# MAGIC - Lakeflow Spark Declarative Pipelines is a declarative ETL framework for the Databricks Data Intelligence Platform that helps data engineers build automated and reliable ETL pipelines for fresh, high quality data for your consumers. Lakeflow Spark Declarative Pipelines automatically manage task orchestration, cluster management, monitoring, data quality and error handling.
# MAGIC - Delivering clean, usable data to your consumers for data warehousing and business intelligence for DBSQL, data science and machine learning using Mosaic AI, or sharing data to other organizations using Delta Sharing.
# MAGIC - Databricks Lakeflow Jobs provides a collection of tools that allow you to schedule and orchestrate end to end data processing tasks on Databricks.
# MAGIC - Lastly, with Unity Catalog, organizations can seamlessly govern both structured and unstructured data in any format, as well as machine learning models, notebooks, dashboards and files across any cloud or platform. Providing data governance, access and security. 
# MAGIC
# MAGIC The Databricks Data Intelligence platform enables the entire data team to collaborate seamlessly on a single secure platform. From the moment data is ingested into the organization to its cleaning, analysis, and application in business decisions, the platform supports every stage of the process.
# MAGIC
# MAGIC With this unified approach, your data engineering workloads become more streamlined and collaborative, supporting every stage from ingestion to insight.
# MAGIC
# MAGIC