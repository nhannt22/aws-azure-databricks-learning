-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS nhan_databricks.f1_processed
MANAGED LOCATION "abfss://processed@formula1dlnt.dfs.core.windows.net/"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS nhan_databricks.f1_presentation 
MANAGED LOCATION "abfss://presentation@formula1dlnt.dfs.core.windows.net/"

-- COMMAND ----------


