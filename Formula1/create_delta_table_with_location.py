# Databricks notebook source
table_uri = "abfss://demo@formula1dlnt.dfs.core.windows.net/results_managed"

sql_command = f"""
CREATE TABLE IF NOT EXISTS nhan_databricks.f1_demo.results_managed (
    cardReferenceID string,
    modifiedDate TIMESTAMP,
    createdDate TIMESTAMP,
    clientCode string,
    processorCode string
)
USING DELTA
LOCATION "{table_uri}"
TBLPROPERTIES (
    delta.enableChangeDataFeed = true, 
    spark.databricks.delta.schema.autoMerge.enabled = true
);
"""

spark.sql(sql_command)
