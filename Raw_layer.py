# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC USE CATALOG retail_data;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC -- Drop all existing tables first to avoid path conflicts
# MAGIC DROP TABLE IF EXISTS customers_raw;
# MAGIC DROP TABLE IF EXISTS products_raw;
# MAGIC DROP TABLE IF EXISTS stores_raw;
# MAGIC DROP TABLE IF EXISTS sales_raw;
# MAGIC
# MAGIC -- Create customers_raw with explicit schema (corrected file path)
# MAGIC CREATE TABLE customers_raw (
# MAGIC   CustomerID INT,
# MAGIC   CustomerName STRING,
# MAGIC   Email STRING,
# MAGIC   City STRING,
# MAGIC   Address STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 's3://retail-datawarehouse-3030/sftp/New_customers_src_20042026100105.csv',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC -- Create products_raw with explicit schema
# MAGIC CREATE TABLE products_raw (
# MAGIC   ProductID INT,
# MAGIC   ProductName STRING,
# MAGIC   Category STRING,
# MAGIC   UnitPrice INT
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 's3://retail-datawarehouse-3030/sftp/products_src_20042026100105.csv',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC -- Create stores_raw with explicit schema
# MAGIC CREATE TABLE stores_raw (
# MAGIC   StoreID INT,
# MAGIC   StoreName STRING,
# MAGIC   Region STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 's3://retail-datawarehouse-3030/sftp/stores_src_20042026100107.csv',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC -- Create sales_raw with explicit schema
# MAGIC CREATE TABLE sales_raw (
# MAGIC   TransactionID INT,
# MAGIC   CustomerID INT,
# MAGIC   ProductID INT,
# MAGIC   StoreID INT,
# MAGIC   Quantity INT,
# MAGIC   TxnDate DATE
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path 's3://retail-datawarehouse-3030/sftp/sales_transactions_src_20042026100107.csv',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC -- Verify the data loaded correctly
# MAGIC SELECT * FROM customers_raw LIMIT 10;
# MAGIC SELECT * FROM products_raw LIMIT 10;
# MAGIC SELECT * FROM stores_raw LIMIT 10;
# MAGIC SELECT * FROM sales_raw LIMIT 10;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# ---------------------------------------------------------
# READ NEW SALES FILE FROM SFTP/LANDING
# ---------------------------------------------------------

sales_incremental = spark.read.format("csv") \
    .option("header", "true") \
    .load(
        "s3://retail-datawarehouse-3030/sftp/sales_transactions_src_20042026100107.csv"
    )

# ---------------------------------------------------------
# ADD METADATA
# ---------------------------------------------------------

sales_raw_incremental = sales_incremental \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

# ---------------------------------------------------------
# WRITE TO RAW LAYER
# ---------------------------------------------------------

sales_raw_incremental.write.mode("append").parquet(
    "s3://retail-datawarehouse-3030/raw/sales/"
)

# ---------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------

print("Incremental Raw Sales Count:",
      sales_raw_incremental.count())

display(sales_raw_incremental)