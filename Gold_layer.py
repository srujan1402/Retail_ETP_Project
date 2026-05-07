# Databricks notebook source
# DBTITLE 1,Cell 1
# =========================================================
# GOLD LAYER TABLE CREATION
# DIM + FACT TABLES
# =========================================================

from pyspark.sql.functions import *

# =========================================================
# USE CATALOG + SCHEMA
# =========================================================

spark.sql("USE CATALOG retail_data")
spark.sql("USE SCHEMA gold")

# =========================================================
# READ PROCESSED LAYER DATA FROM CATALOG TABLES
# =========================================================

customers_df = spark.table("retail_data.processed.customers_clean")
products_df = spark.table("retail_data.processed.products_clean")
stores_df = spark.table("retail_data.processed.stores_clean")
sales_df = spark.table("retail_data.processed.sales_clean")

# =========================================================
# CREATE DIM CUSTOMER
# =========================================================

dim_customer = customers_df \
    .select(
        col("CustomerID").cast("string").alias("CustomerID"),
        "CustomerName",
        "Email",
        "City",
        "Address"
    ) \
    .dropDuplicates(["CustomerID"]) \
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    ) \
    .withColumn(
        "StartDate",
        current_timestamp()
    ) \
    .withColumn(
        "EndDate",
        lit(None).cast("timestamp")
    ) \
    .withColumn(
        "IsActive",
        lit(1)
    )

# =========================================================
# WRITE DIM CUSTOMER
# =========================================================

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option(
        "path",
        "s3://retail-datawarehouse-3030/gold/dim_customer/"
    ) \
    .saveAsTable(
        "retail_data.gold.dim_customer"
    )

print("DIM CUSTOMER CREATED")

# =========================================================
# CREATE DIM PRODUCT
# =========================================================

dim_product = products_df \
    .withColumn("ProductID", col("ProductID").cast("string")) \
    .dropDuplicates(["ProductID"]) \
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )

# =========================================================
# WRITE DIM PRODUCT
# =========================================================

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option(
        "path",
        "s3://retail-datawarehouse-3030/gold/dim_product/"
    ) \
    .saveAsTable(
        "retail_data.gold.dim_product"
    )

print("DIM PRODUCT CREATED")

# =========================================================
# CREATE DIM STORE
# =========================================================

dim_store = stores_df \
    .withColumn("StoreID", col("StoreID").cast("string")) \
    .dropDuplicates(["StoreID"]) \
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )

# =========================================================
# WRITE DIM STORE
# =========================================================

dim_store.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option(
        "path",
        "s3://retail-datawarehouse-3030/gold/dim_store/"
    ) \
    .saveAsTable(
        "retail_data.gold.dim_store"
    )

print("DIM STORE CREATED")

# =========================================================
# CREATE FACT SALES
# JOIN WITH PRODUCTS TO GET UNITPRICE AND CALCULATE SALESAMOUNT
# =========================================================

fact_sales = sales_df \
    .join(
        products_df.select(
            col("ProductID").cast("string").alias("ProductID"),
            "UnitPrice"
        ),
        on="ProductID",
        how="left"
    ) \
    .select(
        col("TransactionID").cast("string").alias("TransactionID"),
        col("CustomerID").cast("string").alias("CustomerID"),
        col("ProductID").cast("string").alias("ProductID"),
        col("StoreID").cast("string").alias("StoreID"),
        "Quantity",
        "UnitPrice",
        (col("Quantity") * col("UnitPrice")).alias("SalesAmount")
    ) \
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )

# =========================================================
# WRITE FACT SALES
# =========================================================

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option(
        "path",
        "s3://retail-datawarehouse-3030/gold/fact_sales/"
    ) \
    .saveAsTable(
        "retail_data.gold.fact_sales"
    )

print("FACT SALES CREATED")

# =========================================================
# VALIDATION
# =========================================================

print("\n====================================")
print("GOLD LAYER TABLES CREATED")
print("====================================")

spark.sql("""
SHOW TABLES IN retail_data.gold
""").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog retail_data;
# MAGIC use schema gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail_gold;
# MAGIC
# MAGIC USE retail_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE dim_customer (
# MAGIC     CustomerSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     CustomerID STRING,
# MAGIC     CustomerName STRING,
# MAGIC     Email STRING,
# MAGIC     City STRING,
# MAGIC     Address STRING,
# MAGIC     processed_timestamp TIMESTAMP,
# MAGIC     StartDate TIMESTAMP,
# MAGIC     EndDate TIMESTAMP,
# MAGIC     IsActive INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import current_timestamp, lit, col

# ---------------------------------------------------------
# READ PROCESSED CUSTOMER DATA FROM CATALOG TABLE
# ---------------------------------------------------------

customers_df = spark.table("retail_data.processed.customers_clean")

# ---------------------------------------------------------
# PREPARE DIM CUSTOMER DATA WITH TYPE CAST
# ---------------------------------------------------------

dim_customer_df = customers_df \
    .withColumn("CustomerID", col("CustomerID").cast("string")) \
    .withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", lit(None).cast("timestamp")) \
    .withColumn("IsActive", lit(1))

# ---------------------------------------------------------
# SELECT ONLY TABLE COLUMNS
# DO NOT INCLUDE CustomerSK
# ---------------------------------------------------------

dim_customer_df = dim_customer_df.select(
    "CustomerID",
    "CustomerName",
    "Email",
    "City",
    "Address",
    "StartDate",
    "EndDate",
    "IsActive"
)

# ---------------------------------------------------------
# INSERT INTO DELTA TABLE
# ---------------------------------------------------------

dim_customer_df.write.mode("append").saveAsTable(
    "retail_gold.dim_customer"
)

# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("DimCustomer Count:", dim_customer_df.count())

display(dim_customer_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE dim_product (
# MAGIC     ProductSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     ProductID STRING,
# MAGIC     ProductName STRING,
# MAGIC     Category STRING,
# MAGIC     Brand STRING,
# MAGIC     UnitPrice DOUBLE,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.functions import lit, col

# Read from catalog table
products_df = spark.table("retail_data.processed.products_clean")

# Add missing Brand column and cast ProductID to string, UnitPrice to double
dim_product_df = products_df.select(
    col("ProductID").cast("string").alias("ProductID"),
    "ProductName",
    "Category",
    lit(None).cast("string").alias("Brand"),
    col("UnitPrice").cast("double").alias("UnitPrice")
)

dim_product_df.write.mode("append").saveAsTable(
    "retail_gold.dim_product"
)

print("DimProduct Count:", dim_product_df.count())

display(dim_product_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_store (
# MAGIC     StoreSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     StoreID STRING,
# MAGIC     StoreName STRING,
# MAGIC     City STRING,
# MAGIC     State STRING,
# MAGIC     Region STRING,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.functions import lit, col

# Read from catalog table
stores_df = spark.table("retail_data.processed.stores_clean")

# Select and add missing columns, cast StoreID to string
dim_store_df = stores_df.select(
    col("StoreID").cast("string").alias("StoreID"),
    "StoreName",
    lit(None).cast("string").alias("City"),
    lit(None).cast("string").alias("State"),
    "Region"
)

dim_store_df.write.mode("append").saveAsTable(
    "retail_gold.dim_store"
)

print("DimStore Count:", dim_store_df.count())

display(dim_store_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE fact_sales (
# MAGIC     SalesSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     TransactionID STRING,
# MAGIC     CustomerID STRING,
# MAGIC     ProductID STRING,
# MAGIC     StoreID STRING,
# MAGIC     Quantity INT,
# MAGIC     UnitPrice DOUBLE,
# MAGIC     SalesAmount DOUBLE,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql.functions import col

# Read from catalog table
sales_df = spark.table("retail_data.processed.sales_clean")

# Join with dim_product to get UnitPrice and calculate SalesAmount
# Cast ID columns to string to match table schema
fact_sales_df = sales_df \
    .join(
        spark.table("retail_gold.dim_product").select(
            col("ProductID").alias("ProductID"),
            col("UnitPrice").alias("UnitPrice")
        ),
        on="ProductID",
        how="left"
    ) \
    .select(
        col("TransactionID").cast("string").alias("TransactionID"),
        col("CustomerID").cast("string").alias("CustomerID"),
        col("ProductID").cast("string").alias("ProductID"),
        col("StoreID").cast("string").alias("StoreID"),
        "Quantity",
        "UnitPrice",
        (col("Quantity") * col("UnitPrice")).alias("SalesAmount")
    )

fact_sales_df.write.mode("append").saveAsTable(
    "retail_gold.fact_sales"
)

print("FactSales Count:", fact_sales_df.count())

display(fact_sales_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dim_customer;
# MAGIC SELECT COUNT(*) FROM dim_product;
# MAGIC SELECT COUNT(*) FROM dim_store;
# MAGIC SELECT COUNT(*) FROM fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC duplicate validation:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, COUNT(*)
# MAGIC FROM dim_customer
# MAGIC GROUP BY CustomerID
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC sales analysis example:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     StoreID,
# MAGIC     SUM(SalesAmount) AS TotalSales
# MAGIC FROM fact_sales
# MAGIC GROUP BY StoreID
# MAGIC ORDER BY TotalSales DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC SCD:
# MAGIC

# COMMAND ----------

# Reading Excel files directly requires additional libraries not available on serverless compute.
# The Excel file needs to be processed into Parquet format first.
# Use the processed Parquet data from the ETL pipeline instead:

customers_processed_incremental = spark.read.parquet(
    "s3://retail-datawarehouse-3030/processed/customers/"
)

display(customers_processed_incremental)

# COMMAND ----------

dim_customer = spark.table(
    "retail_gold.dim_customer"
)

display(dim_customer)

# COMMAND ----------

from pyspark.sql.functions import *

changed_customers = customers_processed_incremental.alias("new") \
    .join(
        dim_customer.alias("old"),
        col("new.CustomerID") == col("old.CustomerID"),
        "inner"
    ) \
    .filter(
        (col("new.City") != col("old.City")) |
        (col("new.Address") != col("old.Address")) |
        (col("new.Email") != col("old.Email"))
    ) \
    .select("old.CustomerID")

display(changed_customers)

# COMMAND ----------

changed_customers.createOrReplaceTempView(
    "changed_customers"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE retail_gold.dim_customer
# MAGIC SET
# MAGIC     EndDate = current_timestamp(),
# MAGIC     IsActive = 0
# MAGIC WHERE CustomerID IN (
# MAGIC     SELECT CustomerID
# MAGIC     FROM changed_customers
# MAGIC )
# MAGIC AND IsActive = 1;

# COMMAND ----------

from pyspark.sql.functions import *

new_customer_versions = customers_processed_incremental \
    .withColumn(
        "StartDate",
        current_timestamp()
    ) \
    .withColumn(
        "EndDate",
        lit(None).cast("timestamp")
    ) \
    .withColumn(
        "IsActive",
        lit(1)
    )

new_customer_versions.select(
    "CustomerID",
    "CustomerName",
    "Email",
    "City",
    "Address",
    "processed_timestamp",
    "StartDate",
    "EndDate",
    "IsActive"
).write.mode("append").saveAsTable(
    "retail_gold.dim_customer"
)

print("SCD2 Load Completed")

display(new_customer_versions)

# COMMAND ----------

# MAGIC %md
# MAGIC validating:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     StartDate,
# MAGIC     EndDate,
# MAGIC     IsActive
# MAGIC FROM retail_gold.dim_customer
# MAGIC ORDER BY CustomerID, StartDate;

# COMMAND ----------

# =========================================================
# SCD TYPE 2 - DIM CUSTOMER
# PREVENT DUPLICATE HISTORICAL INSERTS
# =========================================================

from pyspark.sql.functions import *

# =========================================================
# READ NEW CUSTOMER FILE
# =========================================================

new_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(
        "s3://retail-datawarehouse-3030/sftp/New_customers_src_20042026100105.csv"
    )

# =========================================================
# CLEAN + STANDARDIZE
# =========================================================

new_customers = new_customers \
    .withColumn(
        "CustomerID",
        col("CustomerID").cast("string")
    ) \
    .dropDuplicates(["CustomerID"]) \
    .withColumn(
        "CustomerName",
        initcap(trim(col("CustomerName")))
    ) \
    .withColumn(
        "Email",
        lower(trim(col("Email")))
    ) \
    .withColumn(
        "City",
        upper(trim(col("City")))
    ) \
    .withColumn(
        "Address",
        trim(col("Address"))
    ) \
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )

# =========================================================
# READ EXISTING DIM CUSTOMER
# =========================================================

dim_customer = spark.table(
    "retail_data.gold.dim_customer"
)

# =========================================================
# FILTER ONLY ACTIVE RECORDS
# =========================================================

active_customers = dim_customer.filter(
    col("IsActive") == 1
)

# =========================================================
# IDENTIFY CHANGED CUSTOMERS
# ONLY COMPARE WITH ACTIVE RECORDS
# =========================================================

changed_customers = new_customers.alias("new") \
    .join(
        active_customers.alias("old"),
        col("new.CustomerID") == col("old.CustomerID"),
        "inner"
    ) \
    .filter(
        (col("new.City") != col("old.City")) |
        (col("new.Address") != col("old.Address")) |
        (col("new.Email") != col("old.Email"))
    ) \
    .select(
        col("new.CustomerID"),
        col("new.CustomerName"),
        col("new.Email"),
        col("new.City"),
        col("new.Address"),
        col("new.processed_timestamp")
    ) \
    .distinct()

# =========================================================
# EXPIRE OLD ACTIVE RECORDS
# =========================================================

changed_ids = [row["CustomerID"]
               for row in changed_customers.select(
                   "CustomerID"
               ).collect()]

if len(changed_ids) > 0:

    ids = ",".join([f"'{x}'" for x in changed_ids])

    spark.sql(f"""

    UPDATE retail_data.gold.dim_customer
    SET
        EndDate = current_timestamp(),
        IsActive = 0
    WHERE CustomerID IN ({ids})
    AND IsActive = 1

    """)

# =========================================================
# CREATE NEW ACTIVE RECORDS
# =========================================================

new_versions = changed_customers \
    .withColumn(
        "StartDate",
        current_timestamp()
    ) \
    .withColumn(
        "EndDate",
        lit(None).cast("timestamp")
    ) \
    .withColumn(
        "IsActive",
        lit(1)
    )

# =========================================================
# APPEND ONLY NEW CHANGES
# =========================================================

if new_versions.count() > 0:

    new_versions.select(
        "CustomerID",
        "CustomerName",
        "Email",
        "City",
        "Address",
        "processed_timestamp",
        "StartDate",
        "EndDate",
        "IsActive"
    ).write \
    .mode("append") \
    .format("delta") \
    .saveAsTable(
        "retail_data.gold.dim_customer"
    )

# =========================================================
# VALIDATION
# =========================================================

print("===================================")
print("SCD TYPE 2 COMPLETED")
print("===================================")

print("Changed Customers:",
      new_versions.count())

display(
    spark.sql("""

    SELECT
        CustomerID,
        CustomerName,
        City,
        StartDate,
        EndDate,
        IsActive
    FROM retail_data.gold.dim_customer
    ORDER BY CustomerID, StartDate

    """)
)