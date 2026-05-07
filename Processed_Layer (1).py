# Databricks notebook source
# =========================================================
# CUSTOMERS PROCESSED LAYER
# =========================================================

from pyspark.sql.functions import *

# ---------------------------------------------------------
# READ RAW CUSTOMER DATA
# ---------------------------------------------------------

customers_raw = spark.read.parquet(
    "s3://retail-datawarehouse-3030/raw/customers/"
)

print("Raw Customer Count:", customers_raw.count())

# ---------------------------------------------------------
# CLEANING + STANDARDIZATION
# ---------------------------------------------------------

customers_processed = customers_raw \
    .dropDuplicates(["CustomerID"]) \
    .withColumn("CustomerName", initcap(trim(col("CustomerName")))) \
    .withColumn("Email", lower(trim(col("Email")))) \
    .withColumn("City", upper(trim(col("City")))) \
    .withColumn("Address", trim(col("Address"))) \
    .filter(col("CustomerID").isNotNull()) \
    .filter(col("Email").contains("@"))

# ---------------------------------------------------------
# ADD PROCESS TIMESTAMP
# ---------------------------------------------------------

customers_processed = customers_processed.withColumn(
    "processed_timestamp",
    current_timestamp()
)

# ---------------------------------------------------------
# WRITE TO PROCESSED LAYER
# ---------------------------------------------------------

customers_processed.write.mode("overwrite").parquet(
    "s3://retail-datawarehouse-3030/raw/customers/"
)

# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("Processed Customer Count:", customers_processed.count())

display(customers_processed)

# COMMAND ----------

# =========================================================
# PRODUCTS PROCESSED LAYER
# =========================================================

from pyspark.sql.functions import *

# ---------------------------------------------------------
# READ RAW PRODUCT DATA
# ---------------------------------------------------------

products_raw = spark.read.parquet(
    "s3://retail-datawarehouse-3030/raw/products/"
)

print("Raw Product Count:", products_raw.count())

# ---------------------------------------------------------
# CLEANING + STANDARDIZATION
# ---------------------------------------------------------

products_processed = products_raw \
    .dropDuplicates(["ProductID"]) \
    .withColumn("ProductName", initcap(trim(col("ProductName")))) \
    .withColumn("Category", upper(trim(col("Category")))) \
    .withColumn("UnitPrice", col("UnitPrice").cast("double")) \
    .filter(col("ProductID").isNotNull()) \
    .filter(col("UnitPrice") > 0)

# ---------------------------------------------------------
# ADD PROCESS TIMESTAMP
# ---------------------------------------------------------

products_processed = products_processed.withColumn(
    "processed_timestamp",
    current_timestamp()
)

# ---------------------------------------------------------
# WRITE TO PROCESSED LAYER
# ---------------------------------------------------------

products_processed.write.mode("overwrite").parquet(
    "s3://retail-datawarehouse-3030/processed/products/"
)

# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("Processed Product Count:", products_processed.count())

display(products_processed)

# COMMAND ----------

# =========================================================
# STORES PROCESSED LAYER
# RAW → PROCESSED
# =========================================================

from pyspark.sql.functions import *

# ---------------------------------------------------------
# READ RAW STORE DATA
# ---------------------------------------------------------

stores_raw = spark.read.parquet(
    "s3://retail-datawarehouse-3030/raw/stores/"
)

print("Raw Store Count:", stores_raw.count())

# ---------------------------------------------------------
# CHECK AVAILABLE COLUMNS
# ---------------------------------------------------------

print(stores_raw.columns)

# ---------------------------------------------------------
# CLEANING + STANDARDIZATION
# ---------------------------------------------------------

stores_processed = stores_raw \
    .dropDuplicates(["StoreID"]) \
    .withColumn("StoreName", initcap(trim(col("StoreName")))) \
    .withColumn("Region", upper(trim(col("Region")))) \
    .filter(col("StoreID").isNotNull())

# ---------------------------------------------------------
# ADD PROCESS TIMESTAMP
# ---------------------------------------------------------

stores_processed = stores_processed.withColumn(
    "processed_timestamp",
    current_timestamp()
)

# ---------------------------------------------------------
# WRITE TO PROCESSED LAYER
# ---------------------------------------------------------

stores_processed.write.mode("overwrite").parquet(
    "s3://retail-datawarehouse-3030/processed/stores/"
)

# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("Processed Store Count:", stores_processed.count())

display(stores_processed)

# COMMAND ----------

# =========================================================
# SALES PROCESSED LAYER
# RAW → PROCESSED
# =========================================================

from pyspark.sql.functions import *

# ---------------------------------------------------------
# READ RAW SALES DATA
# ---------------------------------------------------------

sales_raw = spark.read.parquet(
    "s3://retail-datawarehouse-3030/raw/sales/"
)

print("Raw Sales Count:", sales_raw.count())

# ---------------------------------------------------------
# READ PROCESSED PRODUCT DATA
# ---------------------------------------------------------

products_processed = spark.read.parquet(
    "s3://retail-datawarehouse-3030/processed/products/"
)

# ---------------------------------------------------------
# SELECT REQUIRED PRODUCT COLUMNS
# ---------------------------------------------------------

products_price = products_processed.select(
    "ProductID",
    "UnitPrice"
)

# ---------------------------------------------------------
# JOIN SALES WITH PRODUCTS
# ---------------------------------------------------------

sales_joined = sales_raw.join(
    products_price,
    on="ProductID",
    how="left"
)

# ---------------------------------------------------------
# CLEANING + STANDARDIZATION
# ---------------------------------------------------------

sales_processed = sales_joined \
    .dropDuplicates(["TransactionID"]) \
    .withColumn("Quantity", col("Quantity").cast("int")) \
    .withColumn("UnitPrice", col("UnitPrice").cast("double")) \
    .withColumn("SalesAmount", col("Quantity") * col("UnitPrice")) \
    .filter(col("TransactionID").isNotNull()) \
    .filter(col("Quantity") > 0)

# ---------------------------------------------------------
# ADD PROCESS TIMESTAMP
# ---------------------------------------------------------

sales_processed = sales_processed.withColumn(
    "processed_timestamp",
    current_timestamp()
)

# ---------------------------------------------------------
# WRITE TO PROCESSED LAYER
# ---------------------------------------------------------

sales_processed.write.mode("overwrite").parquet(
    "s3://retail-datawarehouse-3030/processed/sales/"
)

# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("Processed Sales Count:", sales_processed.count())

display(sales_processed)