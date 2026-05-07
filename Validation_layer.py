# Databricks notebook source
# =========================================================
# RETAIL ETL VALIDATION NOTEBOOK
# =========================================================

from pyspark.sql.functions import *

print("======================================")
print("RETAIL ETL VALIDATION STARTED")
print("======================================")

# =========================================================
# READ TABLES
# =========================================================

raw_customers = spark.read.parquet(
    "s3://retail-datawarehouse-3030/raw/customers/"
)

processed_customers = spark.read.parquet(
    "s3://retail-datawarehouse-3030/processed/customers/"
)

dim_customer = spark.table(
    "retail_gold.dim_customer"
)

fact_sales = spark.table(
    "retail_gold.fact_sales"
)

processed_sales = spark.read.parquet(
    "s3://retail-datawarehouse-3030/processed/sales/"
)

processed_products = spark.read.parquet(
    "s3://retail-datawarehouse-3030/processed/products/"
)

# =========================================================
# 1. SOURCE TO TARGET TESTING
# =========================================================

print("\n======================================")
print("1. SOURCE TO TARGET TESTING")
print("======================================")

# ---------------------------------------------------------
# ROW COUNT VALIDATION
# ---------------------------------------------------------

raw_count = raw_customers.count()

processed_count = processed_customers.count()

print(f"Raw Customer Count: {raw_count}")
print(f"Processed Customer Count: {processed_count}")

# ---------------------------------------------------------
# COLUMN VALIDATION
# ---------------------------------------------------------

print("\nRAW CUSTOMER COLUMNS")
print(raw_customers.columns)

print("\nPROCESSED CUSTOMER COLUMNS")
print(processed_customers.columns)

# ---------------------------------------------------------
# DATA TYPE VALIDATION
# ---------------------------------------------------------

print("\nRAW CUSTOMER SCHEMA")
raw_customers.printSchema()

print("\nPROCESSED CUSTOMER SCHEMA")
processed_customers.printSchema()

# =========================================================
# 2. DATA TRANSFORMATION TESTING
# =========================================================

print("\n======================================")
print("2. DATA TRANSFORMATION TESTING")
print("======================================")

# ---------------------------------------------------------
# TRIM VALIDATION
# ---------------------------------------------------------

trim_check = processed_customers.filter(
    col("CustomerName").startswith(" ")
)

print("Trim Validation Failed Count:",
      trim_check.count())

# ---------------------------------------------------------
# LOWERCASE EMAIL VALIDATION
# ---------------------------------------------------------

email_check = processed_customers.filter(
    col("Email") != lower(col("Email"))
)

print("Lowercase Email Validation Failed Count:",
      email_check.count())

# ---------------------------------------------------------
# PROPER CASE VALIDATION
# ---------------------------------------------------------

name_check = processed_customers.filter(
    col("CustomerName") != initcap(col("CustomerName"))
)

print("Proper Case Validation Failed Count:",
      name_check.count())

# ---------------------------------------------------------
# SALES AMOUNT VALIDATION
# ---------------------------------------------------------

sales_amount_check = processed_sales.filter(
    col("SalesAmount") !=
    (col("Quantity") * col("UnitPrice"))
)

print("Sales Amount Validation Failed Count:",
      sales_amount_check.count())

# =========================================================
# 3. DATA QUALITY TESTING
# =========================================================

print("\n======================================")
print("3. DATA QUALITY TESTING")
print("======================================")

# ---------------------------------------------------------
# DUPLICATE CHECK
# ---------------------------------------------------------

duplicate_customers = processed_customers.groupBy(
    "CustomerID"
).count().filter(col("count") > 1)

print("Duplicate Customer Count:",
      duplicate_customers.count())

display(duplicate_customers)

# ---------------------------------------------------------
# NULL CHECK
# ---------------------------------------------------------

null_customers = processed_customers.filter(
    col("CustomerID").isNull()
)

print("Null CustomerID Count:",
      null_customers.count())

# ---------------------------------------------------------
# INVALID EMAIL CHECK
# ---------------------------------------------------------

invalid_email = processed_customers.filter(
    ~col("Email").contains("@")
)

print("Invalid Email Count:",
      invalid_email.count())

# =========================================================
# FIX PRODUCTID FORMAT
# =========================================================

processed_sales = processed_sales.withColumn(
    "ProductID",
    upper(trim(col("ProductID")))
)

processed_products = processed_products.withColumn(
    "ProductID",
    upper(trim(col("ProductID")))
)

# =========================================================
# FIX PRODUCTID FORMAT
# =========================================================

processed_sales = processed_sales.withColumn(
    "ProductID",
    upper(trim(col("ProductID").cast("string")))
)

processed_products = processed_products.withColumn(
    "ProductID",
    upper(trim(col("ProductID").cast("string")))
)

# =========================================================
# REFERENTIAL INTEGRITY CHECK
# =========================================================

invalid_product_sales = processed_sales.join(
    processed_products,
    on="ProductID",
    how="left_anti"
)

print("Invalid Product Reference Count:",
      invalid_product_sales.count())

display(invalid_product_sales)

# =========================================================
# 4. SCD TYPE 2 TESTING
# =========================================================

print("\n======================================")
print("4. SCD TYPE 2 TESTING")
print("======================================")

# ---------------------------------------------------------
# MULTIPLE ACTIVE RECORD CHECK
# ---------------------------------------------------------

multiple_active = dim_customer.filter(
    col("IsActive") == 1
).groupBy("CustomerID").count().filter(
    col("count") > 1
)

print("Multiple Active Records Count:",
      multiple_active.count())

display(multiple_active)

# ---------------------------------------------------------
# ACTIVE RECORD END DATE CHECK
# ---------------------------------------------------------

active_enddate_check = dim_customer.filter(
    (col("IsActive") == 1) &
    (col("EndDate").isNotNull())
)

print("Invalid Active EndDate Count:",
      active_enddate_check.count())

# ---------------------------------------------------------
# INACTIVE RECORD END DATE CHECK
# ---------------------------------------------------------

inactive_enddate_check = dim_customer.filter(
    (col("IsActive") == 0) &
    (col("EndDate").isNull())
)

print("Inactive Record Missing EndDate Count:",
      inactive_enddate_check.count())

# ---------------------------------------------------------
# HISTORICAL RECORD CHECK
# ---------------------------------------------------------

historical_records = dim_customer.groupBy(
    "CustomerID"
).count().filter(col("count") > 1)

print("Historical Customer Records Count:",
      historical_records.count())

display(historical_records)

# =========================================================
# 5. FULL LOAD VS INCREMENTAL LOAD TESTING
# =========================================================

print("\n======================================")
print("5. FULL LOAD VS INCREMENTAL LOAD")
print("======================================")

# ---------------------------------------------------------
# FULL LOAD COUNT
# ---------------------------------------------------------

full_load_count = raw_customers.count()

print("Full Load Customer Count:",
      full_load_count)

# ---------------------------------------------------------
# INCREMENTAL LOAD COUNT
# ---------------------------------------------------------

incremental_count = dim_customer.filter(
    col("IsActive") == 1
).count()

print("Current Active Customer Count:",
      incremental_count)

# ---------------------------------------------------------
# SCD2 ACTIVE VS INACTIVE
# ---------------------------------------------------------

active_count = dim_customer.filter(
    col("IsActive") == 1
).count()

inactive_count = dim_customer.filter(
    col("IsActive") == 0
).count()

print("Active Customers:", active_count)
print("Inactive Customers:", inactive_count)

# =========================================================
# VALIDATION COMPLETE
# =========================================================

print("\n======================================")
print("RETAIL ETL VALIDATION COMPLETED")
print("======================================")