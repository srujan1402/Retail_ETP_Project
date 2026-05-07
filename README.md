# Retail ETL Pipeline using Databricks & AWS S3

## Overview

This project demonstrates an end-to-end ETL pipeline built using Databricks, PySpark, AWS S3, and Delta Lake. The pipeline follows a Medallion Architecture approach for processing retail sales data.

The solution supports:

* Full and Incremental Loads
* SCD Type 2 Implementation
* Data Validation
* Workflow Automation
* Unity Catalog Integration

---

# Architecture

```text id="jlwmdj"
S3 Landing Layer
      ↓
Archive Layer
      ↓
Raw Layer
      ↓
Processed Layer
      ↓
Gold Layer
      ↓
Validation Layer
```

---

# Technologies Used

* AWS S3
* Databricks
* PySpark
* Delta Lake
* Unity Catalog
* SQL

---

# Layers

## Landing Layer

Stores incoming source files.

## Archive Layer

Moves old files from landing to archive storage.

## Raw Layer

Stores ingested data with metadata columns like:

* ingestion_timestamp
* source_file

## Processed Layer

Applies:

* data cleansing
* deduplication
* standardization
* validations

## Gold Layer

Creates:

* dim_customer
* dim_product
* dim_store
* fact_sales

---

# SCD Type 2

Implemented on `dim_customer` to maintain historical customer changes using:

* StartDate
* EndDate
* IsActive

---

# Validation Checks

* Source-to-target validation
* Duplicate checks
* Null handling
* Transformation validation
* Referential integrity
* Incremental load validation

---

# Automation

The pipeline execution is automated using Databricks Workflows and notebook orchestration.

---

# Key Features

✅ Incremental Processing
✅ SCD Type 2
✅ Validation Framework
✅ Workflow Automation
✅ Delta Tables
✅ Unity Catalog Integration

---

# Author

Srujan Reddy Mallesh
