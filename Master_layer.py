# Databricks notebook source
# DBTITLE 1,Cell 1
# =========================================================
# MASTER ETL PIPELINE
# =========================================================

from datetime import datetime

# =========================================================
# PIPELINE START
# =========================================================

start_time = datetime.now()

print("===================================")
print("RETAIL ETL PIPELINE STARTED")
print("Start Time:", start_time)
print("===================================")

# =========================================================
# STEP 1 - ARCHIVE LAYER
# =========================================================

print("\nSTEP 1 - ARCHIVE LAYER STARTED")

dbutils.notebook.run(
    "/Users/mallesh.srujan.reddy@gmail.com/archive _layer",
    600
)

print("STEP 1 COMPLETED")

# =========================================================
# STEP 2 - RAW LAYER
# =========================================================

print("\nSTEP 2 - RAW LAYER STARTED")

dbutils.notebook.run(
    "/Raw_layer",
    600
)

print("STEP 2 COMPLETED")

# =========================================================
# STEP 3 - PROCESSED LAYER
# =========================================================

print("\nSTEP 3 - PROCESSED LAYER STARTED")

dbutils.notebook.run(
    "/processed_layer",
    600
)

print("STEP 3 COMPLETED")

# =========================================================
# STEP 4 - GOLD LAYER
# =========================================================

print("\nSTEP 4 - GOLD LAYER STARTED")

dbutils.notebook.run(
    "/Users/mallesh.srujan.reddy@gmail.com/Gold_layer",
    600
)

print("STEP 4 COMPLETED")

# =========================================================
# STEP 5 - VALIDATION LAYER
# =========================================================

print("\nSTEP 5 - VALIDATION STARTED")

dbutils.notebook.run(
    "/Users/mallesh.srujan.reddy@gmail.com/Validation_layer",
    600
)

print("STEP 5 COMPLETED")

# =========================================================
# PIPELINE END
# =========================================================

end_time = datetime.now()

print("===================================")
print("RETAIL ETL PIPELINE COMPLETED")
print("End Time:", end_time)
print("===================================")

print("\nTOTAL PIPELINE EXECUTION TIME:")
print(end_time - start_time)