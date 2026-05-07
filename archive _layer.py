# Databricks notebook source
display(dbutils.fs.ls(
    "s3://retail-datawarehouse-3030/sftp/"
))

# COMMAND ----------

# =========================================================
# GENERIC AUTOMATIC ARCHIVE PROCESS
# CUSTOMERS / PRODUCTS / STORES / SALES
# =========================================================

# ---------------------------------------------------------
# READ FILES FROM SFTP
# ---------------------------------------------------------

files = dbutils.fs.ls(
    "s3://retail-datawarehouse-3030/sftp/"
)

# ---------------------------------------------------------
# ENTITY GROUPING
# ---------------------------------------------------------

entities = {
    "customers": [],
    "products": [],
    "stores": [],
    "sales": []
}

# ---------------------------------------------------------
# CLASSIFY FILES
# ---------------------------------------------------------

for file in files:

    file_name = file.name.lower()

    file_info = {
        "path": file.path,
        "name": file.name,
        "modificationTime": file.modificationTime
    }

    # CUSTOMERS
    if "customer" in file_name:
        entities["customers"].append(file_info)

    # PRODUCTS
    elif "product" in file_name:
        entities["products"].append(file_info)

    # STORES
    elif "store" in file_name:
        entities["stores"].append(file_info)

    # SALES
    elif "sales" in file_name:
        entities["sales"].append(file_info)

# ---------------------------------------------------------
# PROCESS EACH ENTITY
# ---------------------------------------------------------

for entity, entity_files in entities.items():

    print(f"\n========== {entity.upper()} ==========")

    # -----------------------------------------------------
    # CHECK FILE COUNT
    # -----------------------------------------------------

    if len(entity_files) <= 1:

        print(f"No old {entity} files to archive")

        continue

    # -----------------------------------------------------
    # SORT BY MODIFICATION TIME
    # -----------------------------------------------------

    entity_files = sorted(
        entity_files,
        key=lambda x: x["modificationTime"]
    )

    # -----------------------------------------------------
    # KEEP LATEST FILE
    # -----------------------------------------------------

    latest_file = entity_files[-1]

    old_files = entity_files[:-1]

    print("Latest File To Keep:")
    print(latest_file["name"])

    print("\nOld Files To Archive:")

    for old in old_files:
        print(old["name"])

    # -----------------------------------------------------
    # MOVE OLD FILES
    # -----------------------------------------------------

    for old in old_files:

        archive_path = (
            f"s3://retail-datawarehouse-3030/archive/{entity}/"
            + old["name"]
        )

        dbutils.fs.mv(
            old["path"],
            archive_path
        )

        print(f"Archived: {old['name']}")

print("\nAutomatic archive completed successfully")