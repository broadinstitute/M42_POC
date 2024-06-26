# Databricks notebook source
# MAGIC %md
# MAGIC ### This pipeline loads the parquet files that were generated by GATK and placed in Blob storage and creates a new table in the RAW Zone
# MAGIC
# MAGIC This is the RAW data that will live in the Datalake RAW zone
# MAGIC
# MAGIC ##### NOTE: this notebook requires DBR 14.3 LTS
# MAGIC

# COMMAND ----------


# notebook variables

# Blob storage access
sasToken = "sp=racwl&st=2024-04-10T13:38:18Z&se=2024-05-01T21:38:18Z&spr=https&sv=2022-11-02&sr=c&sig=KRG052bJ6jPgkbcJPWeYKqXNNqjhH2dQkf5YDA1WH8A%3D"
storageAccountName = "gvcfparquet"
storageAccountAccessKey = "none"
blobContainerName = "parquet-files"  

# 3k samples
raw_input_file_path = "v0_3k/parquet-staging"
# table names (these are the output tables from this notebook)
raw_table_name = "raw_variants_v3"
clustered_raw_table_name = "raw_variants_clustered_v3" 

source_files = f"/mnt/data/{raw_input_file_path}/"


# COMMAND ----------

# MAGIC %md
# MAGIC #### First, setup a connector to the Blob storage
# MAGIC

# COMMAND ----------


# umount the blob storage
if any(mount.mountPoint == "/mnt/data/" for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount("/mnt/data")

# mount the blob storage as /mnt/data
mountPoint = "/mnt/data/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e) 
else: 
  print("Error: mount point exists")

# list the files we just mounted
df = spark.createDataFrame(dbutils.fs.ls(source_files))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process all parquet files in this folder
# MAGIC
# MAGIC This will create a table (or dataset) from the parquet files located at the raw_input_file_path

# COMMAND ----------

df = spark.read.parquet(source_files)
#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save dataframe as a Table 
# MAGIC
# MAGIC This will save this dataframe to a permanent table that is cataloged in the Unity Data Catalog  
# MAGIC - A summary of the table can be provided
# MAGIC - columns are described
# MAGIC - comments can be added for each column
# MAGIC - Permmissions can be set for who can access/update this table
# MAGIC - lineage will be created (if we created this table as a join of two tables for example, it will which columns came from which table)
# MAGIC - etc

# COMMAND ----------


spark.sql(f"DROP TABLE IF EXISTS {raw_table_name}")

df.write.saveAsTable(raw_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cluster the table 
# MAGIC Configure liquid clustering on the **location** column, then OPTIMIZE it to perform the clustering 

# COMMAND ----------

cluster_by = "location"

spark.sql(f"DROP TABLE IF EXISTS {clustered_raw_table_name}") 

df = spark.read.table(raw_table_name)

# liquid cluster it by location
df.writeTo(clustered_raw_table_name).using("delta").clusterBy(cluster_by).create()

# now optimize it
spark.sql(f"OPTIMIZE {clustered_raw_table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC