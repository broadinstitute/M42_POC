# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup access to ADLSv2 storage account "shareddelta"
# MAGIC
# MAGIC We have set up an Azure datalake (ADLSv2) where all our files/tables live.
# MAGIC - Storage account: `shareddelta`
# MAGIC - Container: `testcontainer`
# MAGIC - Subscription: `8201558-variants-team`
# MAGIC
# MAGIC This Data Lake Storage has `Hierarchical namespace` enabled to allow accessing via `abfss` connector
# MAGIC
# MAGIC The SAS_Token must be created from the Storage account blade: `Shared access signature` (the breadcrumbs should be `Home > Storage accounts > shareddelta`)

# COMMAND ----------

# setup for the `shareddelta` ADLSv2 storage account and the `testcontainer` container

storage_account = "shareddelta"
container_name = "testcontainer"
infile = "/base/vet_001_ERS4367795.vcf.gz.parquet"
outfile = "/delta_tables/table4"

### from shareddelta, Shared access signature
### top breadcrumbs should be:
### shareddelta | Shared access signature 

sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-05-04T20:31:43Z&st=2024-04-09T12:31:43Z&spr=https&sig=Vc6MfIVjYqOsu%2FcsF1AXNSeOCGGz2bxNPDf%2FHhkaTr8%3D"


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### (run either the above cell or this one, not both) 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Setup access to Blob storage account "gvcfparquet"
# MAGIC
# MAGIC note: this account currently has 'auto delete' enabled which is not supported for writing to the blob over the `abfss` connector, so writes are not enabled

# COMMAND ----------

# setup for the `gvcfparquet` blob storage account and the `parquet-files` container

if False: 
  storage_account = "gvcfparquet"
  container_name = "parquet-files"
  infile = "/v0"
  outfile = "" # currently do not have permissions set to allow writing due to 'auto delete' turned on

  sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-04-07T23:17:17Z&st=2024-04-02T15:17:17Z&spr=https&sig=xcP63u8uQPbAp0ugS4Vq%2F5QgNGSrykH%2Bj6%2BkPayhDlc%3D"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Based on previous tokens, set config to access the ADLSv2 or Blob storage

# COMMAND ----------


in_filename = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net{infile}"
out_filename = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net{outfile}"

# set up config
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)


# COMMAND ----------

print(in_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the parquet files in the in_filename

# COMMAND ----------


df = spark.read.parquet(in_filename)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write a delta table to the out_filename
# MAGIC
# MAGIC note: this does not work for the Blob storage, only for the ADLSv2

# COMMAND ----------


df.write.format("delta").save(out_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ### List the files that were created for the delta table

# COMMAND ----------

df = spark.createDataFrame(dbutils.fs.ls(out_filename))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the table back in

# COMMAND ----------

# read the delta table back in 

df = spark.read.format("delta").load(out_filename)

display(df)