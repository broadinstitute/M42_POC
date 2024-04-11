# Databricks notebook source
# MAGIC %md
# MAGIC #### Set up SQL and python variables
# MAGIC

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------


# RUN the above cell first!  
# Otherwise these changes will not update!
#
#  (not sure why it doesn't work in here, but don't want to spend time looking into it)

version = "_vtest"
#dbutils.widgets.text("variants", f"M42_POC.default.raw_variants_clustered{version}")
dbutils.widgets.text("variants", f"M42_POC.default.raw_variants_clustered_v3")
dbutils.widgets.text("alt_allele", f"alt_allele{version}")
dbutils.widgets.text("alt_allele_not_clustered", f"alt_allele{version}")
dbutils.widgets.text("alt_allele_condensed", f"alt_allele_condensed{version}")

dbutils.widgets.text("new_samples", f"new_samples{version}")
dbutils.widgets.text("new_samples_condensed", f"new_samples_condensed{version}")

# COMMAND ----------

# setup python variables

# tables
variants = dbutils.widgets.get("variants")
alt_allele = dbutils.widgets.get("alt_allele")
alt_allele_condensed = dbutils.widgets.get("alt_allele_condensed")

new_samples = dbutils.widgets.get("new_samples")
new_samples_condensed = dbutils.widgets.get("new_samples_condensed")

# samples to use as 'new incoming samples'
create_samples_table = False
samples_to_extract = 200

# imports
from pyspark.sql.functions import col


# COMMAND ----------

# MAGIC %md
# MAGIC #### create an alt_allele table from the raw variants data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION minimize(ref STRING, allele STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC def minimize(ref, allele):
# MAGIC     while len(ref) != 1 and len(allele) != 1:
# MAGIC         if ref[-1] == allele[-1]:
# MAGIC             ref = ref[:-1]
# MAGIC             allele = allele[:-1]
# MAGIC         else:
# MAGIC             break
# MAGIC     return ref + "," + allele
# MAGIC return minimize(ref, allele)
# MAGIC $$;
# MAGIC
# MAGIC
# MAGIC WITH
# MAGIC position1 AS (
# MAGIC     SELECT * FROM ${variants} WHERE
# MAGIC         call_GT IN ('0/1', '1/0', '1/1', '0|1', '1|0', '1|1', '0/2', '0|2','2/0', '2|0', '1', '2')
# MAGIC ),
# MAGIC position2 AS (
# MAGIC     SELECT * FROM ${variants} WHERE
# MAGIC         call_GT IN ('1/2', '1|2', '2/1', '2|1')
# MAGIC )
# MAGIC select location, sample_id,
# MAGIC     SPLIT(minimize(ref, SPLIT(alt,',')[0]),',')[0] as ref,
# MAGIC     SPLIT(minimize(ref, SPLIT(alt,',')[0]),',')[1] as allele,
# MAGIC     1 as allele_pos, call_GT, call_GQ,
# MAGIC     as_raw_mq,
# MAGIC     try_cast(SPLIT(as_raw_mq,'|')[1] as bigint) raw_mq,
# MAGIC     as_raw_mqranksum,
# MAGIC     cast(SPLIT(as_qualapprox,'|')[0] as bigint) as qual,
# MAGIC     as_raw_readposranksum,
# MAGIC     as_sb_table,
# MAGIC     call_AD,
# MAGIC     cast(SPLIT(call_AD,',')[0] as bigint) as ref_ad,
# MAGIC     cast(SPLIT(call_AD,',')[1] as bigint) as ad
# MAGIC from position1
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select location, sample_id,
# MAGIC     SPLIT(minimize(ref, SPLIT(alt,',')[0]),',')[0] as ref,
# MAGIC     SPLIT(minimize(ref, SPLIT(alt,',')[0]),',')[1] as allele,
# MAGIC     1 as allele_pos, call_GT, call_GQ,
# MAGIC     as_raw_mq,
# MAGIC     try_cast(SPLIT(as_raw_mq,'|')[1] as bigint) raw_mq,
# MAGIC     as_raw_mqranksum,
# MAGIC     cast(SPLIT(as_qualapprox,'|')[0] as bigint) as qual,
# MAGIC     as_raw_readposranksum,
# MAGIC     as_sb_table,
# MAGIC     call_AD,
# MAGIC     cast(SPLIT(call_AD,',')[0] as bigint) as ref_ad,
# MAGIC     cast(SPLIT(call_AD,',')[1] as bigint) as ad
# MAGIC from position2
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select location, sample_id,
# MAGIC     SPLIT(minimize(ref, SPLIT(alt,',')[1]),',')[0] as ref,
# MAGIC     SPLIT(minimize(ref, SPLIT(alt,',')[1]),',')[1] as allele,
# MAGIC     2 as allele_pos, call_GT, call_GQ,
# MAGIC     as_raw_mq,
# MAGIC     try_cast(SPLIT(as_raw_mq,'|')[2] as bigint) raw_mq,
# MAGIC     as_raw_mqranksum,
# MAGIC     cast(SPLIT(as_qualapprox,'|')[1] as bigint) as qual,
# MAGIC     as_raw_readposranksum,
# MAGIC     as_sb_table,
# MAGIC     call_AD,
# MAGIC     cast(SPLIT(call_AD,',')[0] as bigint) as ref_ad,
# MAGIC     cast(SPLIT(call_AD,',')[2] as bigint) as ad
# MAGIC from position2;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Create the alt_allele table
# MAGIC
# MAGIC if we are testing and creating new_samples data from the alt_allele table:
# MAGIC - split off some of the sample_ids so that we can use them later as new_samples to verify processing new incoming samples 
# MAGIC - save alt_allele without the extracted sample_ids
# MAGIC - save new_samples with only the extracted sample_ids

# COMMAND ----------


if not create_samples_table: 
    spark.sql(f"drop table {alt_allele}")
    
    print(f"writing to the {alt_allele} table")

    df = _sqldf

    # write this df out to alt_allele table
    df.writeTo(alt_allele).using("delta").clusterBy("location").create()

    # now optimize it
    spark.sql(f"OPTIMIZE {alt_allele}")
else: 
    print("Skipping this")

# COMMAND ----------

# this is used for testing purposes and is very expensive to run
# it extracts samples_to_extract number of sample_ids from the dataframe
# then writes the dataframe to disk with these samples, and another without these samples
# (this is comparable to creating a training and test dataset from the raw input data)

# we should only do this once on new RAW input data if we are testing ingesting new samples

if create_samples_table:
    # grab the results of the preceding query
    df = spark.sql(sql_statement)

    # grab some sample_ids in this df
    sample_ids = df.select(col("sample_id")).distinct().limit(samples_to_extract)

    # Create alt_allele df by removing records for the sample_ids to extract
    alt_allele_df = df.join(
        sample_ids, df.sample_id == sample_ids.sample_id, "left_anti"
    )

    # write this df out to alt_allele table
    alt_allele_df.writeTo(alt_allele).using("delta").clusterBy("location").create()

    # now optimize it
    spark.sql(f"OPTIMIZE {alt_allele}")

    # create a new_samples table with the extracted sample_ids
    new_samples_df = df.join(sample_ids, ["sample_id"], "inner")

    new_samples_df.writeTo(new_samples).using("delta").clusterBy("location").create()
    spark.sql(f"OPTIMIZE {new_samples}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a alt_allele_condensed table from the alt_allele table
# MAGIC
# MAGIC Transform the alt_allele data to a representation that treats the location/ref/allele tuple as a unique identifier and lists all of the samples that have it 

# COMMAND ----------

 
# SQL: 
# SELECT location, ref, allele, array_agg(sample_id) as sample_ids FROM M42_POC.default.alt_allele_v2 
# group by location, ref, allele

from pyspark.sql.functions import expr
df = (spark
      .read
      .table(alt_allele)
      .groupBy('location', 'ref', 'allele')
      .agg(expr('array_agg (sample_id)').alias('sample_ids'))
) 

df.writeTo(alt_allele_condensed).using("delta").clusterBy("location").create() 

# now optimize it
spark.sql(f"OPTIMIZE {alt_allele_condensed}")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process for continuously ingesting new samples 
# MAGIC
# MAGIC - create a new_samples_condensed table from new_samples
# MAGIC - Merge these new_samples_condensed into the alt_allele_condensed table
# MAGIC

# COMMAND ----------

# create the new_samples_condensed table

if create_samples_table:
    from pyspark.sql.functions import expr

    df = (
        spark.read.table(new_samples)
        .groupBy("location", "ref", "allele")
        .agg(expr("array_agg (sample_id)").alias("sample_ids"))
    )

    df.write.saveAsTable(new_samples_condensed)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge the new_samples_condensed into the alt_allele_condensed table (the **master** table)

# COMMAND ----------

if create_samples_table:
    sql_statement = """
MERGE INTO {alt_allele_condensed}  m
USING {new_samples_condensed} c
on m.location = c.location and m.ref = c.ref and m.allele = c.allele
WHEN MATCHED THEN
  UPDATE SET 
  m.sample_ids = array_union(m.sample_ids, c.sample_ids)
WHEN NOT MATCHED THEN
  INSERT *
"""

    spark.sql(sql_statement)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify that the new samples were merged

# COMMAND ----------

from pyspark.sql.functions import array_contains, col

if create_samples_table:
    df = spark.read.table(alt_allele_condensed).where(
        array_contains(col("sample_ids"), sample_ids.head()[0])
    )

    display(df.select("location", "sample_ids"))