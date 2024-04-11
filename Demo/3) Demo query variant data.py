# Databricks notebook source
# MAGIC %md
# MAGIC #### Set up SQL variables
# MAGIC

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------


schema = "cmotters_m42.demo."
dbutils.widgets.text("variants", f"{schema}raw_variants_clustered") 
dbutils.widgets.text("alt_allele", f"{schema}alt_allele")
dbutils.widgets.text("alt_allele_not_clustered", f"{schema}alt_allele")
dbutils.widgets.text("alt_allele_condensed", f"{schema}alt_allele_condensed")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Querying large (3k samples) dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check unaggregated table within narrow range
# MAGIC SELECT count(*) FROM ${alt_allele}
# MAGIC WHERE 
# MAGIC location > 1000000000000
# MAGIC AND 
# MAGIC location < 1000000050000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check aggregated table within same narrow range
# MAGIC SELECT count(*) FROM ${alt_allele_condensed}
# MAGIC WHERE 
# MAGIC location > 1000000000000
# MAGIC AND 
# MAGIC location < 1000000050000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM ${alt_allele_not_clustered};

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- Get all values from unoptimized table within narrow range
# MAGIC SELECT * FROM ${alt_allele_not_clustered}
# MAGIC WHERE 
# MAGIC location > 1000000000000
# MAGIC AND 
# MAGIC location < 1000000050000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Get all values from clustered table within narrow range
# MAGIC SELECT * FROM ${alt_allele}
# MAGIC WHERE 
# MAGIC location > 1000000000000
# MAGIC AND 
# MAGIC location < 1000000500000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- For looking at the performance
# MAGIC SELECT * FROM ${alt_allele_condensed}
# MAGIC WHERE 
# MAGIC location > 1000000000000
# MAGIC AND 
# MAGIC location < 1000000050000;