# Databricks notebook source
# MAGIC %md
# MAGIC #### Set up SQL variables
# MAGIC

# COMMAND ----------


dbutils.widgets.removeAll()

# COMMAND ----------


dbutils.widgets.text("variants", "M42_POC.default.raw_variants_clustered_v3") 
dbutils.widgets.text("alt_allele", "alt_allele_v3")
dbutils.widgets.text("alt_allele_not_clustered", "alt_allele_not_clustered")
dbutils.widgets.text("alt_allele_condensed", "alt_allele_condensed_v3")

# COMMAND ----------

spark.read.table("alt_allele_not_clustered").count()

# COMMAND ----------

spark.read.table("alt_allele_v3").count()

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

# COMMAND ----------

