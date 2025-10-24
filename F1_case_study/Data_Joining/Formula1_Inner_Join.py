# Databricks notebook source
dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

dbutils.fs.ls("/mnt/blobstorage")

# COMMAND ----------

circuits_df = spark.read.parquet('/mnt/blobstorage/circuits')
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet('/mnt/blobstorage/races').filter("race_year = 2019")
display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner')\
    .select(circuits_df.name, circuits_df.circuit_id, circuits_df.location, races_df.race_year)
display(race_circuit_df)

# COMMAND ----------



# COMMAND ----------

