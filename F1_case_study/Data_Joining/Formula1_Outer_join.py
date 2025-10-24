# Databricks notebook source
dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

circuits_df = spark.read.parquet('/mnt/blobstorage/circuits').filter("circuit_Id < 70")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet('/mnt/blobstorage/races').filter("race_year = 2021")
display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'left')\
    .select(circuits_df.circuit_name, circuits_df.circuit_id, circuits_df.location, races_df.race_year)
display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'right')\
    .select(circuits_df.circuit_name, circuits_df.circuit_id, circuits_df.location, races_df.race_year)
display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'full')\
    .select(circuits_df.circuit_name, circuits_df.circuit_id, circuits_df.location, races_df.race_year)
display(race_circuit_df)