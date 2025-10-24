# Databricks notebook source
dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Join

# COMMAND ----------

circuits_df = spark.read.parquet('/mnt/blobstorage/circuits').filter("circuit_Id < 70")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet('/mnt/blobstorage/races').filter("race_year = 2020")
display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'semi')
display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'anti')
display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### cross join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'cross')
display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df)
display(race_circuit_df)

# COMMAND ----------

