# Databricks notebook source
dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

races_df = spark.read.parquet("/mnt/blobstorage/races")
display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2018")
display(races_filtered_df)

# COMMAND ----------

races_df_2 = races_df.filter(races_df["race_year"] == 2019)
display(races_df_2)

# COMMAND ----------

races_df_3 = races_df.filter("race_year = 2019 and round < 5")
display(races_df_3)

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"]==2017) & (races_df["round"] <= 5))
display(races_filtered_df)