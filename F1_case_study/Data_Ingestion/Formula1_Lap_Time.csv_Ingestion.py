# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Lap Time split file

# COMMAND ----------

dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_time_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("Position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

df_lap_times = spark.read.csv("/mnt/blobstorage/lap_times_split*.csv", schema=lap_time_schema)

display(df_lap_times)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming and adding new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = df_lap_times.withColumnRenamed("driverId", "driver_id") \
                     .withColumnRenamed("raceId", "race_id") \
                     .withColumn("ingestion_time", current_timestamp())

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write it to Parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/blobstorage/lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/lap_times