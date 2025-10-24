# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Pitstop.json file

# COMMAND ----------

dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

df_pitstop = spark.read.json("/mnt/blobstorage/pit_stops.json", multiLine=True, schema=pitstop_schema)

display(df_pitstop)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming and adding new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = df_pitstop.withColumnRenamed("driverId", "driver_id") \
                     .withColumnRenamed("raceId", "race_id") \
                     .withColumn("ingestion_time", current_timestamp())

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write it to Parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/blobstorage/pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/races