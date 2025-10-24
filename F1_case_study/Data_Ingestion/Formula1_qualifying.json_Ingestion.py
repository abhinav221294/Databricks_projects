# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Qualifying.json file

# COMMAND ----------

dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualify_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

df_qualifying = spark.read.option("multiline", True).schema(qualify_schema).json("/mnt/blobstorage/qualifying_split_*.json")

display(df_qualifying)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming and adding new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = df_qualifying.withColumnRenamed("qualifyId", "qualify_id")\
                     .withColumnRenamed("driverId", "driver_id") \
                     .withColumnRenamed("raceId", "race_id") \
                     .withColumnRenamed("constructorId", "constructor_id")\
                     .withColumn("ingestion_time", current_timestamp())

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write it to Parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/blobstorage/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/qualifying