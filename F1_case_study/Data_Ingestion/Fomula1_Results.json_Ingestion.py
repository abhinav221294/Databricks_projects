# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Results.json file

# COMMAND ----------

dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, TimestampType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True),
                            StructField("positionOrder", IntegerType(), True),
                            StructField("points", DoubleType(), True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            StructField("fastestLap", IntegerType(), True),
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", DoubleType(), True),
                            StructField("statusId", StringType(), True)])


# COMMAND ----------

df_result = spark.read.json("/mnt/blobstorage/results.json", schema=results_schema)
display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming Columns and add

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df_results_with_column = df_result.withColumn("ingestion_date", current_timestamp())
display(df_results_with_column)

# COMMAND ----------

df_results_with_column = df_results_with_column.withColumnRenamed("resultId","result_id")\
                                  .withColumnRenamed("raceId","race_id")\
                                  .withColumnRenamed("driverId","driver_id")\
                                  .withColumnRenamed("constructorId","constructor_id")\
                                  .withColumnRenamed("positionText","position_text")\
                                  .withColumnRenamed("positionOrder","position_order")\
                                      .withColumnRenamed("fastestLap","fastest_lap")\
                                       .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                        .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")
display(df_results_with_column)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_resul_final = df_results_with_column.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the output to the parquet file

# COMMAND ----------

df_resul_final.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/blobstorage/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/results