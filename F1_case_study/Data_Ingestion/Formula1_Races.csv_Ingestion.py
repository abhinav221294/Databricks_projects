# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Races.csv file

# COMMAND ----------

dbutils.notebook.run('../Mounting_storage', 60)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("year",IntegerType(),True),
                                 StructField("round",IntegerType(),True),
                                 StructField("circuitId",IntegerType(),True),
                                 StructField("name",StringType(),True),
                                 StructField("date",DateType(), True),
                                 StructField("time",StringType(),True),
                                 StructField("url",StringType(),True)
                                 ])

# COMMAND ----------

df_races = spark.read.csv('/mnt/blobstorage/races.csv', header=True, schema=races_schema)
display(df_races.head(5))

# COMMAND ----------

# MAGIC %md ### Adding Ingestion date and race time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,current_date,col, to_timestamp, concat_ws
df_races = df_races.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat_ws(' ', col('date'), col('time')), 'yyyy-MM-dd HH:mm:ss'))
display(df_races.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Required columns and renaming column

# COMMAND ----------

races_select_df = df_races.select(col("raceid").alias("race_id"),
                                         col("year").alias("race_year"),
                                         col("round"),
                                         col("circuitId").alias("circuit_id"),
                                         col("name").alias("race_name"),
                                         col("date"),
                                         col("time"),
                                         col("ingestion_date"),
                                         col("race_timestamp"))
display(races_select_df.head(5))  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to Parquet

# COMMAND ----------

races_select_df.write.mode("overwrite").parquet('/mnt/blobstorage/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/races

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creation of Partition for race.csv file

# COMMAND ----------

races_select_df.write.mode("overwrite").partitionBy("race_year").parquet('/mnt/blobstorage/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/races