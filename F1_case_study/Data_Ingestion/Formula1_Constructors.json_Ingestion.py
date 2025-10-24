# Databricks notebook source
# MAGIC %md # Ingest constructor.json file

# COMMAND ----------

dbutils.notebook.run('../Mounting_storage', 60)

# COMMAND ----------

dbutils.fs.ls("/mnt/blobstorage")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


df_contructors = spark.read.json("/mnt/blobstorage/constructors.json", schema=constructors_schema)
display(df_contructors.head(5))

# COMMAND ----------

df_contructors.printSchema()

# COMMAND ----------

display(df_contructors)

# COMMAND ----------

# MAGIC %md ### Drop unwanted columns from database

# COMMAND ----------

from pyspark.sql.functions import col

df_contructors_dropped = df_contructors.drop(col('url'))

display(df_contructors_dropped)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_contructors_final = df_contructors_dropped.withColumnRenamed("constructorRef","contructor_ref")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn('ingestion_date', current_timestamp())

display(df_contructors_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the output to parquet file

# COMMAND ----------

df_contructors_final.write.mode("overwrite").parquet("/mnt/blobstorage/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/constructors