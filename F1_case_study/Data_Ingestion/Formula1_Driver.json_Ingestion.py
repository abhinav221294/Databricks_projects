# Databricks notebook source
dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

dbutils.fs.ls("/mnt/blobstorage")

# COMMAND ----------

drivers_schema = """
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
""" 

# COMMAND ----------

name_schema = """
forename STRING,
surname STRING
"""

# COMMAND ----------

df_drivers = spark.read.json("/mnt/blobstorage/drivers.json", schema = drivers_schema)
display(df_drivers)

# COMMAND ----------

df_drivers.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Removing columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = df_drivers.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
        .withColumn("ingestion_time", current_timestamp())\
            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Dropping unwanted column

# COMMAND ----------

df_drivers_final = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

display(df_drivers_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write it to parquet format

# COMMAND ----------

df_drivers_final.write.mode("overwrite").parquet("/mnt/blobstorage/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/drivers