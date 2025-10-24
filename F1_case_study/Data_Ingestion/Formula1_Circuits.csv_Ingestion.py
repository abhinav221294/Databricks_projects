# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuit.csv

# COMMAND ----------

# MAGIC %md ### read data from csv file

# COMMAND ----------

dbutils.notebook.run('../Mounting_storage', 60)

# COMMAND ----------

#df_circuits = spark.read.csv('/mnt/blobstorage/circuits.csv', header=True)
#display(df_circuits.head(5))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), nullable=False),
                                     StructField("circuitRef", StringType(), nullable=True),
                                     StructField("name", StringType(),  nullable=True),
                                     StructField("location", StringType(),  nullable=True),
                                     StructField("country", StringType(),  nullable=True),
                                     StructField("lat", DoubleType(),  nullable=True),
                                     StructField("lng", DoubleType(),  nullable=True),
                                     StructField("alt", IntegerType(),  nullable=True),
                                     StructField("url", StringType(),  nullable=True)])

# COMMAND ----------

df_circuits = spark.read.csv('/mnt/blobstorage/circuits.csv', header=True, schema=circuits_schema)
display(df_circuits.head(5))

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

#from pyspark.sql.functions import col
#
## Filter out rows where circuitId is null
#df_circuits = df_circuits.filter(col("circuitId").isNotNull())

# COMMAND ----------

#circuits_select = df_circuits.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
#
#display(circuits_select)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming the required columns

# COMMAND ----------

from pyspark.sql.functions import col

circuits_select = df_circuits.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

display(circuits_select)

# COMMAND ----------

circuits_select_df = circuits_select.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_Ref")\
        .withColumnRenamed("name","circuit_name")\
        .withColumnRenamed("lat","latitude")\
            .withColumnRenamed("lng","longitude")\
                .withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md ### Adding a column with ingestion Date

# COMMAND ----------

#from pyspark.sql.functions import current_timestamp,lit
#
#circuits_select_df = circuits_select_df.withColumn("ingestion_date",current_timestamp())\
#    .withColumn("env",lit("Production"))
#display(circuits_select_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_select_df = circuits_select_df.withColumn("ingestion_date",current_timestamp())
display(circuits_select_df)

# COMMAND ----------

circuits_select_df.write.mode("overwrite").parquet('/mnt/blobstorage/circuits')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/blobstorage/circuits