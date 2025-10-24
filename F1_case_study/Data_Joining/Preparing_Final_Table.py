# Databricks notebook source
dbutils.notebook.run("../Mounting_storage", 120)

# COMMAND ----------

circuits_df = spark.read.parquet('/mnt/blobstorage/circuits').withColumnRenamed('location', 'circuit_location')
#\    .withColumnRenamed('circuit_id', 'id')
display(circuits_df.head(2))

# COMMAND ----------

races_df = spark.read.parquet('/mnt/blobstorage/races').withColumnRenamed('date', 'race_date').withColumnRenamed('time', 'race_time')
display(races_df.head(2))

# COMMAND ----------

constructors_df = spark.read.parquet('/mnt/blobstorage/constructors')
display(constructors_df.head(2))

# COMMAND ----------

drivers_df = spark.read.parquet('/mnt/blobstorage/drivers').withColumnRenamed('name', 'driver_name').withColumnRenamed('nationality', 'driver_nationality')
display(drivers_df.head(2))

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner').drop(races_df.circuit_id, races_df.ingestion_date)
display(race_circuit_df.head(2))

# COMMAND ----------

result_df = spark.read.parquet('/mnt/blobstorage/results')
display(result_df)

# COMMAND ----------

race_circuit_result_df = race_circuit_df.join(result_df, race_circuit_df.race_id == result_df.race_id, 'inner').drop(race_circuit_df.race_id,  race_circuit_df.ingestion_date)

display(race_circuit_result_df)

# COMMAND ----------

race_circuit_result_constructor_df = race_circuit_result_df.join(constructors_df, race_circuit_result_df.constructor_id == constructors_df.constructor_id, 'inner').drop(constructors_df.constructor_id, constructors_df.ingestion_date)

display(race_circuit_result_constructor_df)

# COMMAND ----------

race_circuit_result_constructor__driver_df = race_circuit_result_constructor_df.join(drivers_df, race_circuit_result_constructor_df.driver_id == drivers_df.driver_id, 'inner').drop(drivers_df.driver_id, drivers_df.ingestion_time)
display(race_circuit_result_constructor__driver_df)

# COMMAND ----------

final_df = race_circuit_result_constructor__driver_df.filter("race_year = 2020").select('race_year', 'race_name', 'race_date', 'circuit_location', 'contructor_ref','driver_id','driver_name','driver_nationality','position','grid','fastest_lap', 'race_time', 'points', 'ingestion_date')
display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').save('/mnt/blobstorage/race_results')

# COMMAND ----------

