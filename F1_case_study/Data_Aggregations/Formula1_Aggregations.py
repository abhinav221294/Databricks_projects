# Databricks notebook source
from pyspark.sql.functions import count, countDistinct,col
from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading the race_results parquet file

# COMMAND ----------

race_results = spark.read.parquet('/mnt/blobstorage/race_results').withColumn('points',col('points').cast('int'))
display(race_results)

# COMMAND ----------

race_results.printSchema()

# COMMAND ----------

race_results.select(count("race_name")).show()

# COMMAND ----------

race_results.select(countDistinct("contructor_ref")).show()

# COMMAND ----------

race_results.select(spark_sum("points")).show()


# COMMAND ----------

race_results.filter("driver_name= 'Lewis Hamilton'").select(spark_sum("points")).show()

# COMMAND ----------

race_results.filter("driver_name= 'Max Verstappen'").select(spark_sum("points")).show()

# COMMAND ----------

race_results.filter("contructor_ref= 'mercedes'").select(spark_sum("points")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group by

# COMMAND ----------

# MAGIC %md ### For driver Lewis Hamliton we will take count of different races and sum the points

# COMMAND ----------

race_results.filter("driver_name= 'Lewis Hamilton'").select(spark_sum("points"),\
count("race_name")).withColumnRenamed('sum(points)','total_points')\
.withColumnRenamed('count(DISTINCT race_name)','number_of_races').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### For driver Max Verstappen we will take count of different races and sum the points

# COMMAND ----------

race_results.filter("driver_name= 'Max Verstappen'").select(spark_sum("points"),\
count("race_name")).withColumnRenamed('sum(points)','total_points')\
.withColumnRenamed('count(DISTINCT race_name)','number_of_races').show()

# COMMAND ----------

race_results.groupBy("driver_name").sum("points").withColumnRenamed('sum(points)','total_points').show()

# COMMAND ----------

race_results.groupBy("driver_name").avg("points").withColumnRenamed('sum(points)','total_points').show()

# COMMAND ----------

race_results\
    .groupBy("driver_name")\
        .agg(spark_sum("points").alias("total_points"),
             count("race_name").alias("number_of_races")).show()
race_results\
    .groupBy("driver_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window functions

# COMMAND ----------

demo_grouped_df = race_results\
    .groupBy("race_year","driver_name")\
        .agg(spark_sum("points").alias("total_points"),
             count("race_name").alias("number_of_races"))
demo_grouped_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank = Window.partitionBy("race_year").orderBy(desc("total_points"))
d_rank = demo_grouped_df.withColumn("rank",rank().over(driver_rank))
d_rank.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using positions to assign rank based on total points and number of wins

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

display(race_results)

# COMMAND ----------

driver_standing_df = race_results\
    .groupBy("race_year","driver_name","driver_nationality")\
        .agg(spark_sum("points").alias("total_points"),
             count(when(col("position")==1, True)).alias("wins"))
driver_standing_df.show()

# COMMAND ----------

display(driver_standing_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))
final_df.show()


# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wrting the file in parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/blobstorage/drivers_championship_standing")