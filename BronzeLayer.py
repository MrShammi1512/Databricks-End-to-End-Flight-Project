# Databricks notebook source
# MAGIC %md
# MAGIC **Incrementatl Data Ingestion**

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_val=dbutils.widgets.get("src")


# COMMAND ----------

# dbutils.jobs.taskValues.get(taskKey="Bronze",key="output_key")


# COMMAND ----------

df= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f"/Volumes/workspace/flight_raw/bronze/{src_val}/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode","rescue")\
                .load(f"/Volumes/workspace/flight_raw/rawvolume/{src_val}/")

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(once=True).option("checkpointLocation",f"/Volumes/workspace/flight_raw/bronze/{src_val}/checkpoint").option("path",f"/Volumes/workspace/flight_raw/bronze/{src_val}/data").start()

# COMMAND ----------

df= spark.read.format("delta")\
    .load(f"/Volumes/workspace/flight_raw/bronze/{src_val}/data")
print(df.count())
# df.show()
