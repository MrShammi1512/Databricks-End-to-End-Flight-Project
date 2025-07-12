# Databricks notebook source
# MAGIC %sql 
# MAGIC drop schema workspace.flight

# COMMAND ----------

 
dbutils.fs.mkdirs("/Volumes/workspace/flight_raw/rawvolume/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.flight_raw.silver_passengers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/flight_raw/bronze/airports/data`
