# Databricks notebook source
# MAGIC %md
# MAGIC **Paramerters**

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Fetching Parameters and Creting Variables **

# COMMAND ----------

# #Key column list
# key_cols= "['flight_id']"
# key_col_list= eval(key_cols)


# #CDC Column
# cdc_col= "modifiedDate"

# #Back-dated Refresh
# backdated_refresh= ""


# #Source Object
# source_object="silver_flights"

# #Sourece Schema
# source_schema="flight_raw"

# #target Object
# target_object="DimFlight"

# #Target Schema
# target_schema="gold"

# #Surrogate Key
# surrogate_key= "dimFlightsKey"


# # print(key_col_list)
# # eval(key_col_list)

# COMMAND ----------

# #Key column list
# key_cols= "['airport_id']"
# key_col_list= eval(key_cols)


# #CDC Column
# cdc_col= "modifiedDate"

# #Back-dated Refresh
# backdated_refresh= ""


# #Source Object
# source_object="silver_airports"

# #Sourece Schema
# source_schema="flight_raw"

# #target Object
# target_object="DimAirport"

# #Target Schema
# target_schema="gold"

# #Surrogate Key
# surrogate_key= "dimAirportsKey"


# # print(key_col_list)
# # eval(key_col_list)

# COMMAND ----------

#Key column list
key_cols= "['passenger_id']"
key_col_list= eval(key_cols)


#CDC Column
cdc_col= "modifiedDate"

#Back-dated Refresh
backdated_refresh= ""


#Source Object
source_object="silver_passengers"

#Sourece Schema
source_schema="flight_raw"

#target Object
target_object="DimPassenger"

#Target Schema
target_schema="gold"

#Surrogate Key
surrogate_key= "dimPassengersKey"


# print(key_col_list)
# eval(key_col_list)

# COMMAND ----------

# #Key column list
# key_cols= "['booking_id']"
# key_col_list= eval(key_cols)


# #CDC Column
# cdc_col= "modifiedDate"

# #Back-dated Refresh
# backdated_refresh= ""


# #Source Object
# source_object="silver_bookings"

# #Sourece Schema
# source_schema="flight_raw"

# #target Object
# target_object="DimBooking"

# #Target Schema
# target_schema="gold"

# #Surrogate Key
# surrogate_key= "dimBookingssKey"


# # print(key_col_list)
# # eval(key_col_list)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### ** Last Loaded Data  **
# MAGIC

# COMMAND ----------

# if there is no value in backdated_refresh
if(len(backdated_refresh)==0):

# check if target table is exist or not
    if(spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}")):
        last_load= spark.sql(f"select max({cdc_col}) from workspace.{target_schema}.{target_object}").collect()[0][0]
# if target table is not exist
    else:
        last_load= "1900-01-01 00:00:00"
# if there is a value in backdated_refresh
else:
    last_load= backdated_refresh

##Test the last load datae 
last_load

# COMMAND ----------

# MAGIC %md
# MAGIC ### ** INCREMENTAL DATA INGESTION  **

# COMMAND ----------

df_src=spark.sql(f"SELECT * FROM workspace.{source_schema}.{source_object} WHERE  {cdc_col}>='{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ** OLD VS NEW Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### ** Key column string 
# MAGIC

# COMMAND ----------

if (spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}")):
  #key col string for Inceremental
  key_cols_string_incremental=', '.join(key_col_list)

  #If table exist then read the table
  df_trgt= spark.sql(f"select {key_cols_string_incremental},{surrogate_key}, create_date, update_date from workspace.{target_schema}.{target_object}")

 #If table not exist 
else :

#key col string for Intial
  key_cols_string_initial=[f"'' AS {i}" for i in key_col_list]
  key_cols_string_initial=', '.join(key_cols_string_initial)

  #Create new table
  df_trgt= spark.sql(f""" select {key_cols_string_initial},cast('0' AS bigint) As {surrogate_key}, cast('1900-01-01 00:00:00' AS timestamp) AS create_date, cast('1900-01-01 00:00:00' AS timestamp) AS update_date """)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Joining Source and Target Dataframe
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Join Condition**

# COMMAND ----------


Join_condition=' AND'.join( [f" src.{i}=trgt.{i}" for i in key_col_list])
print(Join_condition)

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trgt.createOrReplaceTempView("trgt")

result_df=spark.sql(f"""
          select src.*,
          trgt.{surrogate_key},
          trgt.create_date,
          trgt.update_date 
          from src
          left join trgt
          on {Join_condition}
          """)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

old_df= result_df.filter(col(f'{surrogate_key}').isNotNull())
new_df= result_df.filter(col(f'{surrogate_key}').isNull())


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preaparing DF_OLD**

# COMMAND ----------

df_old_enr= old_df.withColumn("update_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing New_DF**

# COMMAND ----------

if (spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}")):
  max_surrogate_key= spark.sql(f""" select max({surrogate_key}) from workspace.{target_schema}.{target_object}""").collect()[0][0]

  new_df_enr= new_df.withColumn(f"{surrogate_key}",lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
    .withColumn("create_date",current_timestamp())\
    .withColumn("update_date",current_timestamp())
else : 
  max_surrogate_key =0
  new_df_enr= new_df.withColumn(f"{surrogate_key}",lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
    .withColumn("create_date",current_timestamp())\
    .withColumn("update_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union in OLD and New Records**

# COMMAND ----------

df_union= df_old_enr.union(new_df_enr)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if ( spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}") ):
    #Update the data
   dlt_obj= DeltaTable.forName(spark,f"workspace.{target_schema}.{target_object}")

   dlt_obj.alias("trg").merge(df_union.alias("src"),f"src.{surrogate_key}=trg.{surrogate_key}")\
       .whenMatchedUpdateAll(condition=f"src.{cdc_col} >=trg.{cdc_col}")\
           .whenNotMatchedInsertAll()\
               .execute()
    

else :
    #Insert the data
    df_union.write.format("delta").mode("append").saveAsTable(f"workspace.{target_schema}.{target_object}")
    
