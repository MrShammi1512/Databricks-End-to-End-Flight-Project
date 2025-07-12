# Databricks notebook source
# MAGIC %md
# MAGIC #### **Parameters **
# MAGIC

# COMMAND ----------

#catalog Name
catalog= "workspace"

#Source Scehma
source_schema="flight_raw"

#Source Object
source_object="silver_bookings"

#CDC COlumn
cdc_column="modifiedDate"

#BackDated Refresh
backdated_refresh=""

#SOurce Fact Table
fact_table=f"{catalog}.{source_schema}.{source_object}"

#Target Schema
target_schema="gold"

#Target Object
target_object="FactBookings"

#Fact Key ccols list
fact_key_cols = ["DimPassengersKey","DimAirportsKey","DimFlightsKey","booking_date"]

# COMMAND ----------

dimensions =[
    {
       "table" : f"{catalog}.{target_schema}.DimPassenger",
       "alias" : " DimPassenger",
       "tableKey": "DimPassengers",
       "join_keys": [("passenger_id","passenger_id")] # (fact col, dim col)
    },
    {
        "table" : f"{catalog}.{target_schema}.DimFlight",
       "alias" : " DimFlight",
       "tableKey": "DimFlights",
       "join_keys": [("flight_id","flight_id")] # (fact col, dim col)
    },
     {
        "table" : f"{catalog}.{target_schema}.DimAirport",
       "alias" : " DimAirport",
       "tableKey": "DimAirports",
       "join_keys": [("airport_id","airport_id")] # (fact col, dim col)
    },
    
]
fact_columns =["amount","booking_date","modifiedDate"]


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Last Load Date**

# COMMAND ----------

# if there is no value in backdated_refresh
if(len(backdated_refresh)==0):

# check if target table is exist or not
    if(spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}")):
        last_load= spark.sql(f"select max({cdc_column}) from workspace.{target_schema}.{target_object}").collect()[0][0]
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
# MAGIC ### **Dynamic Fact query Generation**

# COMMAND ----------

def generate_fact_query_incremental(fact_table,dimensions,fact_columns, cdc_column, processing_date):
    fact_alias ="f"

    #Base columns to select
    select_cols =[f"{fact_alias}.{col}" for col in fact_columns]

    #Builds Joins dynamically
    join_clauses =[]
    for dim in dimensions :
        table_full= dim["table"]
        alias= dim["alias"]
        table_key = dim["tableKey"]
        table_name= table_full.split('.')[-1]
        surrogate_key=f"{alias}.{table_key}Key"
        select_cols.append(surrogate_key)

        #Build ON clauses
        on_conditions =[
            f"{fact_alias}.{fk}={alias}.{dk}" for fk, dk,in dim["join_keys"]

        ]
        join_clause=f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_conditions)
        join_clauses.append(join_clause)

    #Final select and join clauses
    select_clause =",\n   ".join(select_cols)
    joins = "\n".join(join_clauses)

    #Where clause for Incremental filtering
    where_clause = f"{fact_alias}.{cdc_column} >= DATE('{last_load}')"

    #Final query
    query = f"""
    SELECT
    {select_clause}
    FROM { fact_table} {fact_alias}
    {joins}
    WHERE {where_clause}
    """.strip()

    return query

    

# COMMAND ----------

query = generate_fact_query_incremental(fact_table,dimensions,fact_columns, cdc_column, last_load)


# COMMAND ----------

# MAGIC %md
# MAGIC # **DB FACT**

# COMMAND ----------

d_fact= spark.sql(query)
df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **UPSERT**

# COMMAND ----------

#Fact key cols conditions
fact_key_cols_str= " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable
if ( spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}") ):
    #Update the data
   dlt_obj= DeltaTable.forName(spark,f"workspace.{target_schema}.{target_object}")

   dlt_obj.alias("trg").merge(df_fact.alias("src"),fact_key_cols_str)\
       .whenMatchedUpdateAll(condition=f"src.{cdc_column} >=trg.{cdc_column}")\
           .whenNotMatchedInsertAll()\
               .execute()
    

else :
    #Insert the data
    df_fact.write.format("delta").mode("append").saveAsTable(f"workspace.{target_schema}.{target_object}")
    

# COMMAND ----------

from pyspark.sql.functions import *
df= spark.sql("select * from workspace.gold.dimairport"). groupBy("DimAirportsKey").count().filter(col("count")>1)
df.display()
