# Databricks notebook source

dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')


warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------

## TODO Batch Validation queries

## need to get from benchmark people 


# COMMAND ----------

#insert phase completion record into DI Messages
spark.sql(f"""
insert into
   {warehouse_database}.di_messages (message_date_and_time,batch_id,message_source,message_text,message_type,message_data)
    select current_timestamp(), 0, 'Batch1', 'Batch1 Complete', 'PCR', ''


""")
