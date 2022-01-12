# Databricks notebook source
dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')



# COMMAND ----------


staging_database = dbutils.widgets.get("staging_database")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------

# declare notebook scoped variables
SOURCE_DATABASE = staging_database
SOURCE_TABLE = "hr"
TARGET_DATABASE = warehouse_database
TARGET_TABLE = "dim_broker"

spark.sql(f""" 
Create or replace temp view broker_source_view as 
select employeeid brokerid, managerid managerid , employeefirstname firstname, employeelastname lastname, employeemi middleinitial, employeebranch branch, employeeoffice office, employeephone phone, '1/1/1983' effectivedate, '9999-12-31' enddate
from {SOURCE_DATABASE}.{SOURCE_TABLE} where employeejobcode = 314 
""")



# COMMAND ----------

spark.sql(f"""
INSERT INTO {TARGET_DATABASE}.{TARGET_TABLE} (brokerid, managerid, firstname, lastname, middleinitial, branch, office, phone, iscurrent, batchid, effectivedate, enddate)
SELECT brokerid, managerid, firstname, lastname, middleinitial, branch, office, phone, iscurrent, batchid, effectivedate, enddate FROM broker_source_view
""")
