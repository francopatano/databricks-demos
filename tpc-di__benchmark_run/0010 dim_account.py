# Databricks notebook source
dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')

staging_database = dbutils.widgets.get("staging_database")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# declare notebook scoped variables
SOURCE_DATABASE = staging_database
SOURCE_TABLE = "customermgmt_raw"
DIM_BROKER = "dim_broker"
DIM_CUST = "dim_customer"
TARGET_DATABASE = warehouse_database
TARGET_TABLE = "dim_account"

BATCH_ID = 1

spark.sql(f""" 
CREATE OR REPLACE TEMP VIEW source_view as 


select
  Customer.Account._CA_ID accountid,
  b.sk_broker_id sk_brokerid,
  d.sk_customerid sk_customerid,
  decode(_ActionType,"NEW","ACTIVE", "ADDACCT", "ACTIVE","UPDACCT", "ACTIVE","UPDCUST", "ACTIVE", "CLOSEACCT", "INACTIVE", "INACT","INACTIVE") status,
  
  coalesce( Customer.Account.CA_NAME, last_value(Customer.Account.CA_NAME) IGNORE NULLS OVER (PARTITION BY Customer.Account._CA_ID ORDER BY _ActionTS)  ) accountdesc,
  coalesce( Customer.Account._CA_TAX_ST, last_value(Customer.Account._CA_TAX_ST) IGNORE NULLS OVER (PARTITION BY Customer.Account._CA_ID ORDER BY _ActionTS)  ) taxstatus,
  case when lead(date(_ActionTS)) OVER (PARTITION BY Customer.Account._CA_ID ORDER BY _ActionTS) is null then true else false end is_current,
  
  {BATCH_ID} batch_id,
  '{BATCH_DATE}'  effective_date,
  coalesce( lead(date(_ActionTS)) OVER (PARTITION BY Customer.Account._CA_ID ORDER BY _ActionTS)  ,'9999-12-31') end_date,

  _ActionType,
  _ActionTS
  , row_number() OVER(PARTITION BY Customer.Account._CA_ID ORDER BY _ActionTS) rn


  from {SOURCE_DATABASE}.{SOURCE_TABLE} c
    left join {TARGET_DATABASE}.{DIM_BROKER} b on c.Customer.Account.CA_B_ID = b.broker_id and date(c._ActionTS) >= b.effective_date and date(c._ActionTS) <= b.end_date
    left join {TARGET_DATABASE}.{DIM_CUST} d on d.customerid = c.Customer.Account._CA_ID and date(c._ActionTS) >= d.effective_date and date(c._ActionTS) <= d.end_date
  
  where _ActionType in (
    "NEW",
    "ADDACCT",
    "UPDACCT",
    "CLOSEACCT",
    "UPDCUST",
    "INACT"
  )
""")



# COMMAND ----------

spark.sql(f"""
insert into {TARGET_DATABASE}.{TARGET_TABLE} (accountid,
  sk_brokerid,
  sk_customerid,
  status,
  accountdesc,
  taxstatus,
  is_current,
  batch_id,
  effective_date,
  end_date)
select accountid,
  sk_brokerid,
  sk_customerid,
  status,
  accountdesc,
  taxstatus,
  is_current,
  batch_id,
  effective_date,
  end_date from source_view
""")


