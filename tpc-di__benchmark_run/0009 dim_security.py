# Databricks notebook source
dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')

staging_database = dbutils.widgets.get("staging_database")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------



# COMMAND ----------

SOURCE_DATABASE = staging_database
SOURCE_TABLE = "finwire_sec"
SOURCE_TABLE_STATUS = "ref_status_type"

SOURCE_DATABASE_GOLD = warehouse_database
SOURCE_TABLE_GOLD = "dim_company"
TARGET_DATABASE = warehouse_database
TARGET_TABLE = "dim_security"


BATCH_ID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function isnumeric (p_string string)
# MAGIC returns boolean
# MAGIC return ((cast(p_string as float) is not null));

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""

create or replace temp view dimsecurity_source_view
as

with finwiresec as (

select *, 
case isnumeric(conameorcik) when false then conameorcik when true then NULL end coname ,
case isnumeric(conameorcik) when false then null when true then conameorcik end cik,
isnumeric(conameorcik) isnumeric_conameorcik
from {SOURCE_DATABASE}.{SOURCE_TABLE}
)

select
  fw.Symbol as symbol,
  IssueType as issue,
  s.ST_NAME as status,
  fw.Name as name,
  fw.ExID as exchangeid,
  c.sk_company_id sk_companyid,
  fw.ShOut as sharesoutstanding,
  fw.FirstTradeDate as firsttrade,
  fw.FirstTradeExchg as firsttradeonexchange,
  fw.Dividend dividend,
  case when lead(PTS) OVER (PARTITION BY Symbol ORDER BY PTS ) is null then true else false end iscurrent,
  {BATCH_ID} batchid,
  date(PTS)  effectivedate,
  coalesce( date_add(lead(date(PTS)) OVER (PARTITION BY Symbol ORDER BY PTS ),-1)  ,date('9999-12-31')) enddate

from
  finwiresec fw
  inner join {SOURCE_DATABASE_GOLD}.{SOURCE_TABLE_GOLD} c 
    on fw.coname = c.name or fw.cik = c.companyid
  left join {BRONZE_DATABASE}.{SOURCE_TABLE_STATUS} s on s.ST_ID = fw.status

""")

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""

insert into {TARGET_DATABASE}.{TARGET_TABLE} (symbol,issue,status,name,exchangeid,sk_companyid,sharesoutstanding,firsttrade,firsttradeonexchange,dividend,iscurrent,batchid,effectivedate,enddate)
select symbol,issue,status,name,exchangeid,sk_companyid,sharesoutstanding,firsttrade,firsttradeonexchange,dividend,iscurrent,batchid,effectivedate,enddate from dimsecurity_source_view

""")


# COMMAND ----------


