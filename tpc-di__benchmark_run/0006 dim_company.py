# Databricks notebook source
dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')


# COMMAND ----------


staging_database = dbutils.widgets.get("staging_database")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------


# declare notebook scoped variables
SOURCE_DATABASE = staging_database
SOURCE_TABLE = "finwire_cmp"

# TODO: #need to figure out the convention for adding multiple source tables
# -- Add this to contributing docs in Github to make sure we follow a convention
SOURCE_TABLE1 = "ref_industry"
SOURCE_TABLE2 = "ref_status_type"

TARGET_DATABASE = warehouse_database
TARGET_TABLE = "dim_company"

BATCHID = 1

# COMMAND ----------

spark.sql(f"""

create or replace temp view dim_company_source_view as 

select 
cik companyid,
st.st_name status,
companyname name,
companyname,
ind.in_name industry,
sprating sprating,
case when left(sprating,1) ilike "A" then false when sprating ilike "BBB" then false else true end as islowgrade,
ceoname ceo,
addrline1 addressline1,
addrline2 addressline2,
postalcode postalcode,
city city,
stateprovince stateprov,
country country,
description description,
foundingdate foundingdate,
case when lead(pts) OVER (PARTITION BY cik ORDER BY pts ) is null then true else false end iscurrent,
{BATCHID} batchid,
make_timestamp(substr(pts from 1 for 4),substr(pts from 5 for 2),substr(pts from 7 for 2),substr(pts from 10 for 2),substr(pts from 12 for 2),substr(pts from 14 for 2))  effectivedate,
coalesce( date_add(lead(date(make_timestamp(substr(pts from 1 for 4),substr(pts from 5 for 2),substr(pts from 7 for 2),substr(pts from 10 for 2),substr(pts from 12 for 2),substr(pts from 14 for 2)))) OVER (PARTITION BY cik ORDER BY make_timestamp(substr(pts from 1 for 4),substr(pts from 5 for 2),substr(pts from 7 for 2),substr(pts from 10 for 2),substr(pts from 12 for 2),substr(pts from 14 for 2)) desc),-1)  ,'9999-12-31') enddate

 from {SOURCE_DATABASE}.{SOURCE_TABLE} cmp
  left join {SOURCE_DATABASE}.{SOURCE_TABLE2} st on cmp.status = st.st_id 
  left join {SOURCE_DATABASE}.{SOURCE_TABLE1} ind on cmp.industryid = ind.in_id
   

""")

# COMMAND ----------

spark.sql(f"""

INSERT INTO {TARGET_DATABASE}.{TARGET_TABLE} (companyid, status, name, industry, sprating, islowgrade, ceo, addressline1, addressline2, postalcode, city, stateprov, country, description, foundingdate, iscurrent, batchid, effectivedate, enddate)

select companyid, status, name, industry, sprating, islowgrade, ceo, addressline1, addressline2, postalcode, city, stateprov, country, description, foundingdate, iscurrent, batchid, effectivedate, enddate from dim_company_source_view

""")
