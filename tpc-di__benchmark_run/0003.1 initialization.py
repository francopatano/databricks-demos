# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## TODO: create a initialization version that creates table on top of the existing files, currently we are copying data from teh raw files into delta tables, then running the benchmark on that.  
# MAGIC 
# MAGIC This version would do the create table on top of the existing files in the datagen folder to compare benchmark runs to see which is more efficient 
# MAGIC 
# MAGIC The following cell is an exmaple of how to create table on top of the files 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE {staging_database}.watch_history_incremental (
# MAGIC cdc_flag STRING COMMENT 'Rows are only added',
# MAGIC cdc_dsn BIGINT COMMENT 'Database Sequence Number',
# MAGIC w_c_id BIGINT COMMENT 'Customer identifier',
# MAGIC w_s_symb STRING COMMENT 'Symbol of the security to watch',
# MAGIC w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action',
# MAGIC w_action STRING COMMENT 'Whether activating or canceling the watch'
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS('sep'='|')
# MAGIC LOCATION '/tmp/tpc-di/1000/Batch2/WatchHistory.txt'
# MAGIC COMMENT 'watch history incremental  table';

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')

# COMMAND ----------

staging_database = dbutils.widgets.get("staging_database")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.dim_date (
      sk_dateid BIGINT COMMENT 'Surrogate key for the date',
      datevalue TIMESTAMP COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse',
      datedesc STRING COMMENT 'The date in full written form, e.g. “July 7,2004”',
      calendaryearid INT COMMENT 'Year number as a number',
      calendaryeardesc STRING COMMENT 'Year number as text',
      calendarqtrid INT COMMENT 'Quarter as a number, e.g. 20042',
      calendarqtrdesc STRING COMMENT 'Quarter as text, e.g. “2004 Q2”',
      calendarmonthid INT COMMENT 'Month as a number, e.g. 20047',
      calendarmonthdesc STRING COMMENT 'Month as text, e.g. “2004 July”',
      calendarweekid INT COMMENT 'Week as a number, e.g. 200428',
      calendarweekdesc STRING COMMENT 'Week as text, e.g. “2004-W28”',
      dayofweeknum INT COMMENT 'Day of week as a number, e.g. 3',
      dayofweekdesc STRING COMMENT 'Day of week as text, e.g. “Wednesday”',
      fiscalyearid INT COMMENT 'Fiscal year as a number, e.g. 2005',
      fiscalyeardesc STRING COMMENT 'Fiscal year as text, e.g. “2005”',
      fiscalqtrid INT COMMENT 'Fiscal quarter as a number, e.g. 20051',
      fiscalqtrdesc STRING COMMENT 'Fiscal quarter as text, e.g. “2005 Q1”',
      holidayflag BOOLEAN COMMENT 'Indicates holidays'
)
USING DELTA COMMENT 'dim date table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.dim_time (
    sk_timeid BIGINT COMMENT 'Surrogate key for the time',
    timevalue TIMESTAMP COMMENT 'The time stored appropriately for doing',
    hourid INT COMMENT 'Hour number as a number, e.g. 01',
    hourdesc STRING COMMENT 'Hour number as text, e.g. “01”',
    minuteid INT COMMENT 'Minute as a number, e.g. 23',
    minutedesc STRING COMMENT 'Minute as text, e.g. “01:23”',
    secondid INT COMMENT 'Second as a number, e.g. 45',
    seconddesc STRING COMMENT 'Second as text, e.g. “01:23:45”',
    markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours',
    officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours'
)
USING DELTA COMMENT 'dim time table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.ref_industry (
in_id STRING COMMENT 'Industry code',
in_name STRING COMMENT 'Industry description',
in_sc_id STRING COMMENT 'Sector identifier'
)
USING DELTA COMMENT 'industry reference table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.ref_status_type (
st_id STRING COMMENT 'Status code',
st_name STRING COMMENT 'Status description'
)
USING DELTA COMMENT 'status type reference table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.ref_tax_rate (
tx_id STRING COMMENT 'Tax rate code',
tx_name STRING COMMENT 'Tax rate description',
tx_rate INT COMMENT 'Tax rate'
)
USING DELTA COMMENT 'tax rate reference table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.trade_historical (
t_id BIGINT COMMENT 'Trade identifier.',
t_dts TIMESTAMP COMMENT 'Date and time of trade.',
t_st_id STRING COMMENT 'Status type identifier',
t_tt_id STRING COMMENT 'Trade type identifier',
t_is_cash BOOLEAN COMMENT 'Is this trade a cash (‘1’) or margin (‘0’) trade?',
t_s_symb STRING COMMENT 'Security symbol of the security',
t_qty INT COMMENT 'Quantity of securities traded.',
t_bid_price DOUBLE COMMENT 'The requested unit price.',
t_ca_id BIGINT COMMENT 'Customer account identifier.',
t_exec_name STRING COMMENT 'Name of the person executing the trade.',
t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.',
t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.',
t_comm DOUBLE COMMENT 'Commission earned on this trade',
t_tax DOUBLE COMMENT 'Amount of tax due on this trade'
)
USING DELTA COMMENT 'historical trade  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.trade_incremental (
cdc_flag STRING COMMENT 'Denotes insert, update',
cdc_dsn BIGINT COMMENT 'Database Sequence Number',
t_id BIGINT COMMENT 'Trade identifier.',
t_dts TIMESTAMP COMMENT 'Date and time of trade.',
t_st_id STRING COMMENT 'Status type identifier',
t_tt_id STRING COMMENT 'Trade type identifier',
t_is_cash BOOLEAN COMMENT 'Is this trade a cash (‘1’) or margin (‘0’) trade?',
t_s_symb STRING COMMENT 'Security symbol of the security',
t_qty INT COMMENT 'Quantity of securities traded.',
t_bid_price DOUBLE COMMENT 'The requested unit price.',
t_ca_id BIGINT COMMENT 'Customer account identifier.',
t_exec_name STRING COMMENT 'Name of the person executing the trade.',
t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.',
t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.',
t_comm DOUBLE COMMENT 'Commission earned on this trade',
t_tax DOUBLE COMMENT 'Amount of tax due on this trade'
)
USING DELTA COMMENT 'incremental trade  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.trade_history (
th_t_id BIGINT COMMENT 'Trade identifier.  Corresponds to T_ID in the Trade.txt file',
th_dts TIMESTAMP COMMENT 'When the trade history was updated.',
th_st_id STRING COMMENT 'Status type identifier.'
)
USING DELTA COMMENT 'trade history  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.cash_transaction_historical (
ct_ca_id BIGINT COMMENT 'Customer account identifier',
ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place',
ct_amt DOUBLE COMMENT 'Amount of the cash transaction.',
ct_name STRING COMMENT 'Transaction name, or description: e.g. “Cash from sale of DuPont stock”.'
)
USING DELTA COMMENT 'cash transaction historical  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.cash_transaction_incremental (
cdc_flag STRING COMMENT 'Denotes insert',
cdc_dsn BIGINT COMMENT 'Database Sequence Number',
ct_ca_id BIGINT COMMENT 'Customer account identifier',
ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place',
ct_amt DOUBLE COMMENT 'Amount of the cash transaction.',
ct_name STRING COMMENT 'Transaction name, or description: e.g. “Cash from sale of DuPont stock”.'
)
USING DELTA COMMENT 'cash transaction incremental  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.holding_history_historical (
hh_h_t_id INT COMMENT 'Trade Identifier of the trade that originally created the holding row.',
hh_t_id INT COMMENT 'Trade Identifier of the current trade',
hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.',
hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.'
)
USING DELTA COMMENT 'holding history historical  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.holding_history_incremental (
cdc_flag STRING COMMENT 'Denotes insert',
cdc_dsn BIGINT COMMENT 'Database Sequence Number',
hh_h_t_id INT COMMENT 'Trade Identifier of the trade that originally created the holding row.',
hh_t_id INT COMMENT 'Trade Identifier of the current trade',
hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.',
hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.'
)
USING DELTA COMMENT 'holding history incremental  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.daily_market_historical (
dm_date TIMESTAMP COMMENT 'Date of last completed trading day.',
dm_s_symb STRING COMMENT 'Security symbol of the security',
dm_close DOUBLE COMMENT 'Closing price of the security on this day.',
dm_high DOUBLE COMMENT 'Highest price for the secuirity on this day.',
dm_low DOUBLE COMMENT 'Lowest price for the security on this day.',
dm_vol INT COMMENT 'Volume of the security on this day.'
)
USING DELTA COMMENT 'daily market historical  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.daily_market_incremental (
cdc_flag STRING COMMENT 'Denotes insert',
cdc_dsn BIGINT COMMENT 'Database Sequence Number',
dm_date TIMESTAMP COMMENT 'Date of last completed trading day.',
dm_s_symb STRING COMMENT 'Security symbol of the security',
dm_close DOUBLE COMMENT 'Closing price of the security on this day.',
dm_high DOUBLE COMMENT 'Highest price for the secuirity on this day.',
dm_low DOUBLE COMMENT 'Lowest price for the security on this day.',
dm_vol INT COMMENT 'Volume of the security on this day.'
)
USING DELTA COMMENT 'daily market incremental  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.watch_history_historical (
dm_date TIMESTAMP COMMENT 'Date of last completed trading day.',
dm_s_symb STRING COMMENT 'Security symbol of the security',
dm_close DOUBLE COMMENT 'Closing price of the security on this day.',
dm_high DOUBLE COMMENT 'Highest price for the secuirity on this day.',
dm_low DOUBLE COMMENT 'Lowest price for the security on this day.',
dm_vol INT COMMENT 'Volume of the security on this day.',
)
USING DELTA COMMENT 'watch history historical  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.watch_history_incremental (
cdc_flag STRING COMMENT 'Rows are only added',
cdc_dsn BIGINT COMMENT 'Database Sequence Number',
w_c_id BIGINT COMMENT 'Customer identifier',
w_s_symb STRING COMMENT 'Symbol of the security to watch',
w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action',
w_action STRING COMMENT 'Whether activating or canceling the watch'
)
USING DELTA COMMENT 'watch history incremental  table';

""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists {staging_database}.customermgmt_raw; 
# MAGIC 
# MAGIC create table elh10bronze.customermgmt_raw
# MAGIC (`Customer` STRUCT<`Account`: STRUCT<`CA_B_ID`: BIGINT, `CA_NAME`: STRING, `_CA_ID`: BIGINT, `_CA_TAX_ST`: BIGINT, `_VALUE`: STRING>, `Address`: STRUCT<`C_ADLINE1`: STRING, `C_ADLINE2`: STRING, `C_CITY`: STRING, `C_CTRY`: STRING, `C_STATE_PROV`: STRING, `C_ZIPCODE`: STRING>, `ContactInfo`: STRUCT<`C_ALT_EMAIL`: STRING, `C_PHONE_1`: STRUCT<`C_AREA_CODE`: BIGINT, `C_CTRY_CODE`: BIGINT, `C_EXT`: BIGINT, `C_LOCAL`: STRING>, `C_PHONE_2`: STRUCT<`C_AREA_CODE`: BIGINT, `C_CTRY_CODE`: BIGINT, `C_EXT`: BIGINT, `C_LOCAL`: STRING>, `C_PHONE_3`: STRUCT<`C_AREA_CODE`: BIGINT, `C_CTRY_CODE`: BIGINT, `C_EXT`: BIGINT, `C_LOCAL`: STRING>, `C_PRIM_EMAIL`: STRING>, `Name`: STRUCT<`C_F_NAME`: STRING, `C_L_NAME`: STRING, `C_M_NAME`: STRING>, `TaxInfo`: STRUCT<`C_LCL_TX_ID`: STRING, `C_NAT_TX_ID`: STRING>, `_C_DOB`: DATE, `_C_GNDR`: STRING, `_C_ID`: BIGINT, `_C_TAX_ID`: STRING, `_C_TIER`: BIGINT, `_VALUE`: STRING>,`_ActionTS` TIMESTAMP,`_ActionType` STRING)
# MAGIC using com.databricks.spark.xml
# MAGIC OPTIONS (path "dbfs:/tmp/tpc-di/3/Batch1/CustomerMgmt.xml", rowTag "TPCDI:Action")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /tmp/tpc-di/1000/Batch1/

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC head /tmp/tpc-di/1000/Batch2/WatchHistory.txt

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.watch_history_incremental (
cdc_flag STRING COMMENT 'Rows are only added',
cdc_dsn BIGINT COMMENT 'Database Sequence Number',
w_c_id BIGINT COMMENT 'Customer identifier',
w_s_symb STRING COMMENT 'Symbol of the security to watch',
w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action',
w_action STRING COMMENT 'Whether activating or canceling the watch'
)
USING DELTA COMMENT 'watch history incremental  table';

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE francos_dump.watch_history_incremental (
# MAGIC cdc_flag STRING COMMENT 'Rows are only added',
# MAGIC cdc_dsn BIGINT COMMENT 'Database Sequence Number',
# MAGIC w_c_id BIGINT COMMENT 'Customer identifier',
# MAGIC w_s_symb STRING COMMENT 'Symbol of the security to watch',
# MAGIC w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action',
# MAGIC w_action STRING COMMENT 'Whether activating or canceling the watch'
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS('sep'='|')
# MAGIC LOCATION '/tmp/tpc-di/1000/Batch2/WatchHistory.txt'
# MAGIC COMMENT 'watch history incremental  table';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from francos_dump.watch_history_incremental

# COMMAND ----------

{ { [CREATE OR] REPLACE TABLE | CREATE TABLE [ IF NOT EXISTS ] }
  table_name
  [ column_specification ] [ USING data_source ]
  [ table_clauses ]
  [ AS query ] }

column_sepcification
  ( { column_identifier column_type [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expr ) ] [ COMMENT column_comment ] } [, ...] )

table_clauses
  { OPTIONS ( { option_key [ = ] option_val } [, ...] ) |
    PARTITIONED BY clause |
    clustered_by_clause |
    LOCATION path |
    COMMENT table_comment |
    TBLPROPERTIES ( { property_key [ = ] property_val } [, ...] ) } [...]

clustered_by_clause
  { CLUSTERED BY ( cluster_column [, ...] )
    [ SORTED BY ( { sort_column [ ASC | DESC ] } [, ...] ) ]
    INTO num_buckets BUCKETS }
