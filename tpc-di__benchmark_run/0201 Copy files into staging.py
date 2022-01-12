# Databricks notebook source
dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')
dbutils.widgets.text("files_directory", "/dbfs/tmp/tpc-di/1/", "Directory where Files are located")


# COMMAND ----------

staging_database = dbutils.widgets.get("staging_database")
files_directory = dbutils.widgets.get("files_directory")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------

# add historical load batch id into staging

files_directory = files_directory + 'Batch2/'

# COMMAND ----------

##  TODO: we went with underscore case in code, but the spec is written in camelCase.  We should refactor to get compliance with the spec.  need to change this and step 0003 Initialzation 


# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.batch_date (

FROM (
  select _c0::TIMESTAMP batchdate
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'BatchDate.csv'
FORMAT_OPTIONS('sep' = ',')

""")


# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.account
FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::BIGINT ca_id,
_c3::BIGINT ca_b_id,
_c4::BIGINT ca_c_id,
_c5::STRING ca_name,
_c6::INT ca_tax_st,
_c7::STRING ca_st_id
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Account.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.customer
FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::BIGINT c_id,
_c3::STRING c_tax_id,
_c4::STRING c_st_id,
_c5::STRING c_l_name,
_c6::STRING c_f_name,
_c7::STRING c_m_name,
_c8::STRING c_gndr,
_c9::INT c_tier,
_c10::TIMESTAMP c_dob,
_c11::STRING c_adline1,
_c12::STRING c_adline2,
_c13::STRING c_zipcode,
_c14::STRING c_city,
_c15::STRING c_state_pro,
_c16::STRING c_ctry,
_c17::STRING c_ctry_1,
_c18::STRING c_area_1,
_c19::STRING c_local_1,
_c20::STRING c_ext_1,
_c21::STRING c_ctry_2,
_c22::STRING c_area_2,
_c23::STRING c_local_2,
_c24::STRING c_ext_2,
_c25::STRING c_ctry_3,
_c26::STRING c_area_3,
_c27::STRING c_local_3,
_c28::STRING c_ext_3,
_c29::STRING c_email_1,
_c30::STRING c_email_2,
_c31::STRING c_lcl_tx_id,
_c32::STRING c_nat_tx_id
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Customer.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.cash_transaction_incremental 
FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::BIGINT ct_ca_id,
_c3::TIMESTAMP ct_dts,
_c4::DOUBLE ct_amt,
_c5::STRING ct_name
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'CashTransaction.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.trade_history 
FROM (
  select _c0::BIGINT th_t_id,
_c1::TIMESTAMP th_dts,
_c2::STRING th_st_id
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'TradeHistory.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.trade_incremental
FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::BIGINT t_id,
_c3::TIMESTAMP t_dts,
_c4::STRING t_st_id,
_c5::STRING t_tt_id,
_c6::BOOLEAN t_is_cash,
_c7::STRING t_s_symb,
_c8::INT t_qty,
_c9::DOUBLE t_bid_price,
_c10::BIGINT t_ca_id,
_c11::STRING t_exec_name,
_c12::DOUBLE t_trade_price,
_c13::DOUBLE t_chrg,
_c14::DOUBLE t_comm,
_c15::DOUBLE t_tax
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Trade.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.holding_history_incremental (

FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::INT hh_h_t_id,
_c3::INT hh_t_id,
_c4::INT hh_before_qty,
_c5::INT hh_after_qty
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'HoldingHistory.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.daily_market_incremental (

FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::TIMESTAMP dm_date,
_c3::STRING dm_s_symb,
_c4::DOUBLE dm_close,
_c5::DOUBLE dm_high,
_c6::DOUBLE dm_low,
_c7::INT dm_vol
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'DailyMarket.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.watch_history_incremental (

FROM (
  select _c0::STRING cdc_flag,
_c1::BIGINT cdc_dsn,
_c2::BIGINT w_c_id,
_c3::STRING w_s_symb,
_c4::TIMESTAMP w_dts,
_c5::STRING w_action
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'WatchHistory.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.prospect_raw (

FROM (
  select _c0::STRING agencyid,
_c1::STRING lastname,
_c2::STRING firstname,
_c3::STRING middleinitial,
_c4::STRING gender,
_c5::STRING addressline1,
_c6::STRING addressline2,
_c7::STRING postalcode,
_c8::STRING city,
_c9::STRING state,
_c10::STRING country,
_c11::STRING phone,
_c12::STRING income,
_c13::INT numbercars,
_c14::INT numberchildren,
_c15::STRING maritalstatus,
_c16::INT age,
_c17::INT creditrating,
_c18::STRING ownorrentflag,
_c19::STRING employer,
_c20::INT numbercreditcards,
_c21::INT networth
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Prospect.csv'
FORMAT_OPTIONS('sep' = ',')

""")




# COMMAND ----------


