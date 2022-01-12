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

files_directory = files_directory + 'Batch1/'

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

COPY INTO {warehouse_database}.dim_date
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
    _c15::STRING c_state_prov,
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
PATTERN = 'Date.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {warehouse_database}.dim_time
FROM (
  select _c0::BIGINT sk_timeid,
_c1::TIMESTAMP timevalue,
_c2::INT hourid,
_c3::STRING hourdesc,
_c4::INT minuteid,
_c5::STRING minutedesc,
_c6::INT secondid,
_c7::STRING seconddesc,
_c8::BOOLEAN markethoursflag,
_c9::BOOLEAN officehoursflag
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Date.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {warehouse_database}.ref_industry
FROM (
  select _c0::STRING in_id,
_c1::STRING in_name,
_c2::STRING in_sc_id
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Industry.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {warehouse_database}.ref_status_type
FROM (
  select _c0::STRING st_id,
_c1::STRING st_name
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'StatusType.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {warehouse_database}.ref_tax_rate
FROM (
  select _c0::STRING tx_id,
_c1::STRING tx_name,
_c2::INT tx_rate
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'TaxRate.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {warehouse_database}.ref_trade_type 
FROM (
  select _c0::STRING tt_id,
_c1::STRING tt_name,
_c2::INT tt_is_sell,
_c3::INT tt_is_mrkt
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'TradeType.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.trade_historical 
FROM (
  select _c0::BIGINT t_id,
_c1::TIMESTAMP t_dts,
_c2::STRING t_st_id,
_c3::STRING t_tt_id,
_c4::BOOLEAN t_is_cash,
_c5::STRING t_s_symb,
_c6::INT t_qty,
_c7::DOUBLE t_bid_price,
_c8::BIGINT t_ca_id,
_c9::STRING t_exec_name,
_c10::DOUBLE t_trade_price,
_c11::DOUBLE t_chrg,
_c12::DOUBLE t_comm,
_c13::DOUBLE t_tax
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'Trade.txt'
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

COPY INTO {staging_database}.cash_transaction_historical 
FROM (
  select _c0::BIGINT ct_ca_id,
_c1::TIMESTAMP ct_dts,
_c2::DOUBLE ct_amt,
_c3::STRING ct_name
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'CashTransaction.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.holding_history_historical (

FROM (
  select _c0::INT hh_h_t_id,
_c1::INT hh_t_id,
_c2::INT hh_before_qty,
_c3::INT hh_after_qty
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'HoldingHistory.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.daily_market_historical (

FROM (
  select _c0::TIMESTAMP dm_date,
_c1::STRING dm_s_symb,
_c2::DOUBLE dm_close,
_c3::DOUBLE dm_high,
_c4::DOUBLE dm_low,
_c5::INT dm_vol
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'DailyMarket.txt'
FORMAT_OPTIONS('sep' = '|')

""")




# COMMAND ----------

spark.sql(f"""

COPY INTO {staging_database}.watch_history_historical (

FROM (
  select _c0::BIGINT w_c_id,
_c1::STRING w_s_symb,
_c2::TIMESTAMP w_dts,
_c3::STRING w_action
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

spark.sql(f"""

COPY INTO {staging_database}.hr (

FROM (
  select _c0::BIGINT employeeid,
_c1::BIGINT managerid,
_c2::STRING employeefirstname,
_c3::STRING employeelastname,
_c4::STRING employeemi,
_c5::INT employeejobcode,
_c6::STRING employeebranch,
_c7::STRING employeeoffice,
_c8::STRING employeephone
  FROM '{files_directory}'
)
FILEFORMAT = CSV
PATTERN = 'HR.csv'
FORMAT_OPTIONS('sep' = ',')

""")




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

raw_finwire_df = spark.read.text(f'{files_directory}FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]')

# COMMAND ----------

finwire_record_types = {
  'CMP': """
PTS 15;
RecType 3;
CompanyName 60;
CIK 10;
Status 4;
IndustryID 2;
SPrating 4;
FoundingDate 8;
AddrLine1 80;
AddrLine2 80;
PostalCode 12;
City 25;
StateProvince 20;
Country 24;
CEOname 46;
Description 150
""",
  'SEC': """
PTS 15;
RecType 3;
Symbol 15;
IssueType 6;
Status 4;
Name 70;
ExID 6;
ShOut 13;
FirstTradeDate 8;
FirstTradeExchg 8;
Dividend 12;
CNameOrCIK 60
""",
  'FIN': """
PTS 15;
RecType 3;
Year 4;
Quarter 1;
QtrStartDate 8;
PostingDate 8;
Revenue 17;
Earnings 17
"""
}

# COMMAND ----------

from pyspark.sql.functions import substring

def finwire_parser(rec_type, df):
  
  column_list = []
  offset = 1
  # construct parser for given record type
  for r in finwire_record_types[rec_type].split(';'):
    col_info = r.strip().split()
    col_length = int(col_info[1])
    col_name = col_info[0]
    column_list.append(substring('value', offset, col_length).alias(col_name))
    offset += col_length
  # filter finwire records for given record type and parse row
  
  print(column_list)
  parsed_df = (
    df
    .filter(f"substr(value, 16, 3) = '{rec_type}'")
    .select(*column_list))
  
  return parsed_df

# COMMAND ----------

finwire_cmp_df = finwire_parser('CMP', raw_finwire_df)
finwire_sec_df = finwire_parser('SEC', raw_finwire_df)
finwire_fin_df = finwire_parser('FIN', raw_finwire_df)

# COMMAND ----------

finwire_cmp_df.createOrReplaceTempView("finwire_cmp_raw_view")
finwire_sec_df.createOrReplaceTempView("finwire_sec_raw_view")
finwire_fin_df.createOrReplaceTempView("finwire_fin_raw_view")

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""

INSERT INTO {staging_database}.finwire_cmp

SELECT * from finwire_cmp_raw_view;

""")

# COMMAND ----------

spark.sql(f"""

INSERT INTO {staging_database}.finwire_fin

SELECT * from finwire_fin_raw_view;

""")

# COMMAND ----------

spark.sql(f"""

INSERT INTO {staging_database}.finwire_sec

SELECT * from finwire_cmp_raw_view;

""")
