# Databricks notebook source
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
CREATE OR REPLACE TABLE {warehouse_database}.ref_trade_type (
tt_id STRING COMMENT 'Trade type code',
tt_name STRING COMMENT 'Trade type description',
tt_is_sell INT COMMENT 'Flag indicating a sale',
tt_is_mrkt INT COMMENT 'Flag indicating a market order'
)
USING DELTA COMMENT 'trade type  table';

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

spark.sql(f"""
CREATE OR REPLACE TABLE {staging_database}.prospect_raw (
agencyid STRING COMMENT 'Unique identifier from agency',
lastname STRING COMMENT 'Last name',
firstname STRING COMMENT 'First name',
middleinitial STRING COMMENT 'Middle initial',
gender STRING COMMENT '‘M’ or ‘F’ or ‘U’',
addressline1 STRING COMMENT 'Postal address',
addressline2 STRING COMMENT 'Postal address',
postalcode STRING COMMENT 'Postal code',
city STRING COMMENT 'City',
state STRING COMMENT 'State or province',
country STRING COMMENT 'Postal country',
phone STRING COMMENT 'Telephone number',
income STRING COMMENT 'Annual income',
numbercars INT COMMENT 'Cars owned',
numberchildren INT COMMENT 'Dependent children',
maritalstatus STRING COMMENT '‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’',
age INT COMMENT 'Current age',
creditrating INT COMMENT 'Numeric rating',
ownorrentflag STRING COMMENT '‘O’ or ‘R’ or ‘U’',
employer STRING COMMENT 'Name of employer',
numbercreditcards INT COMMENT 'Credit cards',
networth INT COMMENT 'Estimated total net worth'
)
USING DELTA COMMENT 'prospect raw  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.prospect (
agencyid STRING COMMENT 'Unique identifier from agency',
sk_recorddateid STRING COMMENT 'Last date this prospect appeared in input',
sk_updatedateid STRING COMMENT 'Latest change date for this prospect',
batchid STRING COMMENT 'Batch ID when this record was last modified',
iscustomer STRING COMMENT 'True if this person is also in DimCustomer,else False',
lastname STRING COMMENT 'Last name',
firstname STRING COMMENT 'First name',
middleinitial STRING COMMENT 'Middle initial',
gender STRING COMMENT 'M / F / U',
addressline1 STRING COMMENT 'Postal address',
addressline2 STRING COMMENT 'Postal address',
postalcode STRING COMMENT 'Postal code',
city STRING COMMENT 'City',
state STRING COMMENT 'State or province',
country STRING COMMENT 'Postal country',
phone STRING COMMENT 'Telephone number',
income STRING COMMENT 'Annual income',
numbercars STRING COMMENT 'Cars owned',
numberchildren STRING COMMENT 'Dependent children',
maritalstatus STRING COMMENT 'S / M / D / W / U',
age STRING COMMENT 'Current age',
creditrating STRING COMMENT 'Numeric rating',
ownorrentflag STRING COMMENT 'O / R / U',
employer STRING COMMENT 'Name of employer',
numbercreditcards STRING COMMENT 'Credit cards',
networth STRING COMMENT 'Estimated total net worth',
marketingnameplate STRING COMMENT 'For marketing purposes'
)
USING DELTA COMMENT 'prospect processed  table';

""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {warehouse_database}.finwire_cmp (
    pts TIMESTAMP  COMMENT'Posting date & time as YYYYMMDD-HHMMSS',
    companyname STRING  COMMENT'Name of the company',
    cik STRING  COMMENT'Company identification code from SEC',
    status STRING  COMMENT'‘ACTV’ for Active company, ‘INAC’ for inactive',
    industryid STRING  COMMENT'Code for industry segment',
    sprating STRING  COMMENT'S&P rating',
    foundingdate DATE  COMMENT'A date as YYYYMMDD',
    addrline1 STRING  COMMENT'Mailing address',
    addrline2 STRING  COMMENT'Mailing address',
    postalcode STRING  COMMENT'Mailing address',
    city STRING  COMMENT'Mailing address',
    stateprovince STRING  COMMENT'Mailing address',
    country STRING  COMMENT'Mailing address',
    ceoname STRING  COMMENT'Name of company CEO',
    description STRING  COMMENT'Description of the company'
) USING DELTA COMMENT 'Company finwire table';
""")

# COMMAND ----------

spark.sql(f"""
create or replace table {warehouse_database}.finwire_fin (
    pts TIMESTAMP  COMMENT 'Posting date & time as YYYYMMDD-HHMMSS',
    year INTEGER  COMMENT 'Year of the quarter end.',
    quarter INTEGER  COMMENT 'Quarter number: valid values are ‘1’, ‘2’, ‘3’, ‘4’ Start date of quarter, as YYYYMMDD',
    qtrstartdate DATE  COMMENT 'Posting date of quarterly report as YYYYMMDD Reported revenue for the quarter',
    postingdate DATE  COMMENT 'Net earnings reported for the quarter',
    revenue NUMERIC(14, 2)  COMMENT 'Reported revenue for the quarter',
    earnings NUMERIC(14, 2)  COMMENT 'Net earnings reported for the quarter',
    eps NUMERIC(9, 2)  COMMENT 'Basic earnings per share for the quarter',
    dilutedeps NUMERIC(9, 2)  COMMENT 'Diluted earnings per share for the quarter',
    margin NUMERIC(9, 2)  COMMENT 'Profit divided by revenues for the quarter',
    inventory NUMERIC(14, 2)  COMMENT 'Value of inventory on hand at end of quarter',
    assets NUMERIC(14, 2)  COMMENT 'Value of total assets at the end of quarter',
    liabilities NUMERIC(14, 2)  COMMENT 'Value of total liabilities at the end of quarter',
    shout NUMERIC(13, 0)  COMMENT 'Average number of shares outstanding',
    dilutedshout NUMERIC(13, 0)  COMMENT 'Average number of shares outstanding (diluted)',
    conameorcik STRING  COMMENT 'Company CIK number (if only digits, 10 chars) or or 10) name (if not only digits, 60 chars)'
) USING DELTA COMMENT 'Finance silver table';
""")

# COMMAND ----------

spark.sql(f"""
create or replace table {warehouse_database}.finwire_sec (
    pts TIMESTAMP  COMMENT 'Posting date & time as YYYYMMDD-HHMMSS',
    symbol STRING  COMMENT'Security symbol',
    issuetype STRING  COMMENT'Issue type',
    status STRING  COMMENT'’ACTV’ for Active security, ‘INAC’ for inactive',
    name STRING  COMMENT'Security name',
    exid STRING  COMMENT'ID of the exchange the security is traded on',
    shout INTEGER  COMMENT'Number of shares outstanding',
    firsttradedate DATE  comment'Date of first trade as YYYYMMDD',
    firsttradeexchg DATE  COMMENT'Date of first trade on exchange as YYYYMMDD',
    dividend NUMERIC(10, 2)  COMMENT'Paid Dividend',
    conameorcik STRING  COMMENT'Company CIK number (if only digits, 10 chars) or name (if not only digits, 60 chars)'
) USING DELTA COMMENT 'Securities silver table';
""")

# COMMAND ----------

spark.sql(f"""
create or replace table {staging_database}.hr (
  employeeid BIGINT COMMENT 'ID of employee',
  managerid BIGINT COMMENT 'ID of employee’s manager',
  employeefirstname STRING COMMENT 'First name',
  employeelastname STRING COMMENT 'Last name',
  employeemi STRING COMMENT 'Middle initial',
  employeejobcode INT COMMENT 'Numeric job code',
  employeebranch STRING COMMENT 'Facility in which employee has office',
  employeeoffice STRING COMMENT 'Office number or description',
  employeephone STRING COMMENT 'Employee phone number'
) USING DELTA COMMENT 'HR';
""")

# COMMAND ----------

spark.sql(f"""
create or replace table {staging_database}.batch_date (
batchdate TIMESTAMP COMMENT 'Date of the data batch in the Staging Area'
) USING DELTA COMMENT 'Batch Date';
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE  {warehouse_database}.di_messages
(
  message_date_and_time TIMESTAMP  COMMENT "Date and time of the message"
  ,batch_id INT  COMMENT "DI run number; see the section “Overview of BatchID usage"
  ,message_source string  COMMENT "Typically the name of the transform that logs the message"
  ,message_text string  COMMENT "Description of why the message was logged"
  ,message_type string  COMMENT "“Status” or “Alert” or “Reject”"
  ,message_data string  COMMENT "Varies with the reason for logging the message"
)
USING DELTA COMMENT 'DI messages table';

""")

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""
insert into
   {warehouse_database}.di_messages (message_date_and_time,batch_id,message_source,message_text,message_type,message_data)
    select current_timestamp(), 0, 'Initialization', 'Initialization Complete', 'PCR', ''


""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### TODO: write custom messages to DIMessages for the Automate Price Performance calculation in the TPCDI Runbook
# MAGIC 
# MAGIC https://docs.google.com/document/d/1W2bo3OLjHbQnCZVwXccR1_7Z7oMB-ZjzaGtXblIoarI/edit#heading=h.8rwfk1cbpt3s

# COMMAND ----------


