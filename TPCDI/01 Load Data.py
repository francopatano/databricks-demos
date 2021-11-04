# Databricks notebook source


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![the struggle](https://github.com/francopatano/mixedmediafornotebooks/blob/master/thestruggle.jpg?raw=true)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![Lakehouse](https://github.com/francopatano/mixedmediafornotebooks/blob/master/Lakehouse%20Platform.jpg?raw=true)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![What it is](https://github.com/francopatano/mixedmediafornotebooks/blob/master/whatistpcdi.jpg?raw=true)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![tpcdi](https://github.com/francopatano/mixedmediafornotebooks/blob/master/mainconcepts.jpg?raw=true)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![whytpcdi](https://github.com/francopatano/mixedmediafornotebooks/blob/master/whytpcdi.jpg?raw=true)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC du -ch /dbfs/tmp/tpc-di/10000/ | grep total

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create database if not exists lakehouse_tpcdi_10000; 
# MAGIC set spark.databricks.delta.optimize.maxFileSize = 33554432; 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![howtoimplment](https://github.com/francopatano/mixedmediafornotebooks/blob/master/referencearchitecture.jpg?raw=true)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC du -ch /dbfs/tmp/tpc-di/10000/Batch*/CustomerMgmt.xml | grep total

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists lakehouse_tpcdi_10000.customermgmt_raw; 
# MAGIC 
# MAGIC create table lakehouse_tpcdi_10000.customermgmt_raw
# MAGIC using com.databricks.spark.xml
# MAGIC OPTIONS (path "dbfs:/tmp/tpc-di/10000/Batch1/CustomerMgmt.xml", rowTag "TPCDI:Action")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lakehouse_tpcdi_10000.customermgmt_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Create or replace table lakehouse_tpcdi_10000.customer_management
# MAGIC as 
# MAGIC select
# MAGIC   _ActionTS, _ActionType, 
# MAGIC   Customer.Account.CA_B_ID,
# MAGIC   Customer.Account.CA_NAME,
# MAGIC   Customer.Account._CA_ID,
# MAGIC   Customer.Account._CA_TAX_ST,
# MAGIC   Customer.Account._VALUE as Account_Value,
# MAGIC   Customer.Address.*,
# MAGIC   Customer.ContactInfo.C_PHONE_1.C_AREA_CODE as C_PHONE_1__C_AREA_CODE,
# MAGIC   Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE as C_PHONE_1__C_CTRY_CODE,
# MAGIC   Customer.ContactInfo.C_PHONE_1.C_EXT as C_PHONE_1__C_EXT,
# MAGIC   Customer.ContactInfo.C_PHONE_1.C_LOCAL as C_PHONE_1__C_LOCAL,
# MAGIC   Customer.ContactInfo.C_PHONE_2.C_AREA_CODE as C_PHONE_2__C_AREA_CODE,
# MAGIC   Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE as C_PHONE_2__C_CTRY_CODE ,
# MAGIC   Customer.ContactInfo.C_PHONE_2.C_EXT as C_PHONE_2__C_EXT ,
# MAGIC   Customer.ContactInfo.C_PHONE_2.C_LOCAL as C_PHONE_2__C_LOCAL ,
# MAGIC   Customer.ContactInfo.C_PHONE_3.C_AREA_CODE as C_PHONE_3__C_AREA_CODE ,
# MAGIC   Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE as C_PHONE_3__C_CTRY_CODE  ,
# MAGIC   Customer.ContactInfo.C_PHONE_3.C_EXT as C_PHONE_3__C_EXT  ,
# MAGIC   Customer.ContactInfo.C_PHONE_3.C_LOCAL as C_PHONE_3__C_LOCAL  ,
# MAGIC   Customer.Name.*,
# MAGIC   Customer.TaxInfo.*,
# MAGIC   Customer._C_DOB,
# MAGIC   Customer._C_GNDR,
# MAGIC   Customer._C_ID,
# MAGIC   Customer._C_TAX_ID,
# MAGIC   Customer._C_TIER,
# MAGIC   Customer._VALUE Customer_Value
# MAGIC from
# MAGIC   lakehouse_tpcdi_10000.customermgmt_raw
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lakehouse_tpcdi_10000.customer_management

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE lakehouse_tpcdi_10000.customer_management ZORDER BY CA_B_ID; 
# MAGIC ANALYZE TABLE lakehouse_tpcdi_10000.customer_management COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC du -ch /dbfs/tmp/tpc-di/10000/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4] | grep total

# COMMAND ----------

# MAGIC %sh head /dbfs/tmp/tpc-di/10000/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]

# COMMAND ----------

raw_finwire_df = spark.read.text('dbfs:/tmp/tpc-di/10000/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]')

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

finwire_cmp_df.createOrReplaceTempView("finwire_cmp")
finwire_sec_df.createOrReplaceTempView("finwire_sec")
finwire_fin_df.createOrReplaceTempView("finwire_fin")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table lakehouse_tpcdi_10000.finwire_cmp_bronze as select * from finwire_cmp;
# MAGIC create or replace table lakehouse_tpcdi_10000.finwire_sec_bronze as select * from finwire_sec;
# MAGIC create or replace table lakehouse_tpcdi_10000.finwire_fin_bronze as select * from finwire_cmp;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE lakehouse_tpcdi_10000.finwire_cmp_bronze ZORDER BY CIK; 
# MAGIC ANALYZE TABLE lakehouse_tpcdi_10000.finwire_cmp_bronze COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE lakehouse_tpcdi_10000.finwire_sec_bronze ZORDER BY CNameOrCIK; 
# MAGIC ANALYZE TABLE lakehouse_tpcdi_10000.finwire_sec_bronze COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE lakehouse_tpcdi_10000.finwire_fin_bronze ZORDER BY CIK; 
# MAGIC ANALYZE TABLE lakehouse_tpcdi_10000.finwire_fin_bronze COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC drop table if exists lakehouse_tpcdi_10000.prospect_bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE lakehouse_tpcdi_10000.prospect_bronze
# MAGIC ( AgencyID string COMMENT 'Unique identifier from agency', 
# MAGIC LastName string COMMENT 'Last name', 
# MAGIC FirstName string COMMENT 'First name', 
# MAGIC MiddleInitial string COMMENT 'Middle Initial', 
# MAGIC Gender string COMMENT 'M or F or U', 
# MAGIC AddressLine1 string COMMENT 'Postal address', 
# MAGIC AddressLine2 string COMMENT 'Postal address', 
# MAGIC PostalCode string COMMENT 'Postal Code', 
# MAGIC City string COMMENT 'City', 
# MAGIC State string COMMENT 'State or province', 
# MAGIC Country string COMMENT 'Postal country', 
# MAGIC Phone string COMMENT 'Telephone number', 
# MAGIC Income numeric COMMENT 'Annual income', 
# MAGIC NumberCars int COMMENT 'Cars owned', 
# MAGIC NumberChildren string COMMENT 'Dependent children', 
# MAGIC MaritalStatus string COMMENT 'S or M or D or W or U', 
# MAGIC Age string COMMENT 'Current Age', 
# MAGIC CreditRating string COMMENT 'Numeric rating', 
# MAGIC OwnOrRentFlag string COMMENT 'O or R or U', 
# MAGIC Employer string COMMENT 'Name of employer', 
# MAGIC NumberCreditCards string COMMENT 'Credit cards', 
# MAGIC NetWorth string comment 'Estimated total net worth' )

# COMMAND ----------

spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "false") \
  .option("sep", ",") \
  .load("dbfs:/tmp/tpc-di/10000/Batch1/Prospect.csv") \
  .createOrReplaceTempView("prospect_raw")

# COMMAND ----------

tableSchema = spark.sql(f"select * from lakehouse_tpcdi_10000.prospect_bronze limit 1").schema

# COMMAND ----------

spark.readStream.format("cloudFiles") \
.schema(tableSchema) \
.option("cloudFiles.inferColumnTypes", True) \
.option("cloudFiles.format", "CSV") \
.option("sep", ",") \
.load("dbfs:/tmp/tpc-di/10000/Batch*/Prospect.csv") \
.writeStream \
.trigger(once=True) \
.format("delta") \
.option("mergeSchema", "true") \
.option("checkpointLocation", "/dbfs/tmp/tpc-di/10000/checkpoints/prospect/4") \
.start("dbfs:/user/hive/warehouse/lakehouse_tpcdi_10000.db/prospect_bronze") 

# COMMAND ----------

df=spark.table("lakehouse_tpcdi_10000.prospect_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lakehouse_tpcdi_10000.prospect_bronze;

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l --block-size=M /dbfs/tmp/tpc-di/10000/Batch*/DailyMarket.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/tmp/tpc-di/10000/Batch*/DailyMarket.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC du -ch /dbfs/tmp/tpc-di/10000/Batch*/DailyMarket.txt | grep total

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC head /dbfs/tmp/tpc-di/10000/Batch1/DailyMarket.txt

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE lakehouse_tpcdi_10000.daily_market (
# MAGIC     dm_date DATE COMMENT 'Date of last completed trading day.',
# MAGIC     dm_s_symb string  COMMENT 'Security symbol of the security',
# MAGIC     dm_close NUMERIC(15,2)  COMMENT 'Closing price of the security on this day.',
# MAGIC     dm_high NUMERIC(15,2)  COMMENT 'Highest price for the secuirity on this day.',
# MAGIC     dm_low NUMERIC(15,2)  COMMENT 'Lowest price for the security on this day.',
# MAGIC     dm_vol NUMERIC(15,2)  COMMENT 'Volume of the security on this day.'
# MAGIC ) USING DELTA COMMENT 'daily market silver table';

# COMMAND ----------

tableSchema = spark.sql(f"select * from lakehouse_tpcdi_10000.daily_market limit 1").schema

# COMMAND ----------

spark.readStream.format("cloudFiles") \
.schema(tableSchema) \
.option("cloudFiles.schemaLocation","/dbfs/tmp/tpc-di/10000/checkpoints/schemas/dailymarket2") \
.option("cloudFiles.format", "CSV") \
.option("sep", "|") \
.load("dbfs:/tmp/tpc-di/10000/Batch*/DailyMarket.txt") \
.writeStream \
.trigger(once=True) \
.format("delta") \
.option("mergeSchema", "true") \
.option("checkpointLocation", "/dbfs/tmp/tpc-di/10000/checkpoints/dailymarket/2") \
.start("dbfs:/user/hive/warehouse/lakehouse_tpcdi_10000.db/daily_market") 

# COMMAND ----------

spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "false") \
  .option("sep", "|") \
  .load("dbfs:/tmp/tpc-di/10000/Batch1/DailyMarket.txt") \
  .createOrReplaceTempView("daily_market_raw")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from daily_market_raw

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC insert into lakehouse_tpcdi_10000.daily_market
# MAGIC select * from daily_market_raw

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC OPTIMIZE lakehouse_tpcdi_10000.daily_market ZORDER BY dm_date; 
# MAGIC ANALYZE TABLE lakehouse_tpcdi_10000.daily_market COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![howtoimplment](https://github.com/francopatano/mixedmediafornotebooks/blob/master/dbsqlunderthehood.jpg?raw=true)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![howtoimplment](https://github.com/francopatano/mixedmediafornotebooks/blob/master/cloudfetch.jpg?raw=true)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![howtoimplment](https://github.com/francopatano/mixedmediafornotebooks/blob/master/smallfileproblemsolved.jpg?raw=true)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![howtoimplment](https://github.com/francopatano/mixedmediafornotebooks/blob/master/highconcurrency.jpg?raw=true)
