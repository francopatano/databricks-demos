# Databricks notebook source
dbutils.widgets.text("staging_database",'tpcdi_staging','Name of the staging database')
dbutils.widgets.text("warehouse_database",'tpcdi_warehouse','Name of the warehouse database')



# COMMAND ----------

staging_database = dbutils.widgets.get("staging_database")
warehouse_database = dbutils.widgets.get("warehouse_database")


# COMMAND ----------

# declare notebook scoped variables
SOURCE_DATABASE = staging_database
SOURCE_TABLE = "customermgmt_raw"
SOURCE_TABLE_TAX = "ref_tax_rate"
SOURCE_TABLE_PROSPECT = "prospect"

TARGET_DATABASE = warehouse_database
TARGET_TABLE = "dim_customer"

BATCH_ID = 1


# COMMAND ----------


spark.sql(f""" 
CREATE OR REPLACE TEMP VIEW source_view as 
select
  _ActionType, _ActionTS,
  C_ID as customerid,
  
  taxid,
  decode(_ActionType, 'NEW', 'ACTIVE' , 'UPDCUST', 'ACTIVE', 'INACT', "INACTIVE" ) as customerstatus,
  L_NAME as customerlastname,
  F_NAME as customerfirstname,
  M_NAME as customermiddleinitial,
  case
    upper(C_GNDR)
    when 'M' then 'M'
    when 'F' then 'F'
    else 'U'
  end as customergender,
  C_TIER as customertier,
  C_DOB as customerdob,
  ADLINE1 as customeraddressline1,
  ADLINE2 as customeraddressline2,
  ZIPCODE as customerpostalcode,
  c.CITY as customercity,
  STATE_PROV as customerstateprov,
  CTRY as customercountry,
    concat(
    if(
      isnull(C_PHONE_1__C_CTRY_CODE),
      '',
      '+' || C_PHONE_1__C_CTRY_CODE
    ),
    if(
      isnull(C_PHONE_1__C_AREA_CODE),
      '',
      '(' || C_PHONE_1__C_AREA_CODE || ')'
    ),
    C_PHONE_1__C_LOCAL,
    if(isnull(C_PHONE_1__C_EXT), '', C_PHONE_1__C_EXT)
  ) as customerphone1,
  concat(
    if(
      isnull(C_PHONE_2__C_CTRY_CODE),
      '',
      '+' || C_PHONE_2__C_CTRY_CODE
    ),
    if(
      isnull(C_PHONE_2__C_AREA_CODE),
      '',
      '(' || C_PHONE_2__C_AREA_CODE || ')'
    ),
    C_PHONE_2__C_LOCAL,
    if(isnull(C_PHONE_2__C_EXT), '', C_PHONE_2__C_EXT)
  ) as customerphone2,
  concat(
  if(
      isnull(C_PHONE_3__C_CTRY_CODE),
      '',
      '+' || C_PHONE_3__C_CTRY_CODE
    ),
  if(
    isnull(C_PHONE_3__C_AREA_CODE),
    '',
    '(' || C_PHONE_3__C_AREA_CODE || ')'
  ),
  C_PHONE_3__C_LOCAL,
  if(isnull(C_PHONE_3__C_EXT), '', C_PHONE_3__C_EXT)
) as customerphone3,
  ALT_EMAIL as customeremail1,
  NULL as customeremail2,
  r_nat.TX_NAME as customernationaltaxratedesc,
  r_nat.TX_RATE as customernationaltaxrate,
  r_lcl.TX_NAME as customerlocaltaxratedesc,
  r_lcl.TX_RATE as customerlocaltaxrate,
  p.AgencyID as customeragencyid,
  p.CreditRating as customercreditrating,
  p.NetWorth as customernetworth,
  '' as customermarketingnameplate,
--     True is_current,
    
    {BATCH_ID} batch_id,
    date(_ActionTS) effective_date,
--     '9999-12-31' end_date,
    
    case when lead(date(_ActionTS)) OVER (PARTITION BY C_ID ORDER BY _ActionTS) is null then true else false end is_current,
    coalesce( date_add(lead(date(_ActionTS)) OVER (PARTITION BY C_ID ORDER BY _ActionTS),-1)  ,'9999-12-31') end_date
from
      (SELECT 
    _ActionTS,
    _ActionType,
    
    coalesce( Customer.Account._CA_TAX_ST, last_value(Customer.Account._CA_TAX_ST) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) taxid,
    
    coalesce( Customer.Address.C_ADLINE1, last_value(Customer.Address.C_ADLINE1) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) ADLINE1,
    coalesce( Customer.Address.C_ADLINE2, last_value(Customer.Address.C_ADLINE2) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) ADLINE2,
    coalesce( Customer.Address.C_CITY, last_value(Customer.Address.C_CITY) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) CITY,
    coalesce( Customer.Address.C_CTRY, last_value(Customer.Address.C_CTRY) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) CTRY,
    coalesce( Customer.Address.C_STATE_PROV, last_value(Customer.Address.C_STATE_PROV) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) STATE_PROV,
    coalesce( Customer.Address.C_ZIPCODE, last_value(Customer.Address.C_ZIPCODE) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) ZIPCODE,
    
    coalesce( nullif(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE,''), last_value(nullif(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_1__C_AREA_CODE,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE,''), last_value(nullif(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_1__C_CTRY_CODE,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_1.C_EXT,''), last_value(nullif(Customer.ContactInfo.C_PHONE_1.C_EXT,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_1__C_EXT,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_1.C_LOCAL,''), last_value(nullif(Customer.ContactInfo.C_PHONE_1.C_LOCAL,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_1__C_LOCAL,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE,''), last_value(nullif(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_2__C_AREA_CODE,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE,''), last_value(nullif(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_2__C_CTRY_CODE,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_2.C_EXT,''), last_value(nullif(Customer.ContactInfo.C_PHONE_2.C_EXT,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_2__C_EXT,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_2.C_LOCAL,''), last_value(nullif(Customer.ContactInfo.C_PHONE_2.C_LOCAL,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_2__C_LOCAL,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE,''), last_value(nullif(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_3__C_AREA_CODE,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE,''), last_value(nullif(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_3__C_CTRY_CODE,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_3.C_EXT,''), last_value(nullif(Customer.ContactInfo.C_PHONE_3.C_EXT,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_3__C_EXT,
    coalesce( nullif(Customer.ContactInfo.C_PHONE_3.C_LOCAL,''), last_value(nullif(Customer.ContactInfo.C_PHONE_3.C_LOCAL,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_PHONE_3__C_LOCAL,
    
    coalesce( nullif(Customer.ContactInfo.C_ALT_EMAIL,''), last_value(nullif(Customer.ContactInfo.C_ALT_EMAIL,'')) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) ALT_EMAIL,
    
    coalesce( Customer.Name.C_F_NAME, last_value(Customer.Name.C_F_NAME) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) F_NAME,
    coalesce( Customer.Name.C_L_NAME, last_value(Customer.Name.C_L_NAME) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) L_NAME,
    coalesce( Customer.Name.C_M_NAME, last_value(Customer.Name.C_M_NAME) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) M_NAME,
    
    coalesce( Customer.TaxInfo.C_LCL_TX_ID, last_value(Customer.TaxInfo.C_LCL_TX_ID) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) LCL_TX_ID,
    coalesce( Customer.TaxInfo.C_NAT_TX_ID, last_value(Customer.TaxInfo.C_NAT_TX_ID) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) NAT_TX_ID,
    
    coalesce( Customer._C_DOB, last_value(Customer._C_DOB) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_DOB,
    coalesce( Customer._C_GNDR, last_value(Customer._C_GNDR) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_GNDR,
    coalesce( Customer._C_ID, last_value(Customer._C_ID) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_ID,
    coalesce( Customer._C_TAX_ID, last_value(Customer._C_TAX_ID) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_TAX_ID,
    coalesce( Customer._C_TIER, last_value(Customer._C_TIER) IGNORE NULLS OVER (PARTITION BY Customer._C_ID ORDER BY _ActionTS )  ) C_TIER

    
    from 
    {SOURCE_DATABASE}.{SOURCE_TABLE} where Customer._C_ID is not null ) c
  left join {SOURCE_DATABASE}.{SOURCE_TABLE_TAX} r_lcl on c.LCL_TX_ID ilike r_lcl.TX_ID
  left join {SOURCE_DATABASE}.{SOURCE_TABLE_TAX} r_nat on c.NAT_TX_ID ilike r_nat.TX_ID
  left join {SOURCE_DATABASE}.{SOURCE_TABLE_PROSPECT} p on upper(p.LastName) ilike upper(c.L_NAME)
  and upper(p.FirstName) ilike upper(c.F_NAME)
  and upper(p.AddressLine1) ilike upper(c.ADLINE1)
  and upper(p.AddressLine2) ilike upper(c.ADLINE2)
  and upper(p.PostalCode) = upper(c.ZIPCODE)
where _ActionType in ('NEW','INACT','UPDCUST')


""")



# COMMAND ----------

spark.sql(f"""
insert into {TARGET_DATABASE}.{TARGET_TABLE} (customerid,taxid,customerstatus,customerlastname,customerfirstname,customermiddleinitial,customergender,customertier,customerdob,customeraddressline1,customeraddressline2,customerpostalcode,customercity,customerstateprov,customercountry,customerphone1,customerphone2,customerphone3,customeremail1,customeremail2,customernationaltaxratedesc,customernationaltaxrate,customerlocaltaxratedesc,customerlocaltaxrate,customeragencyid,customercreditrating,customernetworth,customermarketingnameplate,is_current,batch_id,effective_date,end_date)
select customerid,taxid,customerstatus,customerlastname,customerfirstname,customermiddleinitial,customergender,customertier,customerdob,customeraddressline1,customeraddressline2,customerpostalcode,customercity,customerstateprov,customercountry,customerphone1,customerphone2,customerphone3,customeremail1,customeremail2,customernationaltaxratedesc,customernationaltaxrate,customerlocaltaxratedesc,customerlocaltaxrate,customeragencyid,customercreditrating,customernetworth,customermarketingnameplate,is_current,batch_id,effective_date,end_date from source_view
""")

# COMMAND ----------


