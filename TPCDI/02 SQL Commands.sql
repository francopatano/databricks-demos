-- Databricks notebook source
select 'finwire_cmp_bronze' as table ,count(*) as row_count from lakehouse_tpcdi_bronze.finwire_cmp_bronze union
select 'finwire_fin_bronze',count(*) from lakehouse_tpcdi_bronze.finwire_fin_bronze union
select 'finwire_sec_bronze', count(*) from lakehouse_tpcdi_bronze.finwire_sec_bronze union
select 'prospect_bronze', count(*) from lakehouse_tpcdi_bronze.prospect_bronze union
select 'daily_market', count(*) from lakehouse_tpcdi_bronze.daily_market union
select 'monthly_market', count(*) from lakehouse_tpcdi_bronze.monthly_market union
select 'customer_management', count(*) from lakehouse_tpcdi_bronze.customer_management 


-- COMMAND ----------


select
  cmp.StateProvince, cmp.IndustryID, fin.SPrating, fin.Status, sec.ExID, sec.Symbol, sec.Dividend
from
  lakehouse_tpcdi_bronze.finwire_fin_bronze fin
  inner join lakehouse_tpcdi_bronze.finwire_sec_bronze sec on fin.CIK = sec.CNameOrCIK
  inner join lakehouse_tpcdi_bronze.finwire_cmp_bronze cmp on cmp.CIK = fin.CIK 
  

-- COMMAND ----------

select
  *
from
  lakehouse_tpcdi_bronze.customer_management c
  inner join lakehouse_tpcdi_bronze.prospect_bronze p on lower(c.C_F_NAME) = lower(p.FirstName)
  and lower(c.C_L_NAME) = lower(p.LastName)
  and lower(c.C_ZIPCODE) = lower(p.PostalCode)
  and lower(c.C_ADLINE1) = lower(p.AddressLine1)
  and lower(c.C_ADLINE2) = lower(p.AddressLine2)
  and lower(c.C_CITY) = lower(p.City)
  

-- COMMAND ----------


select 
avg(dm.dm_close) avg_close, avg(dm.dm_vol) as avg_vol , sec.ExID as exchange_id
from lakehouse_tpcdi_bronze.daily_market dm 
    inner join lakehouse_tpcdi_bronze.finwire_sec_bronze sec on sec.Symbol = dm.dm_s_symb
group by sec.ExID 

