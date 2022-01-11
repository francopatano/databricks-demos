# Databricks notebook source
## This is where we put the configurations for the benchmark run, should be invoked at the beginning of the run if called from main notebook, or spawned before any job to set configurations 


# COMMAND ----------

datagen_location = '' # where the datagen tools zip is located.  Look at the prep notebook if you need this
files_directory = '' # tells 001 generate data where to put the files 
staging_database = '' # name of the database for the staging tables in the metastore
warehouse_database = '' # name of the warehouse database in the metastore
database_location = '' # set to specify an object storage directory where to store the database files TODO: need to adjust the create table LOCATION param to enable this




