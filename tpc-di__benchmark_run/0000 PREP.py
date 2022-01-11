# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC This notebook uses TPC-DI data generator. The generator is a single node solution, you should use a single node type cluster. <br>
# MAGIC The generator has high Java memory needs, choose an adequate driver (the more data you generate the larger the drive) <br>
# MAGIC The generator can be found in http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp <br>
# MAGIC 
# MAGIC Download this file
# MAGIC Download TPC-DI_Tools_v1.1.0.zip
# MAGIC 
# MAGIC <br>
# MAGIC The notebook will execute the generator twice, the first time it will accept the license but fail. There may be a need to run this step multiple times<br>
# MAGIC The second cell will generate the data, set the _sf_ argument to the scale value, for test use cases you can set it to 20, for large volume tests do not exceed 10000<br>
# MAGIC <br>
# MAGIC Example cluster: <br>
# MAGIC   "num_workers": 0, <br>
# MAGIC   "cluster_name": "TPC-DI gen", <br>
# MAGIC   "spark_version": "9.1.x-scala2.12", <br>
# MAGIC   "spark_conf": { -- These will be set up for you using Single Node <br>
# MAGIC       "spark.master": "local[*]", <br>
# MAGIC       "spark.databricks.cluster.profile": "singleNode" <br>
# MAGIC   }, <br>
# MAGIC   "node_type_id": "r4.8xlarge", <br>
# MAGIC   "driver_node_type_id": "r4.8xlarge", <br>

# COMMAND ----------

# DBTITLE 1,Download the zip and copy to DBFS for permanent storage
# MAGIC %md
# MAGIC Fill up the form in the website http://tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY . Download the file locally.<br>
# MAGIC Upload the file to DBFS, use Databricks CLI or the Upload file option in the Data tab <br>
# MAGIC dbutils.fs.cp ("<your location>","/dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip")

# COMMAND ----------

# MAGIC %md 
# MAGIC ![Upload](https://github.com/francopatano/databricks-demos/blob/main/content/images/upload.png?raw=true)

# COMMAND ----------

# MAGIC %md 
# MAGIC ![Upload](https://github.com/francopatano/databricks-demos/blob/main/content/images/upload2.png?raw=true)
# MAGIC 
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Copy the file from File Store Shared Uploads to dbfs temp, or another location the clusters will have access to
dbutils.fs.cp ("dbfs:/FileStore/shared_uploads/aws-reinvent2021-demo@databricks.com/aada52c7_0b84_40f8_96d7_45faf21cb2b9_tpc_di_tool.zip","/dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip")

# COMMAND ----------

## "/dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use the directory of where the data generator is located in the config file for the variable datagen_location

# COMMAND ----------

## datagen_location = "/dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip"

