# Databricks notebook source
dbutils.widgets.dropdown('Scale_Factor', '10', ['1','10','100','1000','10000'],'Choose Scale Factor')

# COMMAND ----------

GENERATOR_SCALE_FACTOR = dbutils.widgets.get("Scale_Factor")

# COMMAND ----------

# DBTITLE 1,About
# MAGIC %md
# MAGIC This notebook uses TPC-DI data generator. The generator is a single node solution, you should use a single node type cluster. <br>
# MAGIC The generator has high Java memory needs, choose an adequate driver (the more data you generate the larger the drive) <br>
# MAGIC The generator can be found in http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp <br>
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

# DBTITLE 1,Extra libraries
import os
from pathlib import Path

# COMMAND ----------

# DBTITLE 1,Download the zip and copy to DBFS for permanent storage
# MAGIC %md
# MAGIC Fill up the form in the website. Download the file locally.<br>
# MAGIC Upload the file to DBFS, use Databricks CLI or the Upload file option in the Data tab <br>
# MAGIC dbutils.fs.cp ("<your location>","/dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip")

# COMMAND ----------

# DBTITLE 1,Verify file exists on DBFS
if not Path("/dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip").is_file():
  raise FileNotFoundError(errno.ENOENT, "TPCDI Generator not found at", filename)


# COMMAND ----------

# DBTITLE 1,Unzip and prep to run
# MAGIC %sh 
# MAGIC rm -rf /tmp/tpcdigen
# MAGIC cp /dbfs/tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip /tmp
# MAGIC unzip /tmp/b4b051ef_a906_426d_9502_dde338910e5a_tpc_di_tool.zip -d /tmp/tpcdigen
# MAGIC mv /tmp/tpcdigen/Tools/PDGF /tmp/tpcdigen/Tools/pdgf

# COMMAND ----------

# DBTITLE 1,Accept the license - but fail. We will run the real generator in the next cell
# MAGIC %sh
# MAGIC mkdir -p /tmp/tpcdi/
# MAGIC cd /tmp/tpcdigen/Tools
# MAGIC echo "" > /tmp/response.txt
# MAGIC echo "YES" >> /tmp/response.txt
# MAGIC cat /tmp/response.txt | java -jar DIGen.jar -o "/tmp/tpcdi/5" -sf 5
# MAGIC rm -rf /tmp/tpcdi/5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the generator
# MAGIC 
# MAGIC `%sh` magic command does not allow substitution so using a small trick to enable setting the Scale Factor

# COMMAND ----------

# DBTITLE 1,Generate the script
GENERATOR_SCRIPT_PATH = '/tmp/generator.sh'
try: 
  os.remove(GENERATOR_SCRIPT_PATH) 
except: 
  pass

scale_factor = GENERATOR_SCALE_FACTOR
print(f'Using scale factor {scale_factor}')

generator_target_path = "/dbfs"+ f"/tmp/tpc-di/{scale_factor}/"

gen_script = f'''
if [ -d {generator_target_path} ]; then rm -Rf {generator_target_path}; fi
mkdir -p {generator_target_path}
cd /tmp/tpcdigen/Tools
java -jar DIGen.jar -o "{generator_target_path}" -sf {scale_factor}
'''

with open(GENERATOR_SCRIPT_PATH, 'w') as out:
  out.write(gen_script)
  
print(f'***Run the next cell to generate data with Scale Factor={scale_factor} in {generator_target_path}')

# COMMAND ----------

# DBTITLE 1,Run the created generator script
# MAGIC %sh source /tmp/generator.sh

# COMMAND ----------

# DBTITLE 1,Verify results
# `generator_target_path` is a local path, string starting `/dbfs` from it
dbutils.fs.ls(generator_target_path[len('/dbfs'):])

# COMMAND ----------

# DBTITLE 1,For info purposes only - uncomment and run if need to use custom location and Scale Factor
#%sh
# if [ -d /dbfs/tmp/tpcdi/20 ]; then rm -Rf /dbfs/tmp/tpcdi/20; fi
# mkdir -p /dbfs/tmp/tpcdi/20
# cd /tmp/tpcdigen/Tools
# java -jar DIGen.jar -o "/dbfs/tmp/tpcdi/20" -sf 20
