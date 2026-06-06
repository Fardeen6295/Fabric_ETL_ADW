# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9eb80d3a-1646-4f9f-9c2a-f98f4aa0f858",
# META       "default_lakehouse_name": "AdvWrk_LKH",
# META       "default_lakehouse_workspace_id": "39569afe-3f7e-4b65-8e1e-0e502ac5b583",
# META       "known_lakehouses": [
# META         {
# META           "id": "9eb80d3a-1646-4f9f-9c2a-f98f4aa0f858"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

import pandas as pd
import os
import shutil

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SHOW TABLES IN gold")
gold_tables_list = [row.tableName for row in df.collect()]
print(gold_tables_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def export_data(table_name):
    df = spark.read.table(f"gold.{table_name}")

    df.write.format('csv').mode('overwrite').save(f'Files/gold/{table_name}')

    print(f"{table_name} successfully converted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in gold_tables_list:
    export_data(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(os.listdir('/lakehouse/default/Files/'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folder_to_zip = '/lakehouse/default/Files/gold'

output_filename = '/lakehouse/default/Files/gold_Exported_zipfile'

shutil.make_archive(output_filename, 'zip', folder_to_zip)

print("Folder successfully zipped! You can now download Gold_Export.zip")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
