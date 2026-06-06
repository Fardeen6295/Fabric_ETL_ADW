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
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Gold Dimension Builder
# Notebook is used to create gold dimension tables. \
# Dim Tables can be of SCD type 1 or Type 2 depending on requiremnet.

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **DimProduct**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'product'

target_schema = 'gold'

target_table = 'DimProduct'

cdc = 'ModifiedAt'

key_col = "['ProductID']"
key_col = eval(key_col)

surrogate_key = 'DimProductKey'

cols_to_look = "['ProductName', 'Color', 'SafetyStockLevel', 'ReOrderPoint', 'StandardCost', 'ListPrice', 'Size', 'Weight', 'ProductLine', 'Class', 'Style','DiscontinuedDate', 'ProductSubCategoryName', 'ProductCategoryName', 'ModelName']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
        last_load_date = spark.sql(f"""
            SELECT max({cdc}) FROM {target_schema}.{target_table}
        """).collect()[0][0]
    else:
        last_load_date = '1900-01-01'
else:
    last_load_date = backfill
last_load_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    key_col_comma = ", ".join(key_col)
    target_data = spark.sql(f"""
                        SELECT 
                            *
                        FROM {target_schema}.{target_table}
                        """)
else:
    key_col_as = ", ".join([f' " " AS {col}' for col in key_col])
    target_data = spark.sql(f"""
                        SELECT 
                            {key_col_as},
                            CAST('0' AS INT) AS {surrogate_key},
                            CAST('1900-01-01' AS TIMESTAMP) AS CreatedDate,
                            CAST('1900-01-01' AS TIMESTAMP) AS UpdatedDate
                        WHERE 1=0
                        """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

" AND ".join([f"trg.{col} = src.{col}" for col in key_col])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_data.createOrReplaceTempView("src")
target_data.createOrReplaceTempView("trg")
join_cond = " AND ".join([f"src.{col} = trg.{col}" for col in key_col])
trg_columns = ", ".join([f"trg.{col} AS {col}_trg" for col in cols_to_look])

all_data = spark.sql(f"""
                        SELECT 
                            src.*,
                            {trg_columns},
                            trg.{surrogate_key},
                            trg.CreatedDate AS CreatedDate_trg,
                            trg.UpdatedDate AS UpdatedDate_trg
                        FROM src LEFT JOIN trg 
                        ON {join_cond}
                        AND trg.IsCurrent = 'Y'
                    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# New Data that are not yet available in gold layer
src_col = [c for c in source_data.columns]
new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                    .select(src_col)\
                    .withColumn("MergeAction", lit("INSERT"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cols That have been changed/updated at source
cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

src_col = [c for c in source_data.columns]

changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                        .withColumn("MergeAction", lit("UPDATE"))

df_inserts_from_updates = changed_data.select(src_col)\
                                        .withColumn("MergeAction", lit("INSERT"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_inserts = new_data.unionByName(df_inserts_from_updates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
    max_surrogate_key = spark.sql(f"SELECT max({surrogate_key}) FROM {target_schema}.{target_table}").collect()[0][0]
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = all_inserts.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())
else:
    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = all_inserts.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())
max_surrogate_key

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_merge_input)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_merge_input.createOrReplaceTempView("src_view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

[c for c in df_merge_input.columns if c != "MergeAction"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    insert_cols = [c for c in df_merge_input.columns if c != "MergeAction"]
    cols_insert_str = ", ".join(insert_cols)
    cols_values_str = ", ".join([f"src.{c}" for c in insert_cols])
    spark.sql(f"""
        MERGE INTO {target_schema}.{target_table} as trg 
        USING src_view as src 
        ON trg.{surrogate_key} = (CASE WHEN src.MergeAction = 'UPDATE' THEN src.{surrogate_key} ELSE NULL END)
        WHEN MATCHED AND src.MergeAction = 'UPDATE'
        THEN UPDATE SET
            trg.IsCurrent = 'N',
            trg.EndDate = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT ({cols_insert_str})
            VALUES ({cols_values_str})
    """)

else:
    df_all_inserts_enr.write.format('delta')\
        .mode('append')\
        .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = spark.sql("SELECT * FROM gold.dimproduct WHERE ProductID IN (999, 1000)")
display(test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("UPDATE brz.product SET Name = 'Rooooooad-750 Black, 52' where ProductID = 999")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""INSERT INTO brz.product VALUES 
            (1000,
            'Brooooad-750 Black', 
            '52,BK-R19B-52',
            true,
            true,
            'Black',
            100,
            75,
            343.6496,
            539.9900,
            '52',
            'CM',
            'LB',
            20.42,
            4,
            'R',
            'L',
            'U',
            2,
            31,
            '2013-05-30 00:00:00',
            NULL,
            NULL,
            '2014-02-08 10:01:36.827')
        """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
