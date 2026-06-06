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

# #### **Dim Product**

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

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim SalesPerson**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'salesperson'

target_schema = 'gold'

target_table = 'DimSalesPerson'

cdc = 'ModifiedAt'

key_col = "['SalesPersonID']"
key_col = eval(key_col)

surrogate_key = 'DimSalesPersonKey'

cols_to_look = "['QuarterlyTarget', 'Bonus', 'CommissionPct', 'Title', 'FirstName', 'MiddleName', 'LastName', 'EmailPromotion', 'AddressLine1', 'City', 'PostalCode', 'StateProvince', 'HMTerritoryName', 'HMTerritoryGroup', 'SPTerritoryName', 'SPTerritoryGroup', 'PhoneNumber', 'PhoneNumberType']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'SalesPersonID')


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'SalesPersonID')

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim Employee**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'employee'

target_schema = 'gold'

target_table = 'DimEmployee'

cdc = 'ModifiedAt'

key_col = "['EmployeeID']"
key_col = eval(key_col)

surrogate_key = 'DimEmployeeKey'

cols_to_look = "['LoginID', 'OrganizationNode', 'OrganizationLevel', 'JobTitle', 'BirthDate', 'MaritalStatus', 'HireDate', 'SalariedFlag', 'VacationHours', 'SickLeaveHours', 'CurrentFlag', 'DepartmentName', 'GroupName', 'Shift', 'ShiftStart', 'ShiftEnd', 'HourlyPayRate', 'PayFrequency', 'Title', 'FirstName', 'MiddleName', 'LastName', 'EmailPromotion', 'AddressLine1', 'City', 'PostalCode', 'StateProvince', 'TerritoryName', 'TerritoryGroup', 'PhoneNumber', 'PhoneNumberType']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'EmployeeID')


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'EmployeeID')

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim ProductVendor**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'productvendor'

target_schema = 'gold'

target_table = 'DimProductVendor'

cdc = 'ModifiedAt'

key_col = "['ProductID', 'VendorID']"
key_col = eval(key_col)

surrogate_key = 'DimProductVendorKey'

cols_to_look = "['AverageLeadTime', 'StandardPrice', 'LastReceiptCost', 'LastReceiptDate', 'MinOrderQty', 'MaxOrderQty', 'OnOrderQty', 'UnitMeasure']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'VendorID')


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'VendorID')

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim Vendor**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'vendor'

target_schema = 'gold'

target_table = 'DimVendor'

cdc = 'ModifiedAt'

key_col = "['VendorID']"
key_col = eval(key_col)

surrogate_key = 'DimVendorKey'

cols_to_look = "['AccountNumber', 'VendorName', 'CreditRating', 'PreferredVendorStatus', 'ActiveFlag', 'PurchasingWebServiceURL', 'AddressLine1', 'City', 'PostalCode', 'StateProvince', 'TerritoryName', 'TerritoryGroup']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'VendorID')


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)
    source_data = source_data.withColumnRenamed('BusinessEntityID', 'VendorID')

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim SalesReason**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'salesreason'

target_schema = 'gold'

target_table = 'DimSalesReason'

cdc = 'ModifiedAt'

key_col = "['SalesReasonID']"
key_col = eval(key_col)

surrogate_key = 'DimSalesReasonKey'

cols_to_look = "['ReasonName', 'ReasonType']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)
    source_data = source_data.withColumnRenamed('Name', 'ReasonName')


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)
    source_data = source_data.withColumnRenamed('Name', 'ReasonName')

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim ShipMethod**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'shipmethod'

target_schema = 'gold'

target_table = 'DimShipMethod'

cdc = 'ModifiedAt'

key_col = "['ShipMethodID']"
key_col = eval(key_col)

surrogate_key = 'DimShipMethodKey'

cols_to_look = "['ShipName', 'ShipBase', 'ShipRate']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)
    source_data = source_data.withColumnRenamed('Name', 'ShipName')


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)
    source_data = source_data.withColumnRenamed('Name', 'ShipName')

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim SpecialOffer**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'specialoffer'

target_schema = 'gold'

target_table = 'DimSpecialOffer'

cdc = 'ModifiedAt'

key_col = "['SpecialOfferID']"
key_col = eval(key_col)

surrogate_key = 'DimSpecialOfferKey'

cols_to_look = "['Description', 'DiscountPct', 'Type', 'Category', 'OfferStartedAt', 'OfferEndedAt', 'MinQty', 'MaxQty']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim Reseller**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'reseller'

target_schema = 'gold'

target_table = 'DimResller'

cdc = 'ModifiedAt'

key_col = "['CustomerID']"
key_col = eval(key_col)

surrogate_key = 'DimResellerKey'

cols_to_look = "['AccountNumber', 'StoreName', 'SalesPersonID', 'AddressLine1', 'City', 'PostalCode', 'StateProvince', 'TerritoryName', 'TerritoryGroup', 'QuarterlyTarget', 'Bonus', 'CommissionPct', 'PersonType', 'FirstName', 'MiddleName', 'LastName',]"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim RetailCustomer**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'retailcustomer'

target_schema = 'gold'

target_table = 'DimCustomer'

cdc = 'ModifiedAt'

key_col = "['CustomerID']"
key_col = eval(key_col)

surrogate_key = 'DimCustomerKey'

cols_to_look = "['AccountNumber', 'PersonType', 'Title', 'FirstName', 'MiddleName', 'LastName', 'AddressLine1', 'City', 'PostalCode', 'StateProvince', 'TerritoryName', 'TerritoryGroup']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim ScrapReason**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'scrapreason'

target_schema = 'gold'

target_table = 'DimScrapReason'

cdc = 'ModifiedAt'

key_col = "['ScrapReasonID']"
key_col = eval(key_col)

surrogate_key = 'DimScrapReasonKey'

cols_to_look = "['ScrapReason']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Dim Location**

# CELL ********************

backfill = ''

source_schema = 'sil'

source_table = 'location'

target_schema = 'gold'

target_table = 'DimLocation'

cdc = 'ModifiedAt'

key_col = "['LocationID']"
key_col = eval(key_col)

surrogate_key = 'DimLocationKey'

cols_to_look = "['LocationName', 'CostRate', 'Availability']"
cols_to_look = eval(cols_to_look)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If Incremnetal Load than this will be initiated.
if spark.catalog.tableExists(f"{target_schema}.{target_table}"):

# Deciding last Load Date for Incremnetal Ingestion.
    if len(backfill) == 0:
        if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
            last_load_date = spark.sql(f"""
                                        SELECT max({cdc}) FROM {target_schema}.{target_table}
                                    """).collect()[0][0]
        else:
            last_load_date = '1900-01-01'
    else:
        last_load_date = backfill
    print(last_load_date)


# Ingestion of Source Data with SCD Type 2 Columns after last_load_date value
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                WHERE {cdc} > '{last_load_date}'
                """)


# Ingestion of Target Table Data that already exist in gold table
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


# Jonining Source and Target Table on Business Key (Key_Col Value)
    # Target Table Columns will have _trg at the end of column name for distinction b/w src and trg
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

# Filtering Out New Data that are not yet available in gold layer, rows that don't have Surrogate key value
    # New COlumn is Created called MergeAction with Insert as Value for all new data
    src_col = [c for c in source_data.columns]
    new_data = all_data.filter(col(f"{surrogate_key}").isNull())\
                        .select(src_col)\
                        .withColumn("MergeAction", lit("INSERT"))


# Filtering Data that have been updated by comparing source cols and target cols, rows that have Surrogate key value.
    cols_to_look_or = " OR ".join([f"{c} <> {c}_trg" for c in cols_to_look])

    src_col = [c for c in source_data.columns]

    #This df have all updated data rows
    changed_data = all_data.filter(col(f"{surrogate_key}").isNotNull() & expr(f"({cols_to_look_or})"))\

    # This df have updated data with their Old Surrogate Key and Update as MergeAction, required for MERGE Statement
    df_updates = changed_data.select(*src_col, f"{surrogate_key}")\
                            .withColumn("MergeAction", lit("UPDATE"))

    # This df have updated data but without old surrogate key, new keys will be assinged to them in next step
    df_inserts_from_updates = changed_data.select(src_col)\
                                            .withColumn("MergeAction", lit("INSERT"))


    # Here we are Appending 2 df into one that will be Inserted in target and need NEW SURROGATE KEY
    all_inserts = new_data.unionByName(df_inserts_from_updates)


# This will assign all_inserts df new Surrogate Key Value for both new and updated data after max_surroagate_key value
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
    print(max_surrogate_key)

# Now We will Append df_update and df_all_inserts_enr into one df so that they will become our src for MERGE Statement
    df_merge_input = df_updates.unionByName(df_all_inserts_enr, allowMissingColumns=True)\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

# Next we need a temp view on top of df_merge_input for Merge Statement
    df_merge_input.createOrReplaceTempView("src_view")

# Now We execute Merge Statement with src_view for target table 
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


# This will be executed when it is the INITIAL Run with No Target Table at sink location
else:
    source_data = spark.sql(f"""
                SELECT
                    *,
                    CAST('1900-01-01' AS TIMESTAMP) AS StartDate,
                    CAST('3000-12-31' AS TIMESTAMP) AS EndDate,
                    'Y' AS IsCurrent
                FROM {source_schema}.{source_table}
                """)

    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_all_inserts_enr = source_data.withColumn(f"{surrogate_key}", row_number().over(w) + lit(max_surrogate_key))\
                            .withColumn("CreatedDate", current_timestamp())\
                            .withColumn("UpdatedDate", current_timestamp())

    df_all_inserts_enr.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
