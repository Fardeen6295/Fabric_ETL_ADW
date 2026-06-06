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

# ## Silver Transformations for Fact Tables
# Only Fact Tables are considered in thi notebook. \
# Tables from all department are transformed irrespective of department.

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import * 
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Sales Order Header Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'FactOrderHeader'

source_table = 'salesorderheader'

cdc = 'ModifiedDate'

key_col = 'SalesOrderID'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        SalesOrderID, OrderDate, DueDate, ShipDate, OnlineOrderFlag,
        CustomerID, SalesPersonID, TerritoryID, ShipMethodID,
        SubTotal, TaxAmt, Freight, TotalDue, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
    AND
        Status = 5
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), f"trg.{key_col} = src.{key_col}")\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Sales Order Detail Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'FactOrderDetail'

source_table = 'salesorderdetail'

cdc = 'ModifiedDate'

key_col = "['SalesOrderID', 'SalesOrderDetailID']"
key_col = eval(key_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        SalesOrderID, SalesOrderDetailID, OrderQty, ProductID, SpecialOfferID,
        UnitPrice, UnitPriceDiscount, LineTotal, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales_header = spark.sql(f"""
                    SELECT SalesOrderID, OrderDate FROM sil.factorderheader WHERE {cdc} >= '{last_load_date}'
                    """)

source_data = source_data.join(df_sales_header, source_data.SalesOrderID == df_sales_header.SalesOrderID, how='left')\
                        .drop(df_sales_header.SalesOrderID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keY_col_init = " AND ".join([f"src.{col} = trg.{col}" for col in key_col])
keY_col_init

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), keY_col_init)\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Purchase Order Header Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'PurchaseOrderHeader'

source_table = 'PurchaseOrderHeader'

cdc = 'ModifiedDate'

key_col = 'PurchaseOrderID'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        PurchaseOrderID, EmployeeID, VendorID, ShipMethodID, Status,
        OrderDate, ShipDate,
        SubTotal, TaxAmt, Freight, TotalDue, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_data = source_data.withColumn("Status", when(col('Status') == 1, "Pending")
                                    .when(col("Status") == 2, "Approved") 
                                    .when(col("Status") == 3, "Rejected")
                                    .when(col("Status") == 4, "Complete")
                        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), f"trg.{key_col} = src.{key_col}")\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Purchase Order Detail Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'PurchaseOrderDetail'

source_table = 'PurchaseOrderDetail'

cdc = 'ModifiedDate'

key_col = "['PurchaseOrderID', 'PurchaseOrderDetailID']"
key_col = eval(key_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        PurchaseOrderID, PurchaseOrderDetailID, DueDate, OrderQty, ProductID,
        UnitPrice, LineTotal, ReceivedQty, RejectedQty, StockedQty, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")

source_data_header = spark.sql(f"""
    SELECT 
        PurchaseOrderID, VendorID, OrderDate, EmployeeID, ShipMethodID
    FROM {source_schema}.purchaseorderheader 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_data_joined = source_data.join(source_data_header, "PurchaseOrderID", how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keY_col_init = " AND ".join([f"trg.{col} = src.{col}"for col in key_col])
keY_col_init

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data_joined.alias("src"), keY_col_init)\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data_joined.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Work Order Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'WorkOrder'

source_table = 'WorkOrder'

cdc = 'ModifiedDate'

key_col = "['WorkOrderID']"
key_col = eval(key_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        WorkOrderID, ProductID, OrderQty, StockedQty, ScrappedQty, 
        StartDate, EndDate, DueDate, ScrapReasonID, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keY_col_init = " AND ".join([f"src.{col} = trg.{col}" for col in key_col])
keY_col_init

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), keY_col_init)\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Work Order Rounting Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'WorkOrderRouting'

source_table = 'WorkOrderRouting'

cdc = 'ModifiedDate'

key_col = "['WorkOrderID', 'ProductID', 'LocationID']"
key_col = eval(key_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        WorkOrderID, ProductID, OperationSequence, LocationID,
        ScheduledStartDate, ScheduledEndDate, ActualStartDate, ActualEndDate, 
        ActualResourceHrs, PlannedCost, ActualCost, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keY_col_init = " AND ".join([f"src.{col} = trg.{col}" for col in key_col])
keY_col_init

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), keY_col_init)\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Transaction History Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'TransactionHistory'

source_table = 'TransactionHistory'

cdc = 'ModifiedDate'

key_col = "['TransactionID', 'ProductID', 'ReferenceOrderID']"
key_col = eval(key_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        TransactionID, ProductID, ReferenceOrderID, ReferenceOrderLineID,
        TransactionDate, TransactionType, Quantity, ActualCost, ModifiedDate
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keY_col_init = " AND ".join([f"src.{col} = trg.{col}" for col in key_col])
keY_col_init

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), keY_col_init)\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Sales Order Reason Table**

# CELL ********************

backfill = ''

target_schema = 'sil'

source_schema = 'brz'

target_table = 'FactOrderHeaderReason'

source_table = 'SalesOrderHeaderSalesReason'

cdc = 'ModifiedDate'

key_col = 'SalesOrderID'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        last_load_date = spark.sql(f"""
                                SELECT MAX({cdc}) FROM {target_schema}.{target_table}
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
        *
    FROM {source_schema}.{source_table} 
    WHERE 
        {cdc} >= '{last_load_date}'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(source_data.alias("src"), f"trg.{key_col} = src.{key_col}")\
                        .whenMatchedUpdateAll(condition= f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    source_data.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
