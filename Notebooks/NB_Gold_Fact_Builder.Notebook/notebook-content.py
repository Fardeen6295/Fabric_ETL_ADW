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

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **KEY POINTS TO REMEMBER**
# * fact_cols var will not have any business key columns like ProductID or SalesPersonID, they will have DimSurrogateKeys.
# * fact_key_cols will have all surrogate_keys and any primary key column of the table to facilitate UPSERT activity at last.
# * cdc var is defined to facilitate Incremnetal Load of data
# * backfill can be filled with any date value which user want as start date for loading data from source table
# * join_keys inside dimension array of dict is tuple saying foreign key of fact and primary key of dimension it is refering to.

# MARKDOWN ********************

# #### **Fact Sales Header**

# CELL ********************

source_schema = 'sil'

source_table = 'factorderheader'

target_schema = 'gold'

target_table = 'FactSalesHeader'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'OrderDate'

dimensions = [
    {  
        "table":f"{target_schema}.dimcustomer",
        "alias":"DimCustomer",
        "join_keys":[("CustomerID", "CustomerID")] #(fact_Col, Dim_Col)
    },
    {
        "table":f"{target_schema}.dimreseller",
        "alias":"DimReseller",
        "join_keys":[("CustomerID", "CustomerID")]
    },
    {
        "table":f"{target_schema}.dimsalesperson",
        "alias":"DimSalesPerson",
        "join_keys":[("SalesPersonID", "SalesPersonID")]
    },
    {
        "table":f"{target_schema}.dimshipmethod",
        "alias":"DimShipMethod",
        "join_keys":[("ShipMethodID", "ShipMethodID")]
    }
]

fact_cols = ["SalesOrderID", "OrderDate", "DueDate", "ShipDate", "OnlineOrderFlag", "SubTotal", "TaxAmt", "Freight", "TotalDue", "ModifiedDate"]

fact_key_cols = ["DimCustomerKey", "DimResellerKey", "DimSalesPersonKey", "DimShipMethodKey"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_columns, cdc, last_load_date):
    fact_alias = 'f'

    # Base Columns to Select
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]

    join_clause = []
    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        table_name = dim['table'].split('.')[-1]
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key)

        on_condition = [
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} > {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} <= {table_alias}.EndDate" for fk, dk in dim['join_keys']
        ]

        join_condition = f"LEFT JOIN {table_full} {table_alias} ON " + " AND ".join(on_condition)
        join_clause.append(join_condition)

        select_clause = ", \n".join(select_cols)
        joins = "\n".join(join_clause)
        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                    SELECT 
                        {select_clause}
                    FROM
                        {fact_table} {fact_alias}
                        {joins}
                    WHERE
                        {where_clause}
                    """.strip()
    return final_query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact = spark.sql(query)
df_fact.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Fact Sales Details**

# CELL ********************

source_schema = 'sil'

source_table = 'factorderdetail'

target_schema = 'gold'

target_table = 'FactSalesDetail'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'OrderDate'

dimensions = [
    {  
        "table":f"{target_schema}.dimproduct",
        "alias":"DimProduct",
        "join_keys":[("ProductID", "ProductID")] #(fact_Col, Dim_Col)
    },
    {
        "table":f"{target_schema}.dimspecialoffer",
        "alias":"DimSpecialOffer",
        "join_keys":[("SpecialOfferID", "SpecialOfferID")]
    }
]

fact_cols = ["SalesOrderID", "SalesOrderDetailID", "OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal", "ModifiedDate", "OrderDate"]

fact_key_cols = ["DimProductKey", "DimSpecialOfferKey"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date):
    fact_alias = 'f'

    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]

    join_clause = []

    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        table_name = dim['table'].split('.')[-1]
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key)
    
        on_condition = [
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} > {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} <= {table_alias}.EndDate" for fk, dk in dim['join_keys']
            ]
        join_condition = f"LEFT JOIN {table_full} {table_alias} ON " + " AND ".join(on_condition)
        join_clause.append(join_condition)

        select_clause = ', \n'.join(select_cols)

        joins = '\n'.join(join_clause)

        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                    SELECT 
                        {select_clause}
                    FROM
                        {fact_table} {fact_alias}
                        {joins}
                    WHERE
                        {where_clause}
                    """.strip()
    return final_query


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_details = spark.sql(query)
df_fact_details.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_fact_details.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact_details.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Fact Purchase Header**

# CELL ********************

source_schema = 'sil'

source_table = 'purchaseorderheader'

target_schema = 'gold'

target_table = 'FactPurchaseHeader'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'OrderDate'

dimensions = [
    {
        "table" : f"{target_schema}.DimEmployee",
        "alias" : "DimEmployee",
        "join_keys" : [("EmployeeID", "EmployeeID")]
    },
    {
        "table" : f"{target_schema}.DimVendor",
        "alias" : "DimVendor",
        "join_keys" : [("VendorID", "VendorID")]
    },
    {
        "table" : f"{target_schema}.DimShipMethod",
        "alias" : "DimShipMethod",
        "join_keys" : [("ShipMethodID", "ShipMethodID")]
    }
]

fact_cols = ["PurchaseOrderID", "OrderDate", "ShipDate", "Status", "SubTotal", "TaxAmt", "Freight", "TotalDue", "ModifiedDate"]

fact_key_cols = ["DimEmployeeKey", "DimVendorKey", "DimShipMethodKey"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date):
    fact_alias = "f"
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]
    join_clause = []

    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        table_name = dim['table'].split('.')[-1]
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key)

        on_condition = [
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} > {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} <= {table_alias}.EndDate" for fk,dk in dim['join_keys']
            ]
        join_condition = f"LEFT JOIN {table_full} {table_alias} ON " + " AND ".join(on_condition)
        join_clause.append(join_condition)

        select_clause = ",\n".join(select_cols)

        joins = "\n".join(join_clause)

        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                        SELECT
                            {select_clause}
                        FROM
                            {fact_table} {fact_alias}
                            {joins}
                        WHERE
                            {where_clause}
                        """.strip()
    return final_query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_purchase_header = spark.sql(query)
df_fact_purchase_header.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"trg.{col} = src.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_fact_purchase_header.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact_purchase_header.write.format('delta')\
        .mode('append')\
        .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Fact Purchase Detail**

# CELL ********************

source_schema = 'sil'

source_table = 'purchaseorderdetail'

target_schema = 'gold'

target_table = 'FactPurchaseDetail'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'OrderDate'

dimensions = [
    {
        "table":f"{target_schema}.DimProduct",
        "alias":"DimProduct",
        "join_keys":[("ProductID", "ProductID")]
    },
    {
        "table":f"{target_schema}.DimVendor",
        "alias":"DimVendor",
        "join_keys":[("VendorID", "VendorID")]
    },
    {
        "table":f"{target_schema}.DimEmployee",
        "alias":"DimEmployee",
        "join_keys":[("EmployeeID", "EmployeeID")]
    },
    {
        "table":f"{target_schema}.DimShipMethod",
        "alias":"DimShipMethod",
        "join_keys":[("ShipMethodID", "ShipMethodID")]
    }
]

fact_cols = ["PurchaseOrderID", "PurchaseOrderDetailID", "OrderDate", "DueDate", "OrderQty", "ProductID", "UnitPrice", "LineTotal", "ReceivedQty", "RejectedQty", "StockedQty", "VendorID", "EmployeeID", "ShipMethodID", "ModifiedDate"]

fact_key_cols = ["DimProductKey", "DimEmployeeKey", "DimVendorKey", "DimShipMethodKey"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date):
    fact_alias = "f"
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]
    
    join_cluase = []
    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key)

        on_condition = [ 
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} >= {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} < {table_alias}.EndDate" for fk, dk in dim['join_keys'] 
        ]
        join = f"LEFT JOIN {table_full} AS {table_alias} ON " + " AND ".join(on_condition)
        join_cluase.append(join)

        select_clause = ",\n".join(select_cols)
        joins = "\n".join(join_cluase)

        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                    SELECT
                        {select_clause}
                    FROM
                        {fact_table} {fact_alias}
                        {joins}
                    WHERE
                        {where_clause}
                """.strip()
    return final_query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_purchase_detail = spark.sql(query)
df_fact_purchase_detail.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_fact_purchase_detail.alias("src"),fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact_purchase_detail.write.format('delta')\
                            .mode('append')\
                            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Fact Production Order**

# CELL ********************

source_schema = 'sil'

source_table = 'workorder'

target_schema = 'gold'

target_table = 'FactProductionOrder'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'StartDate'

dimensions = [
    {
        "table":f"{target_schema}.DimProduct",
        "alias":"DimProduct",
        "join_keys":[("ProductID", "ProductID")]
    },
    {
        "table":f"{target_schema}.DimScrapReason",
        "alias":"DimScrapReason",
        "join_keys":[("ScrapReasonID", "ScrapReasonID")]
    }
]

fact_cols = ["WorkOrderID", "OrderQty", "StockedQty", "ScrappedQty", "StartDate", "EndDate", "DueDate", "ModifiedDate"]

fact_key_cols = ["DimProductKey", "DimScrapReasonKey"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_columns, cdc, last_load_date):
    fact_alias = "f"
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]
    join_clauses = []

    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key) 

        on_condition = [
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} >= {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} < {table_alias}.EndDate" for fk, dk in dim['join_keys']
            ]
        join_clause = f"LEFT JOIN {table_full} AS {table_alias} ON " + " AND ".join(on_condition)
        join_clauses.append(join_clause)

        select_clause = ",\n".join(select_cols)
        joins = "\n".join(join_clauses)
        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                    SELECT
                        {select_clause}
                    FROM
                        {fact_table} AS {fact_alias}
                        {joins}
                    WHERE
                        {where_clause}
                    """.strip()
    return final_query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_production = spark.sql(query)
df_fact_production.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_fact_production.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact_production.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Fact Production Order Routing**

# CELL ********************

source_schema = 'sil'

source_table = 'workorderrouting'

target_schema = 'gold'

target_table = 'FactProductionOrderRouting'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'ActualStartDate'

dimensions = [
    {
        "table":f"{target_schema}.DimProduct",
        "alias":"DimProduct",
        "join_keys":[("ProductID", "ProductID")]
    },
    {
        "table":f"{target_schema}.DimLocation",
        "alias":"DimLocation",
        "join_keys":[("LocationID", "LocationID")]
    }
]

fact_cols = ["WorkOrderID", "OperationSequence", "ScheduledStartDate", "ScheduledEndDate", "ActualStartDate", "ActualEndDate", "ActualResourceHrs", "PlannedCost", "ActualCost", "ModifiedDate"]

fact_key_cols = ["DimProductKey", "DimLocationKey", "WorkOrderID", "OperationSequence"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_columns, cdc, last_load_date):
    fact_alias = "f"
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]
    join_clauses = []

    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key) 

        on_condition = [
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} >= {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} < {table_alias}.EndDate" for fk, dk in dim['join_keys']
            ]
        join_clause = f"LEFT JOIN {table_full} AS {table_alias} ON " + " AND ".join(on_condition)
        join_clauses.append(join_clause)

        select_clause = ",\n".join(select_cols)
        joins = "\n".join(join_clauses)
        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                    SELECT
                        {select_clause}
                    FROM
                        {fact_table} AS {fact_alias}
                        {joins}
                    WHERE
                        {where_clause}
                    """.strip()
    return final_query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_production_routing = spark.sql(query)
df_fact_production_routing.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_fact_production_routing.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_fact_production_routing.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Fact Transaction History**

# CELL ********************

source_schema = 'sil'

source_table = 'transactionhistory'

target_schema = 'gold'

target_table = 'FactTransactionHistory'

cdc = 'ModifiedDate'

backfill = ''

fact_table = f'{source_schema}.{source_table}'

fact_table_incident_col = 'TransactionDate'

dimensions = [
    {
        "table":f"{target_schema}.DimProduct",
        "alias":"DimProduct",
        "join_keys":[("ProductID", "ProductID")]
    }
]

fact_cols = ["TransactionID", "ReferenceOrderID", "ReferenceOrderLineID", "TransactionDate", "TransactionType", "Quantity", "ActualCost", "ModifiedDate"]

fact_key_cols = ["DimProductKey", "TransactionID"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(backfill) == 0:
    if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
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

def generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_columns, cdc, last_load_date):
    fact_alias = "f"
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]
    join_clauses = []

    for dim in dimensions:
        table_full = dim['table']
        table_alias = dim['alias']
        surrogate_key = f"{table_alias}.{table_alias}Key"
        select_cols.append(surrogate_key) 

        on_condition = [
            f"{fact_alias}.{fk} = {table_alias}.{dk} AND {fact_alias}.{fact_table_incident_col} >= {table_alias}.StartDate AND {fact_alias}.{fact_table_incident_col} < {table_alias}.EndDate" for fk, dk in dim['join_keys']
            ]
        join_clause = f"LEFT JOIN {table_full} AS {table_alias} ON " + " AND ".join(on_condition)
        join_clauses.append(join_clause)

        select_clause = ",\n".join(select_cols)
        joins = "\n".join(join_clauses)
        where_clause = f"{fact_alias}.{cdc} > DATE('{last_load_date}')"

        final_query = f"""
                    SELECT
                        {select_clause}
                    FROM
                        {fact_table} AS {fact_alias}
                        {joins}
                    WHERE
                        {where_clause}
                    """.strip()
    return final_query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = generate_fact_query_incremnetal_Load(fact_table, dimensions, fact_cols, cdc, last_load_date)
print(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_transaction = spark.sql(query)
df_transaction.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forName(spark, f"{target_schema}.{target_table}")
    dlt_obj.alias("trg").merge(df_transaction.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition=f"src.{cdc} > trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_transaction.write.format('delta')\
            .mode('append')\
            .saveAsTable(f"{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
