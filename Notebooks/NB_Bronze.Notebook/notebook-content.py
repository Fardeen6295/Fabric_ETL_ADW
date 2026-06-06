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

# # Bronze Layer Transformations.
# Here We will Clean and Transform data required as per business. \
# Notebook Cleans & Transform all 60 tables from bronze folder under files. \
# ***Note: No Joins will be performed, only Incremental load & cleaning of Bronze Folder data***

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Human Resource Tables**

# MARKDOWN ********************

# #### Department Table

# CELL ********************

df_department = spark.read.format('parquet')\
                    .load('Files/bronze/HumanResources/Department')

df_department.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.Department')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Employee Table

# CELL ********************

df_employee = spark.read.format('parquet')\
                    .load('Files/bronze/HumanResources/vw_Employee_Clean')\
                    .drop('rowguid')

df_employee.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.Employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### EmployeeDepartmentHistory Table

# CELL ********************

df_employeedpthis = spark.read.format('parquet')\
                    .load('Files/bronze/HumanResources/EmployeeDepartmentHistory')\
                    .drop('rowguid')

df_employeedpthis.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.EmployeeDepartmentHistory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### EmployeePayHistory Table

# CELL ********************

df_employeepayhis = spark.read.format('parquet')\
                    .load('Files/bronze/HumanResources/EmployeePayHistory')\
                    .drop('rowguid')

df_employeepayhis.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.EmployeePayHistory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### JobCandidate Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/HumanResources/JobCandidate')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.JobCandidate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Shift Table

# CELL ********************

shift_schema = StructType([
    StructField("ShiftID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("StartTime", LongType(), False),
    StructField("EndTime", LongType(), False),
    StructField("ModifiedDate", TimestampType(), True)
])

df_shift = spark.read.format('parquet')\
                .schema(shift_schema)\
                .load('Files/bronze/HumanResources/Shift/Shift.parquet')

df_shift = df_shift.withColumn("StartTime_temp", timestamp_seconds(col("StartTime") / 1000000000))\
                    .withColumn("EndTime_temp", timestamp_seconds(col("EndTime") / 1000000000))

df_shift = df_shift.withColumn("StartTime_Str", date_format(col("StartTime_temp"), "HH:mm:ss"))\
                    .withColumn("EndTime_Str", date_format(col("EndTime_temp"), "HH:mm:ss"))

df_shift = df_shift.select(
    col("ShiftID"), 
    col("Name"), 
    col("StartTime_Str").alias("StartTime"), 
    col("EndTime_Str").alias("EndTime"),
    col("ModifiedDate")
)

df_shift.write.format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema', 'true')\
    .saveAsTable('brz.Shift')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Person Tables**

# MARKDOWN ********************

# #### Address Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/vw_Address_Clean')\
                    .drop('rowguid', 'SpatialLocation', 'AddressLine2')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.Address')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### AddressType Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/AddressType')\
                    .drop('rowguid')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.AddressType')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BusinessEntity Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/BusinessEntity')\
                    .drop('rowguid')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.BusinessEntity')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BusinessEntityAddress Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/BusinessEntityAddress')\
                    .drop('rowguid')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.BusinessEntityAddress')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BusinessEntityContact Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/BusinessEntityContact')\
                    .drop('rowguid')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.BusinessEntityContact')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ContactType Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/ContactType')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.ContactType')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### CountryRegion Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/CountryRegion')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.CountryRegion')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### EmailAddress Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/EmailAddress')\
                    .drop('rowguid')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.EmailAddress')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Person Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/Person')\
                    .drop('rowguid', 'Demographics', 'AdditionalContactInfo', 'Suffox')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.Person')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PersonPhone Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/PersonPhone')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.PersonPhone')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PhoneNumberType Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/PhoneNumberType')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.PhoneNumberType')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### StateProvince Table

# CELL ********************

df = spark.read.format('parquet')\
                    .load('Files/bronze/Person/StateProvince')\
                    .drop('rowguid')

df.write.format('delta')\
                    .mode('overwrite')\
                    .option('overwriteSchema', 'true')\
                    .saveAsTable('brz.StateProvince')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Production Tables**

# MARKDOWN ********************

# #### BillsOfMaterial Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/BillOfMaterials')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.BillOfMaterials')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Culture Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/Culture')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Culture')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Location Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/Location')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Location')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Product Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/Product')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Product')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductCategory Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductCategory')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductCategory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductDescription Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductDescription')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductDescription')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductInventory Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductInventory')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductInventory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductModel Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductModel')\
            .drop('rowguid', 'Instructions', 'CatalogDescription')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductModel')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductModelProductDescriptionCulture Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductModelProductDescriptionCulture')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductModelCulture')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductReview Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductReview')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductReview')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ProductSubcategory Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ProductSubcategory')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductSubcategory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ScrapReason Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/ScrapReason')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ScrapReason')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### TransactionHistory Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/TransactionHistory')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.TransactionHistory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### TransactionHistoryArchive Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/TransactionHistoryArchive')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.TransactionHistoryArchive')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### UnitMeasure Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/UnitMeasure')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.UnitMeasure')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### WorkOrder Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/WorkOrder')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.WorkOrder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### WorkOrderRouting Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Production/WorkOrderRouting')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.WorkOrderRouting')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Purchasing Tables**

# MARKDOWN ********************

# #### ProductVendor Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Purchasing/ProductVendor')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ProductVendor')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PurchaseOrderDetail Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Purchasing/PurchaseOrderDetail')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.PurchaseOrderDetail')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PurchaseOrderHeader Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Purchasing/PurchaseOrderHeader')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.PurchaseOrderHeader')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ShipMethod Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Purchasing/ShipMethod')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.ShipMethod')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Vendor Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Purchasing/Vendor')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Vendor')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Sales Tables**

# MARKDOWN ********************

# #### CountryRegionCurrency Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/CountryRegionCurrency')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.CountryRegionCurrency')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### CreditCard Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/CreditCard')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.CreditCard')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Currency Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/Currency')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Currency')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### CurrencyRate Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/CurrencyRate')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.CurrencyRate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Customer Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/Customer')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PersonCreditCard Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/PersonCreditCard')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.PersonCreditCard')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SalesOrderDetail Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesOrderDetail')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesOrderDetail')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SalesOrderHeader Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesOrderHeader')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesOrderHeader')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SalesOrderHeaderSalesReason Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesOrderHeaderSalesReason')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesOrderHeaderSalesReason')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ####  SalesPerson Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesPerson')\
            .drop('rowguid', 'SalesLastYear', 'SalesYTD')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesPerson')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SalesReason Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesReason')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesReason')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SalesTaxRate Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesTaxRate')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesTaxRate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SalesTerritory Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SalesTerritory')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SalesTerritory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SpecialOffer Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SpecialOffer')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SpecialOffer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ####  SpecialOfferProduct Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/SpecialOfferProduct')\
            .drop('rowguid')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.SpecialOfferProduct')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Store Table

# CELL ********************

df = spark.read.format('parquet')\
            .load('Files/bronze/Sales/Store')\
            .drop('rowguid', 'Demographics')

df.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('brz.Store')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
