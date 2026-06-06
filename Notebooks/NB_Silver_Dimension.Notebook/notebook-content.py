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

# # Silver Transformations
# It performs merge between snowflake dimension of OLTP. \
# End result are dimension tables of type 2 or type 1. \
# NB only work for dimension tables

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Employee Table Joins

# CELL ********************

df_department = spark.read.table('brz.department')\
                            .withColumnRenamed('Name', 'DepartmentName')

df_employee = spark.read.table('brz.employee')\
                        .drop('ModifiedDate')
                        
df_emp_dept = spark.read.table('brz.employeedepartmenthistory')

df_payrate = spark.read.table('brz.employeepayhistory')\
                        .withColumnRenamed('Rate', 'HourlyPayRate')\
                        .drop('ModifiedDate')

df_shift = spark.read.table('brz.shift')\
                        .drop('StartDate', 'EndDate', 'ModifiedDate')\
                        .withColumnsRenamed(
                            {'Name':'Shift', 
                            'StartTime':'ShiftStart', 
                            'EndTime':'ShiftEnd'}
                        )

df_person = spark.read.table('brz.Person')\
                .drop('ModifiedDate', 'NameStyle', 'Suffix')

df_person_phone = spark.read.table('brz.personphone')\
                .drop('ModifiedDate')

df_phone_type = spark.read.table('brz.phonenumbertype')\
                .drop('ModifiedDate')\
                .withColumnRenamed('Name', 'PhoneNumberType')

df_address = spark.read.table('brz.address')\
                .drop('ModifiedDate')

df_address_type = spark.read.table('brz.addresstype')\
                        .drop('ModifiedDate')\
                        .withColumnRenamed('Name', 'AddressType')

df_bus_ent_address = spark.read.table('brz.businessentityaddress')\
                        .drop('ModifiedDate')

df_stateProvince = spark.read.table('brz.Stateprovince')\
                        .select('StateProvinceID', col('Name').alias('StateProvince'), 'TerritoryID')

df_territory = spark.read.table('brz.salesterritory')\
                        .select('TerritoryID', col('Name').alias('TerritoryName'), col('Group').alias('TerritoryGroup'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp_dept = df_emp_dept.filter('EndDate IS NULL').drop('StartDate', 'EndDate', 'ModifiedDate')


df_payrate = df_payrate.withColumn('rn', row_number().over(Window.partitionBy('BusinessEntityID').orderBy(col('RateChangeDate').desc())))
df_payrate = df_payrate.filter('rn == 1').drop('rn', 'ModifiedDate')
df_payrate = df_payrate.withColumn('PayFrequency', when(col('PayFrequency') == 1, 'Weekly')\
                                                    .otherwise('Monthly'))                            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_employee.join(df_emp_dept, df_employee.BusinessEntityID == df_emp_dept.BusinessEntityID, how='left')\
                        .drop(df_emp_dept.BusinessEntityID)

df_join = df_join.join(df_department, df_join.DepartmentID == df_department.DepartmentID, how='left')\
                        .drop(df_join.DepartmentID, df_department.DepartmentID, df_department.ModifiedDate)

df_join = df_join.join(df_shift, df_join.ShiftID == df_shift.ShiftID, how='left')\
                        .drop(df_join.ShiftID, df_shift.ShiftID)

df_join = df_join.join(df_payrate, df_join.BusinessEntityID == df_payrate.BusinessEntityID, how='left')\
                        .drop(df_payrate.BusinessEntityID, df_payrate.RateChangeDate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_join.join(df_person, df_join.BusinessEntityID == df_person.BusinessEntityID, how='left')\
                    .drop(df_person.BusinessEntityID)

df_join = df_join.join(df_bus_ent_address, df_join.BusinessEntityID == df_bus_ent_address.BusinessEntityID, how='left')\
                    .drop(df_bus_ent_address.BusinessEntityID)

df_join = df_join.join(df_address, df_join.AddressID == df_address.AddressID, how='left')\
                    .drop(df_address.AddressID, df_join.AddressID)

df_join = df_join.join(df_address_type, df_join.AddressTypeID == df_address_type.AddressTypeID, how='left')\
                    .drop(df_join.AddressTypeID, df_address_type.AddressTypeID)

df_join = df_join.join(df_stateProvince, df_join.StateProvinceID == df_stateProvince.StateProvinceID, how='left')\
                    .drop(df_join.StateProvinceID, df_stateProvince.StateProvinceID)

df_join = df_join.join(df_territory, df_join.TerritoryID == df_territory.TerritoryID, how='left')\
                    .drop(df_join.TerritoryID, df_territory.TerritoryID)

df_join = df_join.join(df_person_phone, df_join.BusinessEntityID == df_person_phone.BusinessEntityID, how='left')\
                    .drop(df_person_phone.BusinessEntityID)

df_join = df_join.join(df_phone_type, df_join.PhoneNumberTypeID == df_phone_type.PhoneNumberTypeID, how='left')\
                    .drop(df_join.PhoneNumberTypeID, df_phone_type.PhoneNumberTypeID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.Employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Vendor Table Joins

# CELL ********************

df_vendor = spark.read.table('brz.vendor')\
                        .drop('ModifiedDate')\
                        .withColumnRenamed('Name', 'VendorName')

df_shipmethod = spark.read.table('brz.shipmethod')\
                        .withColumnRenamed('Name', 'ShipMethod')\
                        .drop('ModifiedDate')


df_person = spark.read.table('brz.Person')\
                .drop('ModifiedDate', 'NameStyle', 'Suffix')
df_person_phone = spark.read.table('brz.personphone')
df_phone_type = spark.read.table('brz.phonenumbertype')
df_address = spark.read.table('brz.address')\
                .drop('ModifiedDate')

df_address_type = spark.read.table('brz.addresstype')\
                        .drop('ModifiedDate')

df_bus_ent_address = spark.read.table('brz.businessentityaddress')\
                        .drop('ModifiedDate')

df_stateProvince = spark.read.table('brz.Stateprovince')\
                        .select('StateProvinceID', col('Name').alias('StateProvince'), 'TerritoryID')

df_territory = spark.read.table('brz.salesterritory')\
                        .select('TerritoryID', col('Name').alias('TerritoryName'), col('Group').alias('TerritoryGroup'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_vendor.join(df_bus_ent_address, df_vendor.BusinessEntityID == df_bus_ent_address.BusinessEntityID, how='left')\
                    .drop(df_bus_ent_address.BusinessEntityID)

df_join = df_join.join(df_address, df_join.AddressID == df_address.AddressID, how='left')\
                    .drop(df_address.AddressID, df_join.AddressID)

df_join = df_join.join(df_address_type, df_join.AddressTypeID == df_address_type.AddressTypeID, how='left')\
                    .drop(df_join.AddressTypeID, df_address_type.AddressTypeID)

df_join = df_join.join(df_stateProvince, df_join.StateProvinceID == df_stateProvince.StateProvinceID, how='left')\
                    .drop(df_join.StateProvinceID, df_stateProvince.StateProvinceID)

df_join = df_join.join(df_territory, df_join.TerritoryID == df_territory.TerritoryID, how='left')\
                    .drop(df_join.TerritoryID, df_territory.TerritoryID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.Vendor')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ProductVendor Table Joins

# CELL ********************

df_product_vendor = spark.read.table('brz.productvendor')\
                        .drop('ModifiedDate')

df_unitmeasure = spark.read.table('brz.unitmeasure')\
                        .drop('ModifiedDate')\
                        .withColumnRenamed('Name', 'UnitMeasure')

df_join = df_product_vendor.join(df_unitmeasure, df_product_vendor.UnitMeasureCode == df_unitmeasure.UnitMeasureCode, how='left')\
                        .drop(df_product_vendor.UnitMeasureCode, df_unitmeasure.UnitMeasureCode)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.ProductVendor')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Product Table Joins

# CELL ********************

df_product = spark.read.table('brz.product')\
                .drop('ModifiedDate', 'SellStartDate', 'SellEndDate')\
                .withColumnRenamed('Name', 'ProductName')

df_prod_subcat = spark.read.table('brz.productsubcategory')\
                .drop('ModifiedDate')\
                .withColumnRenamed('Name', 'ProductSubCategoryName')

df_prod_cat = spark.read.table('brz.productcategory')\
                .drop('ModifiedDate')\
                .withColumnRenamed('Name', 'ProductCategoryName')

df_prod_model = spark.read.table('brz.productmodel')\
                .select('ProductModelID', col('Name').alias('ModelName'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_product = df_product.withColumn('Class', trim(col('Class')))\
                .withColumn('Style', trim(col('Style')))\
                .withColumn('ProductLine', trim(col('ProductLine')))

df_product = df_product.withColumn('ProductLine', when(col('ProductLine') == 'R', 'Road')\
                                    .when(col('ProductLine') == 'M', 'Mountain')\
                                    .when(col("ProductLine") == "T", "Touring")\
                                    .when(col("ProductLine") == "S", "Standard")\
                                    .otherwise("General")
                )

df_product = df_product.withColumn("Class", when(col("Class") == "H", "High")\
                            .when(col("Class") == "M", "Medium")\
                            .when(col("Class") == "L", "Low")\
                            .otherwise("Other")
                )

df_product = df_product.withColumn("Style", when(col("Style") == "M", "Men")\
                            .when(col("Style") == "W", "Women")\
                            .when(col("Style") == "U", "Universal")\
                            .otherwise("No-Style")
                )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_product.join(df_prod_subcat, df_product.ProductSubcategoryID == df_prod_subcat.ProductSubcategoryID, how='left')\
                .drop(df_prod_subcat.ProductSubcategoryID)

df_join = df_join.join(df_prod_cat, df_join.ProductCategoryID == df_prod_cat.ProductCategoryID, how='left')\
                .drop(df_prod_cat.ProductCategoryID)

df_join = df_join.join(df_prod_model, df_join.ProductModelID == df_prod_model.ProductModelID, how='left')\
                .drop(df_join.ProductModelID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.Product')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SalesPerson Table Joins

# CELL ********************

df_salesperson = spark.read.table('brz.salesperson')\
                    .select(
                        'BusinessEntityID', 
                        col('TerritoryID').alias('SalesTerritoryID'), 
                        col('SalesQuota').alias('QuarterlyTarget'),
                        'Bonus', 
                        'CommissionPct'
                    )

df_person = spark.read.table('brz.Person')\
                .drop('ModifiedDate', 'NameStyle', 'Suffix')

df_person_phone = spark.read.table('brz.personphone')\
                .drop('ModifiedDate')

df_phone_type = spark.read.table('brz.phonenumbertype')\
                .drop('ModifiedDate')\
                .withColumnRenamed('Name', 'PhoneNumberType')

df_address = spark.read.table('brz.address')\
                .drop('ModifiedDate')

df_address_type = spark.read.table('brz.addresstype')\
                        .drop('ModifiedDate')\
                        .withColumnRenamed('Name', 'AddressType')

df_bus_ent_address = spark.read.table('brz.businessentityaddress')\
                        .drop('ModifiedDate')

df_stateProvince = spark.read.table('brz.Stateprovince')\
                        .select('StateProvinceID', col('Name').alias('StateProvince'), 'TerritoryID')

df_territory = spark.read.table('brz.salesterritory')\
                        .select('TerritoryID', col('Name').alias('TerritoryName'), col('Group').alias('TerritoryGroup'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_salesperson.join(df_person, df_salesperson.BusinessEntityID == df_person.BusinessEntityID, how='left')\
                    .drop(df_person.BusinessEntityID)

df_join = df_join.join(df_bus_ent_address, df_join.BusinessEntityID == df_bus_ent_address.BusinessEntityID, how='left')\
                    .drop(df_bus_ent_address.BusinessEntityID)

df_join = df_join.join(df_address, df_join.AddressID == df_address.AddressID, how='left')\
                    .drop(df_address.AddressID, df_join.AddressID)

df_join = df_join.join(df_address_type, df_join.AddressTypeID == df_address_type.AddressTypeID, how='left')\
                    .drop(df_join.AddressTypeID, df_address_type.AddressTypeID)

df_join = df_join.join(df_stateProvince, df_join.StateProvinceID == df_stateProvince.StateProvinceID, how='left')\
                    .drop(df_join.StateProvinceID, df_stateProvince.StateProvinceID)

df_join = df_join.join(df_territory, df_join.TerritoryID == df_territory.TerritoryID, how='left')\
                    .drop(df_join.TerritoryID, df_territory.TerritoryID)\
                    .withColumnsRenamed({'TerritoryName':'HMTerritoryName', 'TerritoryGroup':'HMTerritoryGroup'})

df_join = df_join.join(df_territory, df_join.SalesTerritoryID == df_territory.TerritoryID, how='left')\
                    .drop(df_join.SalesTerritoryID, df_territory.TerritoryID, 'TerritoryID')\
                    .withColumnsRenamed({'TerritoryName':'SPTerritoryName', 'TerritoryGroup':'SPTerritoryGroup'})

df_join = df_join.join(df_person_phone, df_join.BusinessEntityID == df_person_phone.BusinessEntityID, how='left')\
                    .drop(df_person_phone.BusinessEntityID)

df_join = df_join.join(df_phone_type, df_join.PhoneNumberTypeID == df_phone_type.PhoneNumberTypeID, how='left')\
                    .drop(df_join.PhoneNumberTypeID, df_phone_type.PhoneNumberTypeID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.SalesPerson')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Sales Reason Table

# CELL ********************

df_salesreason = spark.read.table('brz.salesreason')\
                    .drop('ModifiedDate')\
                    .withColumn('ModifiedAt', current_timestamp())

df_salesreason.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.salesreason')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Scrap Reason

# CELL ********************

df_salesreason = spark.read.table('brz.scrapreason')\
                    .drop('ModifiedDate')\
                    .withColumnRenamed("Name", "ScrapReason")\
                    .withColumn('ModifiedAt', current_timestamp())

df_salesreason.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.scrapreason')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Location

# CELL ********************

df_salesreason = spark.read.table('brz.location')\
                    .drop('ModifiedDate')\
                    .withColumnRenamed("Name", "LocationName")\
                    .withColumn('ModifiedAt', current_timestamp())

df_salesreason.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.location')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Ship Method Table

# CELL ********************

df_shipMethod = spark.read.table('brz.shipMethod')\
                    .drop('ModifiedDate')\
                    .withColumn('ModifiedAt', current_timestamp())

df_shipMethod.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.shipMethod')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Special Offer Table

# CELL ********************

df_specialoffer = spark.read.table('brz.specialoffer')\
                    .drop('ModifiedDate')\
                    .withColumnsRenamed({
                        "StartDate":"OfferStartedAt",
                        "EndDate":"OfferEndedAt"
                    })\
                    .withColumn('ModifiedAt', current_timestamp())

df_specialoffer.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.specialoffer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Customer

# CELL ********************

df_customer = spark.read.table('brz.customer')

df_ind_cust = df_customer.filter(col('StoreID').isNull())\
                .drop("StoreID", "ModifiedDate")

df_reseller = df_customer.filter(col('StoreID').isNotNull())\
                .drop("ModifiedDate")

df_person = spark.read.table('brz.Person')\
                .drop('ModifiedDate', 'NameStyle', 'Suffix')

df_person_phone = spark.read.table('brz.personphone')\
                .drop('ModifiedDate')

df_phone_type = spark.read.table('brz.phonenumbertype')\
                .drop('ModifiedDate')\
                .withColumnRenamed('Name', 'PhoneNumberType')

df_address = spark.read.table('brz.address')\
                .drop('ModifiedDate')

df_address_type = spark.read.table('brz.addresstype')\
                        .drop('ModifiedDate')\
                        .withColumnRenamed('Name', 'AddressType')

df_bus_ent_address = spark.read.table('brz.businessentityaddress')\
                        .drop('ModifiedDate')

df_stateProvince = spark.read.table('brz.Stateprovince')\
                        .select('StateProvinceID', col('Name').alias('StateProvince'), 'TerritoryID')

df_territory = spark.read.table('brz.salesterritory')\
                        .select('TerritoryID', col('Name').alias('TerritoryName'), col('Group').alias('TerritoryGroup'))

df_store = spark.read.table('brz.store')\
                    .select("BusinessEntityID", col("Name").alias("StoreName"), 'SalesPersonID')

df_salesperson = spark.read.table('brz.salesperson')\
                    .select(
                        'BusinessEntityID', 
                        col('TerritoryID').alias('SalesTerritoryID'), 
                        col('SalesQuota').alias('QuarterlyTarget'),
                        'Bonus', 
                        'CommissionPct'
                    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_reseller.join(df_store, df_reseller.StoreID == df_store.BusinessEntityID, how='left')

df_join = df_join.join(df_bus_ent_address, df_join.BusinessEntityID == df_bus_ent_address.BusinessEntityID, how='left')

df_join = df_join.join(df_address, df_join.AddressID == df_address.AddressID, how='left')\
                .drop(df_address.AddressID, df_bus_ent_address.BusinessEntityID, df_join.AddressID)

df_join = df_join.join(df_stateProvince, df_join.StateProvinceID == df_stateProvince.StateProvinceID, how='left')\
                .drop(df_stateProvince.StateProvinceID, df_join.TerritoryID, df_join.StateProvinceID)

df_join = df_join.join(df_address_type, df_join.AddressTypeID == df_address_type.AddressTypeID, how='left')\
                .drop(df_address_type.AddressTypeID, "BusinessEntityID", df_bus_ent_address.AddressTypeID)

df_join = df_join.filter("AddressType = 'Main Office'")

df_join = df_join.join(df_territory, df_join.TerritoryID == df_territory.TerritoryID, how='left')\
                .drop(df_join.TerritoryID, df_territory.TerritoryID)

df_join = df_join.join(df_salesperson, df_join.SalesPersonID == df_salesperson.BusinessEntityID, how='left')\
                .drop(df_salesperson.BusinessEntityID)

df_join = df_join.join(df_person, df_join.SalesPersonID == df_person.BusinessEntityID, how='left')\
                .drop(df_person.BusinessEntityID)

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.reseller')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_ind_cust.join(df_person, df_ind_cust.PersonID == df_person.BusinessEntityID, how='left')

df_join = df_join.join(df_bus_ent_address, df_join.BusinessEntityID == df_bus_ent_address.BusinessEntityID, how='left')

df_join = df_join.join(df_address, df_join.AddressID == df_address.AddressID, how='left')\
                .drop(df_address.AddressID, df_bus_ent_address.BusinessEntityID, df_join.AddressID)

df_join = df_join.join(df_stateProvince, df_join.StateProvinceID == df_stateProvince.StateProvinceID, how='left')\
                .drop(df_stateProvince.StateProvinceID, df_join.TerritoryID, df_join.StateProvinceID)

df_join = df_join.join(df_address_type, df_join.AddressTypeID == df_address_type.AddressTypeID, how='left')\
                .drop(df_address_type.AddressTypeID, "BusinessEntityID", df_bus_ent_address.AddressTypeID)

df_join = df_join.filter("AddressType = 'Home'")

df_join = df_join.join(df_territory, df_join.TerritoryID == df_territory.TerritoryID, how='left')\
                .drop(df_join.TerritoryID, df_territory.TerritoryID, df_join.PersonID)

df_join = df_join.withColumn('ModifiedAt', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join.write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .saveAsTable('sil.retailcustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
