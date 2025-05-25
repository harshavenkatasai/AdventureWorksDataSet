# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.twoproject2datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.twoproject2datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.twoproject2datalake.dfs.core.windows.net", "2e456345-731b-4b53-b44d-76bfd25d4a3b")
spark.conf.set("fs.azure.account.oauth2.client.secret.twoproject2datalake.dfs.core.windows.net", "dlH8Q~7tPlVgYU4nuJUkzqnPEy1Oc-u7ujsaYcBC")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.twoproject2datalake.dfs.core.windows.net", "https://login.microsoftonline.com/8b8e27df-59e2-4292-b0e1-f0c70ad2b5d8/oauth2/token")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year


# COMMAND ----------

df_cal_m=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Calendar/AdventureWorks_Calendar.csv')

# COMMAND ----------

df_cal_m.printSchema()
display(df_cal_m)
df_cal_m.count()

# COMMAND ----------

df_cal_m= df_cal_m.withColumn('Date_curr', current_date())
df_cal_m.display()

# COMMAND ----------

df_cal_m= df_cal_m.withColumn("formatted_date", date_format(col("Date"), "dd-MM-yyy"))
df_cal_m.display()




# COMMAND ----------

df_cal_m = df_cal_m.withColumn('Month', month(col('Date')))\
             .withColumn('Year', year(col('Date')))

# COMMAND ----------

df_cal_m.display()


# COMMAND ----------

df_cal_m = df_cal_m.withColumn("Month_Name", date_format(col("Date"), "MMMM"))
df_cal_m.display()

# COMMAND ----------

df_cal_m = df_cal_m.withColumn("Day_Name", date_format(col("Date"), "EEEE"))
df_cal_m.display()

# COMMAND ----------

df_cal_m.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Calendar').save()
dbutils.fs.ls('abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cal_cust=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Customers/AdventureWorks_Customers.csv')

# COMMAND ----------

df_cal_cust.printSchema()
display(df_cal_cust)
df_cal_cust.count()

# COMMAND ----------

df_cal_cust=df_cal_cust.withColumn('FullName',concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName')))
df_cal_cust.display()

# COMMAND ----------

df_cal_cust = df_cal_cust.withColumn("HomeOwner", when(col("HomeOwner") == "Y", "yes")
                                         .when(col("HomeOwner") == "N", "no")
                                         .otherwise(col("HomeOwner")))
df_cal_cust.display()


# COMMAND ----------

df_cal_cust = df_cal_cust.withColumn("MaritalStatus", when(col("MaritalStatus") == "S", "Single")
                                         .when(col("MaritalStatus") == "M", "Maried"))\
                          .withColumn("Gender", when(col("Gender") == "M", "Male")\
                                         .when(col("Gender") == "F", "Female"))
df_cal_cust.display()
df_cal_cust.write.format('parquet').mode('overwrite')                                         
                                        

# COMMAND ----------

df_cal_cust.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Customers').save()

# COMMAND ----------

df_cal_PC=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Product_Categories/AdventureWorks_Product_Categories.csv')

# COMMAND ----------

df_cal_PC.printSchema()
display(df_cal_PC)
df_cal_PC.count()

# COMMAND ----------

df_cal_PC.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Product_Categories').save()


# COMMAND ----------

dbutils.fs.ls('abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_cal_PSC=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Product_Subcategories/AdventureWorks_Product_Subcategories.csv')

# COMMAND ----------

df_cal_PSC.printSchema()
display(df_cal_PSC)
df_cal_PSC.count()

# COMMAND ----------

df_cal_PSC.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Product_Subcategories').save()

# COMMAND ----------

df_cal_P=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Products/AdventureWorks_Products.csv')

# COMMAND ----------

df_cal_P.printSchema()
display(df_cal_P)
df_cal_P.count()

# COMMAND ----------

df_cal_P.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Products').save()

# COMMAND ----------

df_cal_R=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Returns/AdventureWorks_Returns.csv')

# COMMAND ----------

df_cal_R.printSchema()
display(df_cal_R)
df_cal_R.count()

# COMMAND ----------

df_cal_R=df_cal_R.withColumn('Year', year(col('ReturnDate')))
df_cal_R.display()

# COMMAND ----------

df_cal_R=df_cal_R.withColumn('Month', month(col('ReturnDate')))
df_cal_R.display()

# COMMAND ----------

df_cal_R=df_cal_R.withColumn('MonthName', monthname(col('ReturnDate')))
df_cal_R.display()

# COMMAND ----------

df_cal_R.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Returns').save()

# COMMAND ----------

df_cal_S2015=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Sales_2015/AdventureWorks_Sales_2015.csv')

# COMMAND ----------

df_cal_S2015.printSchema()
display(df_cal_S2015)
df_cal_S2015.count()

# COMMAND ----------

df_cal_S2015=df_cal_S2015.withColumn('StockDate',to_timestamp('StockDate'))
df_cal_S2015.display()

# COMMAND ----------

df_cal_S2015=df_cal_S2015.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))
df_cal_S2015.display()

# COMMAND ----------



# COMMAND ----------

df_cal_S2015.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Sales_2015').save()

# COMMAND ----------

df_cal_S2016=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Sales_2016/AdventureWorks_Sales_2016.csv')

# COMMAND ----------

df_cal_S2016.printSchema()
display(df_cal_S2016)
df_cal_S2016.count()

# COMMAND ----------

df_cal_S2016.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Sales_2016').save()

# COMMAND ----------

df_cal_S2017=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Sales_2017/AdventureWorks_Sales_2017.csv')

# COMMAND ----------

df_cal_S2017.printSchema()
display(df_cal_S2017)
df_cal_S2017.count()

# COMMAND ----------

df_cal_S2017.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Sales_2017').save()

# COMMAND ----------

df_cal_T=spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Territories/AdventureWorks_Territories.csv')

# COMMAND ----------

df_cal_T.printSchema()
display(df_cal_T)
df_cal_T.count()

# COMMAND ----------

df_cal_T.write.format('parquet').mode('overwrite').option('path','abfss://silver@twoproject2datalake.dfs.core.windows.net/AdventureWorks_Territories').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Analyisis
# MAGIC

# COMMAND ----------

df_cal_S2015.groupBy('OrderDate').agg(count('orderNumber').alias('total_order')).display()

# COMMAND ----------

df_cal_TotalSales=df_cal_S2015.join(df_cal_S2016,"ProductKey","inner").join(df_cal_S2017,"ProductKey","inner")
df_cal_TotalSales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Orders for each year

# COMMAND ----------

df_cal_S2015.groupBy(year('OrderDate').alias('year')).agg(count('orderNumber').alias('total_order')).display()
df_cal_S2016.groupBy(year('OrderDate').alias('year')).agg(count('orderNumber').alias('total_order')).display()
df_cal_S2017.groupBy(year('OrderDate').alias('year')).agg(count('orderNumber').alias('total_order')).display()


# COMMAND ----------

df_2015 = df_cal_S2015.groupBy(year(col("OrderDate")).alias("year")) \
                      .agg(count("orderNumber").alias("total_order"))
df_2016 = df_cal_S2016.groupBy(year(col("OrderDate")).alias("year")) \
                      .agg(count("orderNumber").alias("total_order"))
df_2017 = df_cal_S2017.groupBy(year(col("OrderDate")).alias("year")) \
                      .agg(count("orderNumber").alias("total_order"))
df_combined = df_2015.union(df_2016).union(df_2017)
df_combined.display()

# COMMAND ----------

df_cal_S2015.display()
df_cal_P.display()
df_cal_PC.display()

# COMMAND ----------

df_product_sales15=df_cal_S2015.join(df_cal_P,"ProductKey","inner").join(df_cal_PC,"ProductSubCategoryKey","inner")
df_product_sales16=df_cal_S2016.join(df_cal_P,"ProductKey","inner").join(df_cal_PC,"ProductSubCategoryKey","inner")
df_product_sales17=df_cal_S2017.join(df_cal_P,"ProductKey","inner").join(df_cal_PC,"ProductSubCategoryKey","inner")




# COMMAND ----------

df_product_sales15.display()

# COMMAND ----------

df_product_G15=df_product_sales15.groupBy('ProductSubcategoryKey',year('OrderDate')).agg(sum('ProductPrice').alias('total_sales'))
df_product_G16=df_product_sales16.groupBy('ProductSubcategoryKey',year('OrderDate')).agg(sum('ProductPrice').alias('total_sales'))
df_product_G17=df_product_sales17.groupBy('ProductSubcategoryKey',year('OrderDate')).agg(sum('ProductPrice').alias('total_sales'))

df_combinedProduct = df_product_G15.union(df_product_G16).union(df_product_G17)
df_combinedProduct.display()

# COMMAND ----------

