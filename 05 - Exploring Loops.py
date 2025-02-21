# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Loops in Databricks
# MAGIC
# MAGIC Loops are very commonly used in SAS programming. In this notebook, you'll learn how to create loops in Databricks. We will consider two use cases for this notebook and show different options of creating loops 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC 1. Learn how to create loops in Databricks with different options
# MAGIC 1. Understand which option to use when dealing with high data volume

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use case 1: Create a view for each product and calculate distinct customer count

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create some temp views so we can demonstrate syntax
# MAGIC CREATE OR REPLACE TEMP VIEW customers (cust_id, first, last, location) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   (1, 'John', 'Doe', 'Chicago'),
# MAGIC   (2, 'Jane', 'Doe', 'New York'),
# MAGIC   (3, 'Bob', 'Smith', 'San Francisco'),
# MAGIC   (4, 'Mike', 'Adams', 'Chicago'),
# MAGIC   (5, 'Tom', 'Myers', 'New York'),
# MAGIC   (6, 'Nikki', 'Davis', 'San Francisco')
# MAGIC   );
# MAGIC CREATE OR REPLACE TEMP VIEW products (cust_id, product) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   (1, 'lamp'),
# MAGIC   (2, 'stapler'),
# MAGIC   (3, 'pencil'),
# MAGIC   (4, 'pen'),
# MAGIC   (5, 'lamp'),
# MAGIC   (6, 'pencil'),
# MAGIC   (1, 'pen')
# MAGIC   );

# COMMAND ----------

unique_product_df = spark.sql("select distinct product from products")
display(unique_product_df)
type(unique_product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Option 1: Using spark dataframe and collect() method <br/>
# MAGIC **`collect()`** returns all the elements of the dataset as an array at the driver

# COMMAND ----------

for row in unique_product_df.collect():
    product_var = row["product"]
    print(f"running loop for {product_var}")
    dynamic_view_name = f"vw_dim_{product_var}"
    print(f"{dynamic_view_name} view is created")
    spark.sql(f"""
        create or replace temporary view {dynamic_view_name} as
        select count(distinct cust_id) as unique_cust_count 
        from products
        where product = '{product_var}'
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dim_lamp

# COMMAND ----------

# MAGIC %md
# MAGIC _Note_: One of the major drawbacks of collect is that it brings all the data to the driver node, which can cause memory issues if the dataset is too large. The driver node needs to have enough memory to handle the entire dataset, and it may lead to out-of-memory errors

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Option 2: Using Pandas API on Spark <br/>
# MAGIC
# MAGIC Pandas is a Python package that provides easy-to-use data structures and data analysis tools for the Python programming language. However, pandas does not scale out to big data. Pandas API on Spark fills this gap by providing pandas equivalent APIs that work on Apache Spark

# COMMAND ----------

import pyspark.pandas as ps
pandas_unique_product_df = unique_product_df.pandas_api()

for i in range(0, len(pandas_unique_product_df)):
  product_var = pandas_unique_product_df.loc[i, "product"]
  print(f"running loop for {product_var}")
  dynamic_view_name = f"vw_dim_{product_var}"
  print(f"{dynamic_view_name} view is created")
  spark.sql(f"""
            create or replace temporary view {dynamic_view_name} as
            select * from products
            where product = '{product_var}'
            """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dim_pen

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use case 2: Calculate rolling sum of **`sale_price`** by **`cust_id`** without using any built-in function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create temp view so we can demonstrate syntax
# MAGIC CREATE OR REPLACE TEMP VIEW orders (order_date, cust_name, sale_price) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   ('2024-07-01', 'John', 100),
# MAGIC   ('2024-07-02', 'John', 200),
# MAGIC   ('2024-07-03', 'John', 50),
# MAGIC   ('2024-07-01', 'Tim', 100),
# MAGIC   ('2024-07-02', 'Tim', 400),
# MAGIC   ('2024-07-03', 'Tim', 700)
# MAGIC   );

# COMMAND ----------

# create a dataframe

df = spark.table("orders")
display(df)

# COMMAND ----------

# creating a dataframe using spark sql and adding row_num column using window function

spark_df = spark.sql("""
                     SELECT order_date, cust_name, sale_price,
                     ROW_NUMBER() OVER (PARTITION BY cust_name ORDER BY order_date) as row_num
                     FROM orders
                     """)
display(spark_df)

# COMMAND ----------

# utilizing pandas api on spark to create a pyspark.pandas dataframe

pandas_df = spark_df.pandas_api()
display(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Defining a python function with **`for`** loop that will iterate through each row of the dataframe. <br/>
# MAGIC Using **`.loc[]`** method to access the columns in the dataframe

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC def rolling_sum_function(xdf):
# MAGIC
# MAGIC   for i in range(0, len(xdf)):
# MAGIC     if xdf.loc[i, 'row_num'] == 1:
# MAGIC       xdf.loc[i, 'rolling_sum'] = xdf.loc[i, 'sale_price']
# MAGIC     else:
# MAGIC       xdf.loc[i, 'rolling_sum'] = xdf.loc[i-1, 'rolling_sum'] + xdf.loc[i, 'sale_price']
# MAGIC   return xdf

# COMMAND ----------

# calling the function and passing the dataframe as parameter to calculate the rolling sum

result_df = rolling_sum_function(pandas_df)
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### When working with high data volume, Python UDF (example above) might suffer from high serialization and invocation overhead because they operate one-row-at-a-time. <br/>
# MAGIC ##### An efficient way to write the above function would be through <a href="https://docs.databricks.com/en/udf/pandas.html" target="_blank">Pandas UDFs</a>
# MAGIC Pandas UDF are built on top of Apache Arrow bring you the best of both worlds—the ability to define low-overhead, high-performance UDFs entirely in Python

# COMMAND ----------

# MAGIC %md
# MAGIC We will use **`Grouped Map`** Pandas API. You transform your grouped data using groupBy().applyInPandas() to implement the “split-apply-combine” pattern. 
# MAGIC
# MAGIC To use groupBy().applyInPandas(), you must define the following:
# MAGIC 1. A Python function that defines the computation for each group
# MAGIC 2. A StructType object or a string that defines the schema of the output DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC Defining the Python function

# COMMAND ----------

def udf_rolling_sum_function(xdf):
  for i in range(0, len(xdf)):
    if xdf.loc[i, 'row_num'] == 1:
      xdf.loc[i, 'rolling_sum'] = xdf.loc[i, 'sale_price']
    else:
      xdf.loc[i, 'rolling_sum'] = xdf.loc[i-1, 'rolling_sum'] + xdf.loc[i, 'sale_price']
  return xdf

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a string that defines the schema

# COMMAND ----------

schema = "order_date string, cust_name string, sale_price int, row_num int, rolling_sum double"

# COMMAND ----------

final_df=spark_df.groupby(["cust_name"]).applyInPandas(udf_rolling_sum_function, schema = schema)
display(final_df)