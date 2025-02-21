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
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Creating Macros in Databricks
# MAGIC
# MAGIC In this notebook, we will convert two SAS macros to pyspark. In Databricks, the equivalent functionality for SAS Macros can be achieved using python functions. While Python does not have a direct equivalent to SAS Macros, you can use functions and classes to perform macro tasks and achieve code reusability.
# MAGIC * The first example is translating a SAS macro that converts nulls to zero
# MAGIC * The second example is translating a SAS macro that calculates the sum of first **`x`** elements in a Fibonacci series
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC 1. Learn how to convert SAS macros

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: SAS macro to convert nulls to zero
# MAGIC     %macro null_to_zero (table_name=&table_name.);
# MAGIC     data &table_name.;
# MAGIC     set &table_name.;
# MAGIC     array change _numeric_;
# MAGIC     do over change;
# MAGIC     if change=. then change=0;
# MAGIC     end;
# MAGIC     run ;
# MAGIC     %mend null_to_zero ;
# MAGIC
# MAGIC     /* calling the function */
# MAGIC     %null_to_zero(table_name=Final_output);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create some temp views so we can demonstrate syntax
# MAGIC
# MAGIC   CREATE OR REPLACE TEMP VIEW orders (cust_id, product_id, quantity, delivery_zip, order_date) AS
# MAGIC   SELECT * FROM (
# MAGIC   VALUES
# MAGIC     (1, null, 10, 07123, '2022-01-01'),
# MAGIC     (2, 1001, null, 06352, '2022-02-02'),
# MAGIC     (3, 2361, null, 09826, '2022-10-11'),
# MAGIC     (1, 7493, 2, null, '2022-03-28'),
# MAGIC     (6, 7389, 1, null, '2022-05-21')
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %py
# MAGIC from pyspark.sql.functions import when, col
# MAGIC
# MAGIC def null_to_zero(df):
# MAGIC     numeric_columns = [c for c, t in df.dtypes if t == 'int' or t == 'double' or t == 'bigint']
# MAGIC     for column in numeric_columns:
# MAGIC         df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
# MAGIC     return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a dataframe for **`orders`** view and calling the **`null_to_zero`** function

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC df = spark.sql("select * from orders")
# MAGIC final_df = null_to_zero(df)
# MAGIC display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert the dataframe back to temporary view or write it to a delta table

# COMMAND ----------

final_df.createOrReplaceTempView("vw_orders")

# To save the final results to a table, uncomment the following line:
# final_df.write.mode("overwrite").saveAsTable("<table_name>")
# mode value can be "append" if you want to add new rows to an existing table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_orders

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SAS Macros for Fibonacci series
# MAGIC
# MAGIC For use to evaluate macro capability in python we'll look at writing a function that will take the sum of the first `x` elements of the Fibonacci series.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Example 2: SAS Code for Fibonacci series
# MAGIC     %macro fibonacci(n);
# MAGIC         %if &n <= 0 %then 0;
# MAGIC         %else %if &n = 1 %then 1;
# MAGIC         %else %eval(%fibonacci(%eval(&n - 1)) + %fibonacci(%eval(&n - 2)));
# MAGIC     %mend fibonacci;
# MAGIC
# MAGIC     %macro fibonacci_series(x);
# MAGIC         %if &x <= 0 %then %do;
# MAGIC             %put ERROR: 'x' must be a positive integer greater than zero.;
# MAGIC             %return;
# MAGIC         %end;
# MAGIC
# MAGIC         %put Fibonacci Series with &x elements:;
# MAGIC     
# MAGIC         %local i;
# MAGIC         %do i = 0 %to &x -1;
# MAGIC             %put %eval(%fibonacci(&i));
# MAGIC         %end;
# MAGIC
# MAGIC     %mend fibonacci_series;
# MAGIC
# MAGIC     %macro fibonacci_series_sum(x);
# MAGIC         %if &x <= 0 %then %do;
# MAGIC             %put ERROR: 'x' must be a positive integer greater than zero.;
# MAGIC             %return;
# MAGIC         %end;
# MAGIC
# MAGIC         %let total_sum = 0; /* Initialize the cumulative sum */
# MAGIC
# MAGIC         %local i;
# MAGIC         %do i = 0 %to &x -1;
# MAGIC             %let total_sum = %eval(&total_sum + %fibonacci(&i));
# MAGIC         %end;
# MAGIC
# MAGIC         %put The sum of the Fibonacci Series with &x elements is &total_sum elements.;
# MAGIC
# MAGIC     %mend fibonacci_series_sum;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We've successfully written three macros in SAS using the loop functionality that come in the SAS macro language. We will write the same functions in python:
# MAGIC
# MAGIC | function      | decription |
# MAGIC | ------------- | ---------- |
# MAGIC | fibonacci(n)  | return the n-th value of the fibonacci series. |
# MAGIC | fibonacci_series(x) | %put or print the fibonacci series for the first x elements. | 
# MAGIC | fibonacci_series_sum(x) | %put or print the sum of the first x elements in the fibonacci series. |

# COMMAND ----------

def fibonacci(n):
    if n <= 0:
        raise ValueError("ERROR: 'n' must be a positive integer greater than zero.")
    elif n == 1:
        return 0
    elif n == 2:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)

def fibonacci_series(x):
    if x <= 0:
        raise ValueError("ERROR: 'x' must be a positive integer greater than zero.")
    print(f"Fibonacci Series with {x} elements:")
    for i in range(1, x + 1):
        print(fibonacci(i))

def fibonacci_series_sum(x):
    if x <= 0:
        raise ValueError("ERROR: 'x' must be a positive integer greater than zero.")
    total_sum = 0; # Initialize the cumulative sum
    for i in range(1, x + 1):
        total_sum = total_sum + fibonacci(i)
    print(f"The sum of the Fibonacci Series with {x} elements is {total_sum} elements.")



# COMMAND ----------

fibonacci_series(10)

# COMMAND ----------

fibonacci_series_sum(10)