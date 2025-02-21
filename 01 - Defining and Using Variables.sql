-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Creating Variables in Databricks
-- MAGIC
-- MAGIC In this notebook, you'll learn different options available to create variables in Databricks.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC 1. Learn how to create variables in Databricks with three options
-- MAGIC 1. Understand how to use variables in your code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sample Dataset
-- MAGIC Creating two temporary views **`customers`** and **`products`** that we will use as sample datasets for this lesson

-- COMMAND ----------

-- Creating temp view customers
CREATE OR REPLACE TEMP VIEW customers (cust_id, first, last, location) AS
SELECT * FROM (
VALUES
  (1, 'John', 'Doe', 'Chicago'),
  (2, 'Jane', 'Doe', 'New York'),
  (3, 'Bob', 'Smith', 'San Francisco'),
  (4, 'Mike', 'Adams', 'Chicago'),
  (5, 'Tom', 'Myers', 'New York'),
  (6, 'Nikki', 'Davis', 'San Francisco')
  );

-- Creating temp view products
CREATE OR REPLACE TEMP VIEW products (cust_id, product) AS
SELECT * FROM (
VALUES
  (1, 'lamp'),
  (2, 'stapler'),
  (3, 'pencil'),
  (4, 'pen'),
  (5, 'lamp'),
  (6, 'pencil'),
  (1, 'pen')
  );

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

SELECT * FROM products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## SQL variable
-- MAGIC
-- MAGIC Users can set and use <a href="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-set-variable.html" target="_blank">Variables</a> in Databricks. The syntax below demonstrates the essentials required to create SQL variables
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC SET { VAR | VARIABLE } { variable_name = { expression | DEFAULT } } [, ...] <br/>
-- MAGIC SET { VAR | VARIABLE } ( variable_name [, ...] ) = ( query ) } <br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC **NOTE**: The cluster you are using needs Databricks Runtime 14.1 and above to use this option

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC In the example below, we are defining variable **`myvar1`** and using it to filter **`customers`** view

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE myvar1 STRING DEFAULT 'John';

-- COMMAND ----------

SELECT * FROM CUSTOMERS WHERE FIRST = myvar1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC In the next example, we are defining variable **`myvar2`** and setting it's value as **`cust_id`** with the most orders and using it to filter **`customers`** view

-- COMMAND ----------

DECLARE OR REPLACE myvar2 STRING;

SET VAR myvar2 = (
  select cust_id from (
    SELECT cust_id, RANK() OVER (ORDER BY cnt desc) as rnk 
    from (
      SELECT cust_id, count(*) as cnt from products group by cust_id
      )
      ) where rnk = 1);

SELECT * FROM customers WHERE cust_id = myvar2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Python variables and f string
-- MAGIC
-- MAGIC Users can set variables in python and use the variables within their spark.sql api query. The syntax below demonstrates the essentials required to a python variable and use it in your query
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC var = "<some_value>" <br/>
-- MAGIC spark.sql(f"""
-- MAGIC           select * from table where colA = '{var}'<br/>
-- MAGIC           """)
-- MAGIC </code></strong>

-- COMMAND ----------

-- MAGIC %py
-- MAGIC last_name = "Doe"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""create or replace temporary view vw_cust_doe as 
-- MAGIC           SELECT * from customers where last = '{last_name}'
-- MAGIC         """)

-- COMMAND ----------

select * from vw_cust_doe

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating a variable **`cust_id`** and using it in the query

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cust_id = 1
-- MAGIC query = f"""
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW vw_cust_1 AS  
-- MAGIC         SELECT *
-- MAGIC         FROM customers
-- MAGIC         WHERE cust_id = {cust_id}
-- MAGIC         """
-- MAGIC display(query)
-- MAGIC spark.sql(query)

-- COMMAND ----------

select * from vw_cust_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Python variables and format method
-- MAGIC
-- MAGIC The **`format()`** method formats the specified value(s) and insert them inside the string's placeholder.<br/>
-- MAGIC The placeholder is defined using curly brackets: {}
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC var = "<some_value>" <br/>
-- MAGIC spark.sql("""
-- MAGIC           select * from table where colA = '{var}'<br/>
-- MAGIC           """.format(var=var))
-- MAGIC </code></strong>
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC city = "Chicago"
-- MAGIC product = "lamp"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql("""
-- MAGIC           create or replace temporary view vw_customers_chicago as 
-- MAGIC           select * from customers 
-- MAGIC           where location = '{city}'
-- MAGIC           """.format(city=city))

-- COMMAND ----------

select * from vw_customers_chicago

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(
-- MAGIC     """
-- MAGIC           create or replace temporary view vw_sales_chicago as 
-- MAGIC           select c.cust_id, c.first, c.last, c.location, p.product 
-- MAGIC           from customers c inner join products p
-- MAGIC           on c.cust_id = p.cust_id
-- MAGIC           where location = '{city}' and product = '{product}'
-- MAGIC           """.format(
-- MAGIC         city=city, product=product
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

select * from vw_sales_chicago