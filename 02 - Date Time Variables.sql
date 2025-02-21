-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Managing date and time variables when converting SAS code to Pyspark
-- MAGIC
-- MAGIC Datetime variables are widely used in SAS programming and in this notebook, you'll learn how to define different datetime variables and use them in your code
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC 1. Try out few Databricks <a href="https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html" target="_blank">built-in functions</a> 
-- MAGIC 1. Learn how to create datetime variables
-- MAGIC 1. Demonstrate translating SAS datetime variables to Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `dateadd`   
-- MAGIC Use the <a href="https://docs.databricks.com/en/sql/language-manual/functions/dateadd.html" target="_blank">dateadd function</a> to add value units to a timestamp expr

-- COMMAND ----------

SELECT dateadd(DAY, 5, '2022-01-01');

-- COMMAND ----------

SELECT dateadd(MONTH, -1, '2022-03-11');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `to_date`   
-- MAGIC Use the <a href="https://docs.databricks.com/en/sql/language-manual/functions/to_date.html" target="_blank">to_date function</a> to format date columns

-- COMMAND ----------

select to_date("31DEC2017:00:00", "ddMMMyyyy:HH:mm") as date;

-- COMMAND ----------

select to_date("29112023", "ddMMyyyy") as date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `year`   
-- MAGIC To extract the year from a date as an integer, like with the SAS `year()` function, use the <a href="https://docs.databricks.com/en/sql/language-manual/functions/year.html" target="_blank">year function</a>
-- MAGIC If the date is a string, convert it to date using `to_date()` function and extract year using the `year` function

-- COMMAND ----------

select year(to_date("01JUL2020:00:00:02", "ddMMMyyyy:HH:mm:ss")) as year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### `date_format`   
-- MAGIC Extract various date formats from a datetime column using <a href="https://docs.databricks.com/en/sql/language-manual/functions/date_format.html" target="_blank">date_format function</a>

-- COMMAND ----------

-- Create some temp views so we can demonstrate syntax
CREATE OR REPLACE TEMP VIEW orders (order_id, cust_id, transacted_at) AS
SELECT * FROM (
VALUES
  (100, 1, '2022-07-01 10:00:00'),
  (101, 2, '2024-07-18 11:22:10'),
  (102, 1, '2024-07-19 17:35:00'),
  (103, 5, '2024-07-21 06:10:13')
  );

-- COMMAND ----------

SELECT date_format(transacted_at, "MMMM dd, yyyy") AS date, date_format(transacted_at, "HH:mm:ss.SSSSSS") as timestamp from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Let's convert actual SAS variables to SQL variables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### In SAS, defining the date time variables might look like:
-- MAGIC
-- MAGIC ```
-- MAGIC   %let var_x = 0
-- MAGIC   CALL SYMPUT('var_year',substr(PUT(intnx('MONTH',today(),&var_x,"B"),YYMMN6.)1,4));
-- MAGIC   CALL SYMPUT('var_month',substr(PUT(intnx('MONTH',today(),&var_x,"B"),YYMMN6.)5,2));
-- MAGIC   CALL SYMPUT('var_monthname',substr(strip(put(intnx('MONTH',today(),&var_x,"B"),MONNAME9.))1,3));
-- MAGIC   CALL SYMPUT('var_year_minus',substr(put(intnx('MONTH',today(),&var_x-1,"B"),YYMMN6.)1,4));
-- MAGIC   CALL SYMPUT('var_month_minus',substr(put(intnx('MONTH',today(),&var_x-1,"B"),YYMMN6.)5,2));
-- MAGIC   CALL SYMPUT('var_monthname_minus',substr(strin(put(intnx('MONTH',today(),&var_x-1,"B"),MONNAME9.))1,3));
-- MAGIC   CALL SYMPUT('var_date1',PUT(intnx('MONTH',today(),&var_x,"B"),EURDFDE9.));
-- MAGIC   CALL SYMPUT('var_date2',PUT(intnx('MONTH',today(),&var_x,"E"),EURDFDE9.));
-- MAGIC ```

-- COMMAND ----------

-- %let var_x = 0

DECLARE OR REPLACE VARIABLE var_x INT;
SET VAR var_x = 0;
SELECT var_x;

-- COMMAND ----------

-- CALL SYMPUT('var_year',substr(PUT(intnx('MONTH',today(),&var_x,"B"),YYMMN6.)1,4));

DECLARE OR REPLACE VARIABLE var_year STRING;
SET VAR var_year = (SELECT YEAR(DATEADD(MONTH, var_x, current_date())));
SELECT var_year;

-- COMMAND ----------

-- CALL SYMPUT('var_month',substr(PUT(intnx('MONTH',today(),&var_x,"B"),YYMMN6.)5,2));

DECLARE OR REPLACE VARIABLE var_month STRING;
SET VAR var_month = (SELECT LPAD(CAST(MONTH(DATEADD(MONTH, var_x, current_date())) AS STRING), 2, '0'));
SELECT var_month;

-- COMMAND ----------

-- CALL SYMPUT('var_monthname',substr(strip(put(intnx('MONTH',today(),&var_x,"B"),MONNAME9.))1,3));

DECLARE OR REPLACE VARIABLE month_name STRING;
SET VAR month_name = (SELECT MONTHNAME(DATEADD(MONTH, var_x, current_date())));
SELECT month_name;

-- COMMAND ----------

-- CALL SYMPUT('var_year_minus',substr(put(intnx('MONTH',today(),&var_x-1,"B"),YYMMN6.)1,4));

DECLARE OR REPLACE VARIABLE var_year_minus STRING;
SET VAR var_year_minus = (SELECT YEAR(DATEADD(MONTH, var_x-1, current_date())));
SELECT var_year_minus;

-- COMMAND ----------

-- CALL SYMPUT('var_month_minus',substr(put(intnx('MONTH',today(),&var_x-1,"B"),YYMMN6.)5,2));

DECLARE OR REPLACE VARIABLE var_month_minus STRING;
SET VAR var_month_minus = (SELECT LPAD(CAST(MONTH(DATEADD(MONTH, var_x-1, current_date())) AS STRING), 2, '0'));
SELECT var_month_minus;

-- COMMAND ----------

-- CALL SYMPUT('var_monthname_minus',substr(strin(put(intnx('MONTH',today(),&var_x-1,"B"),MONNAME9.))1,3));

DECLARE OR REPLACE VARIABLE var_monthname_minus STRING;
SET VAR var_monthname_minus = (SELECT MONTHNAME(DATEADD(MONTH, var_x-1, current_date())));
SELECT var_monthname_minus;

-- COMMAND ----------

-- CALL SYMPUT('var_date1',PUT(intnx('MONTH',today(),&var_x,"B"),EURDFDE9.));

DECLARE OR REPLACE VARIABLE var_date1 STRING;
SET VAR var_date1 = (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATEADD(MONTH, var_x, current_date())), 'ddMMMyyyy'));
SELECT var_date1;

-- COMMAND ----------

-- CALL SYMPUT('var_date2',PUT(intnx('MONTH',today(),&var_x,"E"),EURDFDE9.));

DECLARE OR REPLACE VARIABLE var_date2 STRING;
SET VAR var_date2 = (SELECT DATE_FORMAT(LAST_DAY(DATEADD(MONTH, var_x, current_date())), 'ddMMMyyyy'));
SELECT var_date2;