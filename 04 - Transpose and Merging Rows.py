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
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Transposing rows to columns and merging rows in Databricks
# MAGIC
# MAGIC In this notebook, we will show how to convert the following SAS codes
# MAGIC * The first example is a SAS macro that transposes rows to columns. We will look at different options on how this could be achieved in Databricks with SparkSQL and Pyspark
# MAGIC * The second example is a SAS code that merges multiple rows to one row grouped by a variable. The Pyspark code shows a step-by-step approach on how to achieve this logic in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SAS code to transpose rows to columns
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAS Code
# MAGIC     /* create a function */
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
# MAGIC     /* flip from row value of segment_desc to column */
# MAGIC     proc transpose data=dataset
# MAGIC     out=step1 (drop=measurement);
# MAGIC     by first last gender age subject;
# MAGIC     var attendance score; 
# MAGIC     run;
# MAGIC
# MAGIC     /* row to column with name change */
# MAGIC     proc transpose data=step1 out=Final_output(drop=_name_) delim=_ ;
# MAGIC     by first last gender age;
# MAGIC     id subject;
# MAGIC     var col1;
# MAGIC     run;
# MAGIC
# MAGIC     /* calling the function */
# MAGIC     %null_to_zero(table_name=Final_output);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW students (first, last, gender, age, subject, attendance, score) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   ('John', 'Doe', 'M', 18, 'Math', 100, 90),
# MAGIC   ('John', 'Doe', 'M', 18, 'History', 100, 95),
# MAGIC   ('Jane', 'Doe', 'M', 17, 'Math', 98, 85),
# MAGIC   ('Jane', 'Doe', 'M', 17, 'History', 99, 88),
# MAGIC   ('Bob', 'Smith', 'M', 18, 'Math', 90, 92),
# MAGIC   ('Bob', 'Smith', 'M', 18, 'History', 92, 87),
# MAGIC   ('Nikki', 'Davis', 'F', 19, 'Math', 99, 80),
# MAGIC   ('Nikki', 'Davis', 'F', 19, 'History', 100, 82),
# MAGIC   ('Mike', 'Adams', 'M', 18, 'Math', 97, 75),
# MAGIC   ('Mike', 'Adams', 'M', 18, 'History', 98, 78),
# MAGIC   ('Tom', 'Myers', 'M', 18, 'Math', 91, 89),
# MAGIC   ('Tom', 'Myers', 'M', 18, 'History', null, null)
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option 1: SQL query with case when statements

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first, last, gender, age, 
# MAGIC COALESCE(math_attendance, 0) AS math_attendance, 
# MAGIC COALESCE(math_score, 0) AS math_score,
# MAGIC COALESCE(history_attendance, 0) AS history_attendance,
# MAGIC COALESCE(history_score, 0) AS history_score FROM (
# MAGIC SELECT first, last, gender, age,
# MAGIC MAX(CASE WHEN subject = "Math" THEN attendance END) AS math_attendance, 
# MAGIC MAX(CASE WHEN subject = "Math" THEN score END) AS math_score, 
# MAGIC MAX(CASE WHEN subject = "History" THEN attendance END) AS history_attendance,
# MAGIC MAX(CASE WHEN subject = "History" THEN score END) AS history_score  
# MAGIC FROM students 
# MAGIC GROUP BY first, last, gender, age
# MAGIC ORDER BY first, last, gender, age
# MAGIC )A

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option 2: SQL query with pivot function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT first, last, gender, age, subject, COALESCE(attendance, 0) as attendance, COALESCE(score, 0) as score
# MAGIC   FROM students
# MAGIC )
# MAGIC PIVOT (
# MAGIC   SUM(attendance) AS attendance, SUM(score) AS score
# MAGIC   FOR subject IN ('Math', 'History') -- Replace with your segment values
# MAGIC ) order by first, last, gender, age
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option 3: Dynamically transposing the "subject" column

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC from pyspark.sql.functions import when, col
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC df = spark.sql("select * from students")
# MAGIC
# MAGIC transposed_df = df.groupBy('first', 'last', 'gender', 'age').pivot('subject').agg(
# MAGIC     F.first('score').alias('score'),
# MAGIC     F.first('attendance').alias('attendance')
# MAGIC )
# MAGIC display(transposed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Defining a function that converts nulls to zeros and using it on the transposed dataset

# COMMAND ----------

# MAGIC %py
# MAGIC def null_to_zero(df):
# MAGIC     numeric_columns = [c for c, t in df.dtypes if t == 'int' or t == 'double' or t == 'bigint']
# MAGIC     for column in numeric_columns:
# MAGIC         df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
# MAGIC     return df

# COMMAND ----------

# MAGIC %py
# MAGIC final_df = null_to_zero(transposed_df)
# MAGIC display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAS code to merge multiple rows to one row by a variable

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAS Code
# MAGIC     data want1;
# MAGIC     length x $40.;
# MAGIC       do until (last.ctn);
# MAGIC         set test;
# MAGIC           by ctn notsorted;
# MAGIC         x=catx(',',x,soc);
# MAGIC         end;
# MAGIC     drop soc;
# MAGIC     run;
# MAGIC
# MAGIC     cnt | soc
# MAGIC     222 | a
# MAGIC     222 | b
# MAGIC     222 | c
# MAGIC     222 | c
# MAGIC     333 | a
# MAGIC     333 | b
# MAGIC     333 | f
# MAGIC
# MAGIC     Results:
# MAGIC     cnt | x
# MAGIC     222 | a,b,c
# MAGIC     333 | a,b,f

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW soc_count (cnt, soc) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   (222, 'a'),
# MAGIC   (222, 'b'),
# MAGIC   (222, 'c'),
# MAGIC   (333, 'a'),
# MAGIC   (333, 'b'),
# MAGIC   (333, 'f')
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a spark dataframe 

# COMMAND ----------

data_df = spark.sql("""
                    select * from soc_count
                    """)
display(data_df)

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import col, concat_ws, when, last, collect_list, collect_set
# MAGIC
# MAGIC # Define the window specification
# MAGIC windowSpec = Window.partitionBy("cnt").orderBy("cnt")
# MAGIC
# MAGIC # Use the last function to get the last value of 'soc' within each partition
# MAGIC df = data_df.withColumn("x", when(last(col("cnt")).over(windowSpec) == col("cnt"), col("soc")).otherwise(None))
# MAGIC
# MAGIC # Aggregate the 'soc' values into a single column 'x' separated by commas
# MAGIC df = df.groupBy("cnt").agg(concat_ws(",", collect_set("x")).alias("x"))
# MAGIC
# MAGIC # Drop the 'soc' column
# MAGIC df = df.drop("soc")
# MAGIC
# MAGIC # Show the resulting DataFrame
# MAGIC display(df)