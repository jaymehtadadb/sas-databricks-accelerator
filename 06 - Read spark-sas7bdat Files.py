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
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Creating Variables in Databricks
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC 1. Read sas7bdat files and create a pandas dataframe
# MAGIC 1. Use Pandas API on Spark

# COMMAND ----------

# MAGIC %md
# MAGIC We will load a sas7bdat file using the [pyreadstat](https://github.com/Roche/pyreadstat) library format option. In order to read in sas7bdat files, we need to install and use the pyreadstat library in our cluster. 

# COMMAND ----------

pip install pyreadstat

# COMMAND ----------

# MAGIC %md
# MAGIC For this example, we are assuming that the SAS dataset is available at Databricks `Workspace` files. [Download](https://docs.github.com/en/get-started/start-your-journey/downloading-files-from-github) sample SAS datasets from this [Github repository](https://github.com/jaymehtadadb/sas-datasets) and [import the files in your Workspace](https://docs.databricks.com/en/files/workspace-basics.html#:~:text=To%20import%20a%20file%2C%20click,be%20imported%20from%20a%20URL.)

# COMMAND ----------

import pyreadstat

# Location of the SAS file. Replace it with your workspace location
filepath = '/Workspace/Shared/sas-files/skinproduct_attributes_seg.sas7bdat'

df, meta = pyreadstat.read_sas7bdat(filepath)

display(df)
print(meta.column_names)
print(meta.column_labels)
print(meta.column_names_to_labels)
print(meta.number_rows)
print(meta.number_columns)
print(meta.file_label)
print(meta.file_encoding)

# COMMAND ----------

display(type(df))

# COMMAND ----------

# MAGIC %md
# MAGIC _Note_: The dataframe `df` is a [Pandas](https://pandas.pydata.org/) dataframe. Pandas is designed for in-memory data processing, which means it can handle datasets that fit within the available RAM. As the dataset size increases, Pandas' performance degrades, and it may run out of memory. Pandas is optimized for single-core processing and may not take full advantage of multi-core processors. This can lead to slower performance on large datasets.
# MAGIC We recommend converting your Pandas dataframe to Spark. Spark is built for scalability, designed for parallel processing and can utilize multiple cores and nodes to process data in parallel, resulting in significant performance gains.
# MAGIC To convert Pandas dataframe to Spark dataframe, we will use [Pandas API on Spark](https://docs.databricks.com/en/pandas/pandas-on-spark.html)

# COMMAND ----------

import pyspark.pandas as ps
spark_df = ps.DataFrame(df)
display(spark_df)

# COMMAND ----------

display(type(spark_df))