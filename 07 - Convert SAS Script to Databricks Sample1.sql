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
-- MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Converting a SAS script to Databricks
-- MAGIC
-- MAGIC In this notebook, you'll learn how to convert an end-to-end SAS script to Databricks Pyspark and Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Complete SAS Code
-- MAGIC 	proc printto log="/app/sasdata/schedule/logs/sales.log" new; run;
-- MAGIC
-- MAGIC 	options compress=yes reuse=yes;
-- MAGIC
-- MAGIC 	proc sql;
-- MAGIC 	create table day_count as 
-- MAGIC 	select activity_date, count(*) as day_count
-- MAGIC 	from new_sales
-- MAGIC 	group by activity_date
-- MAGIC 	order by activity_date;
-- MAGIC 	quit;
-- MAGIC
-- MAGIC 	data day_count; 
-- MAGIC 	set day_count;
-- MAGIC 	by activity_date;
-- MAGIC 	if activity_date ne '' then count + 1;
-- MAGIC 	run;
-- MAGIC
-- MAGIC 	proc sql;
-- MAGIC 	select max(count) into:max_count
-- MAGIC 	from day_count 
-- MAGIC 	;
-- MAGIC 	quit;
-- MAGIC
-- MAGIC 	data _null_;
-- MAGIC 	call symput('max',strip(&max_count.));
-- MAGIC 	run;
-- MAGIC 	%put &max.;
-- MAGIC
-- MAGIC 	%let count_number=1;
-- MAGIC
-- MAGIC 	%put &count_number.;
-- MAGIC
-- MAGIC
-- MAGIC 	%macro ch_nac ();
-- MAGIC 	%do i=&count_number. %to &max.;
-- MAGIC 		proc sql;
-- MAGIC 		select datepart(activity_date) format=yymmddn8. into :activity_date
-- MAGIC 		from day_count where count=&i.;
-- MAGIC
-- MAGIC 		select datepart(activity_date) format=date9. into :activity_date_1
-- MAGIC 		from day_count where count=&i.;
-- MAGIC
-- MAGIC 		%put &activity_date. &activity_date_1.;
-- MAGIC 		
-- MAGIC 		proc sql;
-- MAGIC 		create table ch_nac_&i. as 
-- MAGIC 		select distinct a.*,
-- MAGIC 				b.*
-- MAGIC 		from 
-- MAGIC 			(select *
-- MAGIC 			from new_sales
-- MAGIC 			where datepart(activity_date)="&activity_date_1."d
-- MAGIC 			)a
-- MAGIC 		left join
-- MAGIC 			(select customer_id, customer_location, product_category,
-- MAGIC 					product_price_plan, product_offer, product_code,
-- MAGIC 					product_price, activity_date, closing_code,
-- MAGIC 					sum(product_quantity) as product_quantity, 
-- MAGIC 					sum(product_discount) as product_discount, 
-- MAGIC 					sum(product_price_net) as product_price_net
-- MAGIC 			from closing 
-- MAGIC 			group by enterprise_id, customer_id, customer_location_id, product_lob,
-- MAGIC 					product_price_plan, product_offer, product_tier, product_code,
-- MAGIC 					product_discount_code, activity_date, cr_key
-- MAGIC 			)b	
-- MAGIC 		on a.activity_date=b.activity_date
-- MAGIC 		and a.customer_zip=b.customer_location
-- MAGIC 		and a.customer_id=b.customer_id	
-- MAGIC 		and a.product_code=b.product_code;
-- MAGIC 		quit;
-- MAGIC
-- MAGIC 	%end;
-- MAGIC 	%do i=&count_number. %to &max.;
-- MAGIC 		%if  &i.=&count_number. %then %do;
-- MAGIC 		proc sql;
-- MAGIC 		create table ch_nac_rev as 
-- MAGIC 		select *, &i. as item
-- MAGIC 		from ch_nac_&i	
-- MAGIC 		; quit;   %end;
-- MAGIC 		%if  &i.> &count_number. %then %do;
-- MAGIC 		proc sql;
-- MAGIC 		insert into ch_nac_rev 
-- MAGIC 		select *, &i. as item
-- MAGIC 		from ch_nac_&i; quit; %end;
-- MAGIC 	%end;
-- MAGIC
-- MAGIC 	%do i=&count_number. %to &max.;
-- MAGIC 		proc sql;
-- MAGIC 		drop table ch_nac_&i;
-- MAGIC 	%end;
-- MAGIC
-- MAGIC 	%mend ch_nac;
-- MAGIC
-- MAGIC 	%ch_nac ();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We will break down the SAS code in smaller chunks and convert each of them to Databricks in individual cells

-- COMMAND ----------

-- Create a view to represent the sales data

CREATE OR REPLACE TEMP VIEW new_sales (ACTIVITY_DATE, CUSTOMER_ID, CUSTOMER_ZIP, PRODUCT_CATEGORY, PRODUCT_CODE, PRODUCT_QUANTITY, PRODUCT_PRICE, PRODUCT_DISCOUNT, PRODUCT_PRICE_NET) AS
SELECT * FROM (
VALUES
('17JAN2024:00:00:00', 1024, 07234, 'ELECTRONICS', 1, 1, 200, 10, 190),
('17JAN2024:00:00:00', 1030, 07218, 'HARDWARE', 8, 1, 100, 0, 100),
('17JAN2024:00:00:00', 1021, 09735, 'SOFTWARE', 23, 3, 10, 5, 15),
('18JAN2024:00:00:00', 1024, 07234, 'CLOTHING', 11, 1, 1000, 250, 750),
('18JAN2024:00:00:00', 1028, 07652, 'FURNITURE', 56, 1, 450, 40, 410),
('18JAN2024:00:00:00', 1077, 07452, 'SPECIALTY GOODS', 86, 1, 130, 13, 117),
('18JAN2024:00:00:00', 1123, 07302, 'FURNITURE', 56, 10, 400, 80, 320),
('19JAN2024:00:00:00', 1021, 09735, 'CLOTHING', 11, 1, 200, 10, 190),
('19JAN2024:00:00:00', 1024, 07234, 'CLOTHING', 11, 23, 300, 45, 255),
('20JAN2024:00:00:00', 1030, 07218, 'ELECTRONICS', 1, 10, 200, 20, 180)
);
SELECT * FROM new_sales;

-- COMMAND ----------

-- Create a view to represent the closing data

CREATE OR REPLACE TEMP VIEW closing (CUSTOMER_ID,	CUSTOMER_LOCATION,	PRODUCT_CATEGORY,	PRODUCT_PRICE_PLAN,	PRODUCT_OFFER, PRODUCT_CODE,	PRODUCT_QUANTITY,	PRODUCT_PRICE,	PRODUCT_DISCOUNT, 	PRODUCT_PRICE_NET,	ACTIVITY_DATE,	CLOSING_CODE)
AS SELECT * FROM (
VALUES (1024, 07234, 'ELECTRONICS', '973501132', 515287548, 1, 10, 200, 10, 1900, '24-17-01:12:00:00', 'C'),
(1030, 07218, 'HARDWARE', 'DEFAULT', null, 1, 1, 200, 10, 190, '24-20-01:12:00:00', 'C'),
(1021, 07234, 'SOFTWARE', 'DEFAULT', 515280000, 23, 5, 10, 5, 25, '24-17-01:12:00:00', 'A'),
(1028, 07234, 'FURNITURE', '973501000', 785287548, 56, 1, 450, 40, 410, '24-18-01:12:00:00', 'A'),
(1077, 07234, 'SPECIALTY GOODS', 'SPECIAL', null, 86, 10, 130, 13, 1170, '24-19-01:12:00:00', 'AB'),
(1123, 07234, 'FURNITURE', '973501000', 785287548, 56, 1, 450, 40, 410, '24-18-01:12:00:00', 'C')
);
SELECT * FROM closing;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SAS Code
-- MAGIC     proc sql;
-- MAGIC     create table day_count as 
-- MAGIC     select activity_date, count(*) as day_count
-- MAGIC     from new_sales
-- MAGIC     group by activity_date
-- MAGIC     order by activity_date;
-- MAGIC     quit;

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW day_count AS
SELECT activity_date, COUNT(*) AS day_count
FROM new_sales
GROUP BY activity_date
ORDER BY activity_date;
SELECT * FROM day_count;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SAS Code
-- MAGIC     data day_count; 
-- MAGIC     set day_count;
-- MAGIC     by activity_date;
-- MAGIC     if activity_date ne '' then count + 1;
-- MAGIC     run;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW running_day_count AS
SELECT activity_date, day_count, ROW_NUMBER() OVER (ORDER BY activity_date ASC) as count
FROM day_count;
SELECT * FROM running_day_count;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pyspark.pandas as ps
-- MAGIC running_day_count_df = spark.sql("""SELECT * FROM running_day_count""").pandas_api()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SAS Code
-- MAGIC     data _null_;
-- MAGIC     call symput('max',strip(&max_count.));
-- MAGIC     run;
-- MAGIC     %put &max.;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_count = spark.sql("""
-- MAGIC                       select max(count)as max_count from running_day_count
-- MAGIC                       """).collect()[0][0]
-- MAGIC display(max_count)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SAS Code
-- MAGIC     %let count_number=1;
-- MAGIC
-- MAGIC     %put &count_number.;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count_number = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SAS Code
-- MAGIC     %macro ch_nac ();
-- MAGIC     %do i=&count_number. %to &max.;
-- MAGIC     proc sql;
-- MAGIC     select datepart(activity_date) format=yymmddn8. into :activity_date
-- MAGIC     from day_count where count=&i.;
-- MAGIC
-- MAGIC     select datepart(activity_date) format=date9. into :activity_date_1
-- MAGIC     from day_count where count=&i.;
-- MAGIC
-- MAGIC     %put &activity_date. &activity_date_1.;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table ch_nac_&i. as 
-- MAGIC     select distinct a.*,
-- MAGIC             b.*
-- MAGIC     from 
-- MAGIC         (select *
-- MAGIC         from new_sales
-- MAGIC         where datepart(activity_date)="&activity_date_1."d
-- MAGIC         )a
-- MAGIC     left join
-- MAGIC         (select customer_id, customer_location, product_category,
-- MAGIC                 product_price_plan, product_offer, product_code,
-- MAGIC                 product_price, activity_date, closing_code,
-- MAGIC                 sum(product_quantity) as product_quantity, 
-- MAGIC                 sum(product_discount) as product_discount, 
-- MAGIC                 sum(product_price_net) as product_price_net
-- MAGIC         from closing 
-- MAGIC         group by enterprise_id, customer_id, customer_location_id, product_lob,
-- MAGIC                 product_price_plan, product_offer, product_tier, product_code,
-- MAGIC                 product_discount_code, activity_date, cr_key
-- MAGIC         )b	
-- MAGIC     on a.activity_date=b.activity_date
-- MAGIC     and a.customer_zip=b.customer_location
-- MAGIC     and a.customer_id=b.customer_id	
-- MAGIC     and a.product_code=b.product_code;
-- MAGIC     quit;
-- MAGIC
-- MAGIC     %end;
-- MAGIC     %do i=&count_number. %to &max.;
-- MAGIC     %if  &i.=&count_number. %then %do;
-- MAGIC     proc sql;
-- MAGIC     create table ch_nac_rev as 
-- MAGIC     select *, &i. as item
-- MAGIC     from ch_nac_&i	
-- MAGIC     ; quit;   %end;
-- MAGIC     %if  &i.> &count_number. %then %do;
-- MAGIC     proc sql;
-- MAGIC     insert into ch_nac_rev 
-- MAGIC     select *, &i. as item
-- MAGIC     from ch_nac_&i; quit; %end;
-- MAGIC     %end;
-- MAGIC
-- MAGIC     %do i=&count_number. %to &max.;
-- MAGIC     proc sql;
-- MAGIC     drop table ch_nac_&i;
-- MAGIC     %end;
-- MAGIC
-- MAGIC     %mend ch_nac;
-- MAGIC
-- MAGIC     %ch_nac ();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for i in range(count_number, max_count):
-- MAGIC   temp_view_name = f"ch_nac_{i}"
-- MAGIC   activity_date = running_day_count_df.loc[i, "activity_date"]
-- MAGIC   
-- MAGIC   spark.sql(f"""
-- MAGIC   create or replace temp view {temp_view_name} as 
-- MAGIC 	select distinct 
-- MAGIC   a.ACTIVITY_DATE
-- MAGIC   ,a.CUSTOMER_ID
-- MAGIC   ,a.CUSTOMER_ZIP
-- MAGIC   ,a.PRODUCT_CATEGORY
-- MAGIC   ,b.PRODUCT_PRICE_PLAN
-- MAGIC   ,b.PRODUCT_OFFER
-- MAGIC   ,a.PRODUCT_CODE
-- MAGIC   ,a.PRODUCT_QUANTITY
-- MAGIC   ,a.PRODUCT_PRICE
-- MAGIC   ,a.PRODUCT_DISCOUNT
-- MAGIC   ,a.PRODUCT_PRICE_NET
-- MAGIC   ,b.CLOSING_CODE
-- MAGIC 	from 
-- MAGIC 		(select *
-- MAGIC 		 from new_sales
-- MAGIC 		 where date(to_date(activity_date, "ddMMMyyyy:HH:mm:ss"))=to_date('{activity_date}', "ddMMMyyyy:HH:mm:ss")
-- MAGIC 		)a
-- MAGIC 	left join 
-- MAGIC 		(select CUSTOMER_ID,	CUSTOMER_LOCATION,	PRODUCT_CATEGORY,	PRODUCT_PRICE_PLAN,	PRODUCT_OFFER, PRODUCT_CODE,	PRODUCT_PRICE,	ACTIVITY_DATE,	CLOSING_CODE,
-- MAGIC 				sum(product_quantity) as product_quantity, 
-- MAGIC 				sum(product_discount) as product_discount, 
-- MAGIC 				sum(PRODUCT_PRICE_NET) as PRODUCT_PRICE_NET
-- MAGIC 		from closing 
-- MAGIC 		group by CUSTOMER_ID,	CUSTOMER_LOCATION,	PRODUCT_CATEGORY,	PRODUCT_PRICE_PLAN,	PRODUCT_OFFER, PRODUCT_CODE,	PRODUCT_PRICE,	ACTIVITY_DATE,	CLOSING_CODE
-- MAGIC 		)b	
-- MAGIC 	  on 1=1
-- MAGIC     and FROM_UNIXTIME(UNIX_TIMESTAMP(a.activity_date, 'ddMMMyyyy:HH:mm:ss'), 'yyyy-MM-dd') = date(to_date(b.activity_date, "yy-dd-MM:HH:mm:ss"))
-- MAGIC  	  and a.customer_zip=b.CUSTOMER_LOCATION
-- MAGIC 	  and a.customer_id=b.customer_id	
-- MAGIC     and a.product_code=b.product_code
-- MAGIC             """)
-- MAGIC   print(f"Temp view created for {activity_date}. View name {temp_view_name}")
-- MAGIC   if i == count_number:
-- MAGIC     spark.sql(f"""
-- MAGIC               create or replace temp view ch_nac_rev as 
-- MAGIC               select *, {i} as item
-- MAGIC               from {temp_view_name}
-- MAGIC               """)
-- MAGIC     print(f"Data from temp view {temp_view_name} appended to ch_nac_rev")
-- MAGIC   else:
-- MAGIC     new_df = spark.sql(f"""
-- MAGIC               select *, {i} as item
-- MAGIC               from {temp_view_name}
-- MAGIC               """)
-- MAGIC     existing_df = spark.sql("""select * from ch_nac_rev""")
-- MAGIC     combined_df = new_df.union(existing_df)
-- MAGIC     combined_df.createOrReplaceTempView("ch_nac_rev")
-- MAGIC     print(f"Data from temp view {temp_view_name} appended to ch_nac_rev")

-- COMMAND ----------

SELECT * FROM ch_nac_rev