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
-- MAGIC In the example, we will go through a portition of the SAS code and look at the converted Pyspark code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SAS Code 
-- MAGIC
-- MAGIC     LIBNAME TARGET BASE "/sasmnt/VARW/SAS-to-DTB/TARGET";`
-- MAGIC
-- MAGIC     libname APP_CT ORACLE USER=XXYYZZ PASSWORD='{XXXX}'
-- MAGIC     path= '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dummy.com)(PORT=9999))
-- MAGIC     (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=rxzDIN_SN)))'
-- MAGIC     SCHEMA=APP_CT READBUFF=1 SQL_FUNCTIONS=ALL;
-- MAGIC     
-- MAGIC     libname SAS_ATDM ORACLE USER=XXYYZZ PASSWORD='{XXXX}'
-- MAGIC     path= '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dummy.com)(PORT=9999))
-- MAGIC     (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=RTDMPRS)))'
-- MAGIC     SCHEMA=SAS_ATDM READBUFF=1 SQL_FUNCTIONS=ALL;
-- MAGIC     
-- MAGIC     data _null_;
-- MAGIC     %global today;
-- MAGIC     %global time;
-- MAGIC     tod_str = compress(put(dateTIME(),datetime14.), ':');    
-- MAGIC     call symput('today', compress(tod_str));
-- MAGIC     call symput('time', compress(put(dateTIME(),timeampm.)));
-- MAGIC     run;
-- MAGIC     
-- MAGIC     %let model_path = /sasmnt/VARW/SAS-to-DTB/SOURCE;
-- MAGIC     
-- MAGIC     %let export_path = /sasmnt/VARW/SAS-to-DTB/TARGET;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime as dttm
-- MAGIC
-- MAGIC # Defining variables
-- MAGIC today = dttm.date.today().strftime("%Y-%m-%d")
-- MAGIC time = dttm.datetime.now().strftime("%H-%M-%S")
-- MAGIC #model_path = "/Workspace/Users/jay.mehta@databricks.com/SAS-Databricks-Workshop/Includes/model_score_fake.csv"
-- MAGIC #export_path = "/Workspace/Users/jay.mehta@databricks.com/SAS-Databricks-Workshop/Target/"
-- MAGIC
-- MAGIC display(f"today variable value - {today}")
-- MAGIC display(f"time variable value  - {time}")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW report_hist (event_dt, business, service_type, disposition, offer, id) AS
SELECT * FROM (
VALUES
('2024-07-20', 'retail', 'renewal', 'accepted', 'offer', 221792230),
('2024-07-20', 'media', 'recommended plan', 'accepted', 'offer', 18338485),
('2024-07-20', 'software', 'add new service', 'declined', 'offer', 216970340),
('2024-07-20', 'media', 'renewal', 'accepted', 'offer', 204413605),
('2024-07-20', 'hardware', 'renewal', 'accepted', 'action', 221792230),
('2024-07-21', 'hardware', 'renewal', 'accepted', 'offer', 224565620),
('2024-07-21', 'hardware', 'recommended plan', 'accepted', 'offer', 221247767),
('2024-07-21', 'media', 'renewal', 'accepted', 'offer', 16334019),
('2024-07-21', 'retail', 'recommended plan', 'generated', 'action', 221792230),
('2024-07-21', 'retail', 'add new service', 'generated', 'offer', 221792230)
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW subscriber_activity (activity_dt, activity, account_id, customer_id) AS
SELECT * FROM (
VALUES
('2024-07-19', 'CANC', 32006835, 221792230),
('2024-07-19', 'OP', 32047821, 18338485),
('2024-07-19', 'NSO', 34070419, 216970340),
('2024-07-20', 'NSI', 34275022, 204413605),
('2024-07-20', 'CHO', 35657325, 221792230),
('2024-07-21', 'OP', 39429052, 224565620),
('2024-07-21', 'AMSDS', 39429052, 224565620),
('2024-07-21', 'CL', 39429052, 224565620),
('2024-07-23', 'CL', 32006835, 221792230),
('2024-07-23', 'OP', 32047821, 18338485),
('2024-07-23', 'CL', 39429052, 224565620),
('2024-07-23', 'AMSDS', 39429053, 221247767),
('2024-07-23', 'CANC', 39429054, 16334019)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SAS Code
-- MAGIC
-- MAGIC `%let last_report_max_date;`
-- MAGIC
-- MAGIC
-- MAGIC     proc sql;
-- MAGIC     select
-- MAGIC     datepart(max(EVENT_DT)) format=yymmdd10.
-- MAGIC     into: report_min_date
-- MAGIC     from TARGET.RTDM_FULFILLMENT_REPORT_HIST;
-- MAGIC     quit;

-- COMMAND ----------

select * from report_hist

-- COMMAND ----------

DECLARE OR REPLACE last_report_max_date STRING;

SET VAR last_report_max_date = (SELECT MAX(event_dt) FROM report_hist);

SELECT last_report_max_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SAS Code 
-- MAGIC
-- MAGIC     /*SOURCE DATA DOWNLOAD*/
-- MAGIC     /*BDW Data*/
-- MAGIC     proc sql;
-- MAGIC     CREATE TABLE WORK.BDW_DATA AS
-- MAGIC     SELECT DISTINCT activity_dt, customer_id FROM subscriber_activity
-- MAGIC         WHERE activity_dt >= last_report_max_date
-- MAGIC         AND activity IN ('OP', 'CL');
-- MAGIC     ;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     /*RTDM Data*/
-- MAGIC     proc sql;
-- MAGIC     CREATE TABLE WORK.RTDM_DATA AS
-- MAGIC     SELECT distinct event_dt,
-- MAGIC     ID,
-- MAGIC     (case when business IN( 'retail','media') then 'X' else 'Y' end) AS BRAND,  
-- MAGIC     disposition,
-- MAGIC     offer
-- MAGIC     FROM report_hist
-- MAGIC     WHERE
-- MAGIC     event_dt >= last_report_max_date
-- MAGIC     and offer = 'offer';
-- MAGIC     ;
-- MAGIC   
-- MAGIC     quit;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW bdw_data AS
SELECT DISTINCT activity_dt, customer_id FROM subscriber_activity
WHERE activity_dt >= last_report_max_date
AND activity IN ('OP', 'CL');
SELECT * FROM bdw_data;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rtdm_data AS
SELECT DISTINCT event_dt,
  ID,
  (case when business IN( 'retail','media') then 'X' else 'Y' end) AS BRAND,  
  disposition,
  offer
FROM report_hist
WHERE
     event_dt >= last_report_max_date
 and offer = 'offer';
SELECT * FROM rtdm_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SAS Code 
-- MAGIC
-- MAGIC `/*Import Model Scores*/`
-- MAGIC
-- MAGIC     proc import out=WORK.MODEL_SCORES`
-- MAGIC     datafile = "&model_path/Model_Scores.xlsx"`
-- MAGIC     dbms = xlsx replace;
-- MAGIC     getnames = yes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We are assuming that file is imported and data is ingested in a Databricks table. Refer to <a href="https://docs.databricks.com/en/ingestion/add-data/upload-data.html" target="_blank">documentation</a> for steps to upload files in Databricks

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW model_score (score, id) AS
SELECT * FROM (
VALUES
(0.98,221792230),
(0.9,18338485),
(0.08,216970340),
(0.72,204413605),
(0.8,203963669),
(0.66,227055436),
(0.02,226209103),
(0.52,223030376),
(0.1,19480457),
(0.48,214695185),
(0.87,226239983),
(0.27,219038646),
(0.5,228256675),
(0.37,224565620),
(0.01,217224348),
(0.4,221247767),
(0.15,16334019),
(0.23,221763907),
(0.67,226187966)
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SAS Code 
-- MAGIC
-- MAGIC     proc summary data=WORK.MODEL_SCORES;
-- MAGIC     class ECID;
-- MAGIC     var Score;
-- MAGIC     output out=WORK.MODEL_SCORES_AGGREGATED max=;
-- MAGIC     run;
-- MAGIC
-- MAGIC     proc sort data=WORK.RTDM_DATA;
-- MAGIC     by ECID;
-- MAGIC     run;
-- MAGIC
-- MAGIC     proc sort data=WORK.MODEL_SCORES_AGGREGATED;
-- MAGIC     by ECID;
-- MAGIC     run;
-- MAGIC
-- MAGIC     data WORK.RTDM_MODEL;
-- MAGIC     merge WORK.RTDM_DATA (in = in_rtdm)
-- MAGIC           WORK.MODEL_SCORES_AGGREGATED (in = in_scores);
-- MAGIC     by ECID;
-- MAGIC     if in_rtdm;
-- MAGIC     run;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW model_scores_aggregated AS
SELECT id, MAX(score) as max_score
FROM model_score
GROUP BY id;
SELECT * FROM model_scores_aggregated;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rtdm_data_temp AS
SELECT a.*, b.max_score as score FROM rtdm_data a LEFT JOIN model_scores_aggregated b ON a.id = b.id;
SELECT * FROM rtdm_data_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ####SAS Code 
-- MAGIC
-- MAGIC     data WORK.RTDM_FINAL;
-- MAGIC     set WORK.RTDM_MODEL;
-- MAGIC     where DISPOSITION <> 'Generated';
-- MAGIC     ECID_NUM = input(ECID, 15.);
-- MAGIC     rename ECID=ECID_CHAR
-- MAGIC            ECID_NUM = ECID
-- MAGIC            SCORE = PTB_SCORE;
-- MAGIC     drop OFFER_OR_ACTION ECID_CHAR;
-- MAGIC     run;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rtdm_data_final AS 
SELECT disposition, score, CAST(id AS INT) AS id, score AS ptb_score
FROM rtdm_data_temp
WHERE disposition != 'generated'
AND score IS NOT NULL;
SELECT * FROM rtdm_data_final;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SAS Code 
-- MAGIC
-- MAGIC     proc sql;
-- MAGIC     CREATE TABLE WORK.RTDM_STATS AS
-- MAGIC     SELECT
-- MAGIC     rtdm.ECID,
-- MAGIC     rtdm.PTB_SCORE,
-- MAGIC     CASE
-- MAGIC         WHEN rtdm.PTB_SCORE > 0.5 AND bdw.ECID is not null THEN input('TRUE_HIT', $15.)
-- MAGIC         WHEN rtdm.PTB_SCORE > 0.5 AND bdw.ECID is null THEN input('FALSE_MISS', $15.)
-- MAGIC         WHEN rtdm.PTB_SCORE <= 0.5 AND bdw.ECID is not null THEN input('FALSE_HIT', $15.)
-- MAGIC         WHEN rtdm.PTB_SCORE <= 0.5 AND bdw.ECID is null THEN input('TRUE_MISS', $15.)
-- MAGIC     END as RESOLUTION
-- MAGIC     from WORK.RTDM_FINAL rtdm
-- MAGIC     left join WORK.BDW_DATA bdw ON bdw.ECID = rtdm.ECID;
-- MAGIC     run;
-- MAGIC
-- MAGIC     proc sql;
-- MAGIC     CREATE TABLE WORK.UNIDENTIFIED_HITS AS
-- MAGIC     SELECT
-- MAGIC     bdw.ECID,
-- MAGIC     -1.0 as PTB_SCORE,
-- MAGIC     input('UNDEFINED_HIT', $15.) as RESOLUTION
-- MAGIC     from WORK.BDW_DATA bdw
-- MAGIC     left join WORK.RTDM_FINAL rtdm on rtdm.ECID = bdw.ECID
-- MAGIC     where
-- MAGIC     rtdm.ECID is NULL;
-- MAGIC     run;
-- MAGIC
-- MAGIC     proc append
-- MAGIC     base=WORK.RTDM_STATS
-- MAGIC     data=WORK.UNIDENTIFIED_HITS;
-- MAGIC     run;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rtdm_stats AS
SELECT r.id, r.ptb_score, 
CASE
    WHEN r.PTB_SCORE > 0.5 AND b.customer_id is not null THEN ('TRUE_HIT')
    WHEN r.PTB_SCORE > 0.5 AND b.customer_id is null THEN ('FALSE_MISS')
    WHEN r.PTB_SCORE <= 0.5 AND b.customer_id is not null THEN ('FALSE_HIT')
    WHEN r.PTB_SCORE <= 0.5 AND b.customer_id is null THEN ('TRUE_MISS') END as RESOLUTION
FROM rtdm_data_final r LEFT JOIN bdw_data b
ON r.id = b.customer_id;
SELECT * FROM rtdm_stats;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW unidentified_hits AS
SELECT
b.customer_id,
-1.0 as PTB_SCORE,
'UNDEFINED_HIT' as RESOLUTION
from bdw_data b
left join rtdm_data_final r on r.id = b.customer_id
where
r.id is NULL;
SELECT * FROM unidentified_hits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load Data in Persistent Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ####SAS Code 
-- MAGIC
-- MAGIC     proc sql;
-- MAGIC     CREATE TABLE TARGET.RTDM_FULFILLMENT_REPORT_LAST AS
-- MAGIC     SELECT *
-- MAGIC     FROM WORK.RTDM_STATS;
-- MAGIC     run;
-- MAGIC
-- MAGIC     proc append
-- MAGIC     base=TARGET.RTDM_FULFILLMENT_REPORT_HIST
-- MAGIC     data=WORK.RTDM_STATS;
-- MAGIC     run;

-- COMMAND ----------

-- INSERT OVERWRITE rtdm_report_lastest SELECT * FROM rtdm_stats

-- INSERT INTO rtdm_report_hist SELECT * FROM rtdm_stats