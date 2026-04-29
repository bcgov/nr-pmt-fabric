-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "a4c99642-feeb-81e8-46b6-ca11201b9b7d",
-- META       "default_lakehouse_name": "permitting_lakehouse",
-- META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "a4c99642-feeb-81e8-46b6-ca11201b9b7d",
-- META           "workspace_id": "00000000-0000-0000-0000-000000000000"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC # Fix legacy datetime issues when reading old Parquet files
-- MAGIC spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
-- MAGIC spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
-- MAGIC spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
-- MAGIC spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_amsprocesses_subset AS
-- MAGIC SELECT
-- MAGIC     PROCESSID,
-- MAGIC     CREATEDDATE,
-- MAGIC     JOBID,
-- MAGIC     DATECOMPLETED,
-- MAGIC     OBJECTDEFDESCRIPTION,
-- MAGIC     OUTCOME,
-- MAGIC     PROCESSTYPEID,
-- MAGIC     COMPLETEDBYUSERID,
-- MAGIC     OBJECTID,
-- MAGIC     PROCESSNAME
-- MAGIC FROM apts_replication.apts_amsprocesses
-- MAGIC WHERE PROCESSNAME in ('p_AR_AuthorizePermits'
-- MAGIC ,'p_AR_RecordResponse'
-- MAGIC ,'p_AR_ChangeStatus'
-- MAGIC ,'p_AR_EnterApplication'
-- MAGIC ,'p_AR_HoldOnOff'
-- MAGIC ,'p_AR_ReviewAmendmentRequest'
-- MAGIC ,'p_AR_NoticeOfPermitReportFail'
-- MAGIC ,'p_AR_SendPermitReport'
-- MAGIC ,'p_AR_EvaluateComments'
-- MAGIC ,'p_AR_RecommendPermitIssueRej'
-- MAGIC ,'p_AR_AssignPermitApplication'
-- MAGIC ,'p_AR_ReviewRecommendations'
-- MAGIC ,'p_AR_ReviewApplication'
-- MAGIC ,'p_AR_IssuePermit'
-- MAGIC ,'p_AR_ValidatePermitReport'
-- MAGIC ,'p_AR_InviteComments'
-- MAGIC ,'p_AR_SendCommentReminders'
-- MAGIC ,'p_AR_RequestInformation');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
