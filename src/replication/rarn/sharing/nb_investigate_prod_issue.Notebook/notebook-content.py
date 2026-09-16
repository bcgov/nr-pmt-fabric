# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4c99642-feeb-81e8-46b6-ca11201b9b7d",
# META       "default_lakehouse_name": "permitting_lakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4c99642-feeb-81e8-46b6-ca11201b9b7d",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC  
# MAGIC # Fix legacy datetime issues when reading old Parquet files
# MAGIC spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
# MAGIC spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
# MAGIC spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
# MAGIC spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
# MAGIC spark.conf.set("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY")
# MAGIC spark.conf.set("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC DEV_ASSESSMENT_ID,
# MAGIC DEV_NATURE_CODE,
# MAGIC ADDRESS_ID,
# MAGIC REGION_CODE,
# MAGIC STREAM_CODE,
# MAGIC MUNICIPALITY_CODE,
# MAGIC DEV_CODE,
# MAGIC RIPARIAN_AREA_LENGTH,
# MAGIC CASE     WHEN CAST(PROPOSED_START_DATE AS DATE) < DATE '1582-10-15'         THEN NULL    WHEN CAST(PROPOSED_START_DATE AS DATE) = DATE '1900-01-01'         THEN NULL    ELSE CAST(PROPOSED_START_DATE AS DATE) END AS PROPOSED_START_DATE,
# MAGIC CASE     WHEN CAST(PROPOSED_END_DATE AS DATE) < DATE '1582-10-15'         THEN NULL    WHEN CAST(PROPOSED_END_DATE AS DATE) = DATE '1900-01-01'         THEN NULL    ELSE CAST(PROPOSED_END_DATE AS DATE) END AS PROPOSED_END_DATE,
# MAGIC LOCATION_LEGAL_DSC,
# MAGIC LOCATION_STREAM_NAME,
# MAGIC LOCATION_NEW_WATERSHED_CODE,
# MAGIC LOCATION_LATITUDE,
# MAGIC LOCATION_LONGITUDE,
# MAGIC LOT_AREA,
# MAGIC SECTION_9_PART_7_ACTIVITIES_YN,
# MAGIC ALL_PROFESSIONALS_QUALIFIED_YN,
# MAGIC ASSESSMENT_METHOD_FOLLOWED_YN,
# MAGIC CERTIFIED_NO_HADD_YN,
# MAGIC COMPLETE_REPORT_ATTACHED_YN,
# MAGIC REVISION_NUMBER,
# MAGIC WHO_CREATED,
# MAGIC CASE WHEN WHEN_CREATED = DATE '1900-01-01' THEN NULL WHEN WHEN_CREATED < DATE '1582-10-15' THEN NULL ELSE WHEN_CREATED END AS WHEN_CREATED,
# MAGIC WHO_UPDATED,
# MAGIC CASE WHEN WHEN_UPDATED = DATE '1900-01-01' THEN NULL WHEN WHEN_UPDATED < DATE '1582-10-15' THEN NULL ELSE WHEN_UPDATED END AS WHEN_UPDATED,
# MAGIC DEV_AREA,
# MAGIC DFO_AREA_CODE,
# MAGIC RETAIN_ASSESS_REPORT_YN,
# MAGIC DEV_STATUS_CODE
# MAGIC FROM rarn_replication.rar_dev_assessments;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     PROCESSID,
# MAGIC     CASE
# MAGIC         WHEN CREATEDDATE = DATE'1900-01-01' THEN NULL
# MAGIC         WHEN CREATEDDATE < DATE'1582-10-15' THEN NULL
# MAGIC         ELSE CREATEDDATE
# MAGIC     END AS CREATEDDATE,
# MAGIC     JOBID,
# MAGIC     DATECOMPLETED,
# MAGIC     OBJECTDEFDESCRIPTION,
# MAGIC     OUTCOME,
# MAGIC     PROCESSTYPEID,
# MAGIC     COMPLETEDBYUSERID,
# MAGIC     OBJECTID,
# MAGIC     PROCESSNAME
# MAGIC FROM apts_replication.apts_amsprocesses
# MAGIC WHERE PROCESSNAME in ('p_AR_AuthorizePermits'
# MAGIC ,'p_AR_RecordResponse'
# MAGIC ,'p_AR_ChangeStatus'
# MAGIC ,'p_AR_EnterApplication'
# MAGIC ,'p_AR_HoldOnOff'
# MAGIC ,'p_AR_ReviewAmendmentRequest'
# MAGIC ,'p_AR_NoticeOfPermitReportFail'
# MAGIC ,'p_AR_SendPermitReport'
# MAGIC ,'p_AR_EvaluateComments'
# MAGIC ,'p_AR_RecommendPermitIssueRej'
# MAGIC ,'p_AR_AssignPermitApplication'
# MAGIC ,'p_AR_ReviewRecommendations'
# MAGIC ,'p_AR_ReviewApplication'
# MAGIC ,'p_AR_IssuePermit'
# MAGIC ,'p_AR_ValidatePermitReport'
# MAGIC ,'p_AR_InviteComments'
# MAGIC ,'p_AR_SendCommentReminders'
# MAGIC ,'p_AR_RequestInformation');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC 
# MAGIC     OBJECTID,
# MAGIC     CASE
# MAGIC         WHEN COMPLETIONDATE = DATE'1900-01-01' THEN NULL
# MAGIC         WHEN COMPLETIONDATE < DATE'1582-10-15' THEN NULL
# MAGIC         ELSE COMPLETIONDATE
# MAGIC     END AS COMPLETIONDATE,
# MAGIC     PERMITJOBID,
# MAGIC     REQUESTEDDATE,
# MAGIC     REQUESTNUMBER,
# MAGIC     STATUS,
# MAGIC     AMENDMENTTYPEID
# MAGIC 
# MAGIC FROM apts_replication.apts_ar_permitammendmentrequest;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
