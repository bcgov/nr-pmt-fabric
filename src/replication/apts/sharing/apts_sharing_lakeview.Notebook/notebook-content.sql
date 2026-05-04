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
-- MAGIC spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
-- MAGIC spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
-- MAGIC spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE SCHEMA apts_sharing;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EIapts_sharing.apts_amsprocesses_subset AS
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

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_bordennumber AS
-- MAGIC SELECT
-- MAGIC     OBJECTID,
-- MAGIC     BORDENNUMBER
-- MAGIC FROM apts_replication.apts_ar_bordennumber;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_mapsheet AS
-- MAGIC SELECT
-- MAGIC     OBJECTID,
-- MAGIC     ACTIVE,
-- MAGIC     MAPSHEETPREFIX,
-- MAGIC     MAPSHEETSUFFIX
-- MAGIC FROM apts_replication.apts_ar_mapsheet;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_party_redacted AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     OBJECTID,
-- MAGIC     ACTIVE,
-- MAGIC     ADDRESSLINE1,
-- MAGIC     ADDRESSLINE2,
-- MAGIC     AGENCY,
-- MAGIC     ALTERNATENAME,
-- MAGIC     CITY,
-- MAGIC     COUNTRY,
-- MAGIC     EXTERNALAPPLICANT,
-- MAGIC     FIRSTNATIONAFFILIATION,
-- MAGIC     FIRSTNATIONSYNONYM,
-- MAGIC     ISARCHAEOLOGIST,
-- MAGIC     ISFIRSTNATION,
-- MAGIC     ISSYSTEMADMINISTRATOR,
-- MAGIC     ORGANIZATIONNAME,
-- MAGIC     PARTYROLE,
-- MAGIC     POSITIONTITLE,
-- MAGIC     PROVINCE,
-- MAGIC     RELATEDASSIGNMENT,
-- MAGIC     TYPE,
-- MAGIC     USERID,
-- MAGIC     WORKAUTHORIZATIONOBJECTID
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_party;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_archaeologypermitjob_redacted AS
-- MAGIC SELECT
-- MAGIC     JOBID,
-- MAGIC     ADDDEVELOPMENTAREA,
-- MAGIC     ADDFIELDDIRECTOR,
-- MAGIC     ADDPROPONENT,
-- MAGIC     AMENDMENTAPPROVEDDATE,
-- MAGIC     APPLICATIONNUMBER,
-- MAGIC     APPLICATIONRECEIVEDDATE,
-- MAGIC     APPLICATIONREVISED,
-- MAGIC     DATEASSIGNEDTOPO,
-- MAGIC     DELIVERABLESDUEDATE,
-- MAGIC     ISSUEDATE,
-- MAGIC     PERMITEXPIRYDATE,
-- MAGIC     PERMITNUMBER,
-- MAGIC     PERMITTYPEOBJECTID,
-- MAGIC     POREVIEWCOMPLETEDDATE,
-- MAGIC     POREVIEWDUEDATE,
-- MAGIC     PROJECTOFFICERID,
-- MAGIC     STATUSDESCRIPTION,
-- MAGIC     PROJECTTYPEID,
-- MAGIC     ISSUINGAGENCYID,
-- MAGIC     PROPONENTOBECTID,
-- MAGIC     SECTOROBJECTID
-- MAGIC FROM apts_replication.apts_ar_archaeologypermitjob;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permitparticipant_redacted AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     OBJECTID,
-- MAGIC     JOBID,
-- MAGIC     AFFILIATIONOBJECTID,
-- MAGIC     PARTYOBJECTID,
-- MAGIC     ROLEOBJECTID
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permitparticipant;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permitrelatedfile_redacted AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     OBJECTID,
-- MAGIC     REFERENCENUMBER,
-- MAGIC     JOBID,
-- MAGIC     FILETYPEID
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permitrelatedfile;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permittobordennumberxref AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     RELATIONSHIPID,
-- MAGIC     PERMITJOBID,
-- MAGIC     BORDENNUMBEROBJECTID
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permittobordennumberxref;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permittogeographareaxref AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     RELATIONSHIPID,
-- MAGIC     PERMITJOBID,
-- MAGIC     GEOGRAPHICALAREAOBJECTID
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permittogeographareaxref;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permittointerestedpartyxref AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     RELATIONSHIPID,
-- MAGIC     PERMITJOBID,
-- MAGIC     INTERESTEDPARTYOBJECTID,
-- MAGIC     COMMENTSREQUIRED
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permittointerestedpartyxref;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permittomapsheetxref AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     RELATIONSHIPID,
-- MAGIC     PERMITJOBID,
-- MAGIC     MAPSHEETOBJECTID
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permittomapsheetxref;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_permittype AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     OBJECTID,
-- MAGIC     ACTIVE,
-- MAGIC     EXTERNALCANCREATE,
-- MAGIC     OGCCANCREATE,
-- MAGIC     PERMITTYPE
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_permittype;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW apts_sharing.apts_ar_processes_redacted AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     PROCESSID,
-- MAGIC     CREATEDDATE,
-- MAGIC     DATECOMPLETED,
-- MAGIC     JOBID,
-- MAGIC     OUTCOME,
-- MAGIC     SCHEDULEDCOMPLETEDATE,
-- MAGIC     SCHEDULEDSTARTDATE,
-- MAGIC     OBJECTDEFDESCRIPTION
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_processes;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS apts_sharing.apts_ar_systemtable AS
-- MAGIC SELECT
-- MAGIC 
-- MAGIC     OBJECTID,
-- MAGIC     ACTIVE,
-- MAGIC     SYSTEMTABLETYPE,
-- MAGIC     VALUE
-- MAGIC 
-- MAGIC FROM apts_replication.apts_ar_systemtable;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
