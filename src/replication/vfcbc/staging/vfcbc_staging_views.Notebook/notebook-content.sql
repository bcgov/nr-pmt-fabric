-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "201b9b7d-ca11-46b6-81e8-feeba4c99642",
-- META       "default_lakehouse_name": "permitting_lakehouse",
-- META       "default_lakehouse_workspace_id": "79dafa8a-b966-4240-80f7-2e9d46baa5c3",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "201b9b7d-ca11-46b6-81e8-feeba4c99642"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE OR ALTER VIEW vfcbc_staging.vfcbc_mines_auths_vw AS
SELECT
    a.JOBID,
    a.TRACKINGNUMBER,
    a.ATSLINKID,
    b.ATSAUTHORIZATIONNUMBER,
    a.ATSPROJECTNUMBER,
    a.EXTERNALREFERENCE,
    a.BUSINESSAREA,
    a.BUSINESSAREANUMBER,
    a.OFFICE,
    a.APPLICANT,
    a.SUBMITTER,
    a.AUTHORIZATIONTYPE,
    a.APPLICATIONTYPE,
    a.STATUS,
    a.STATUSSHOWNTOCLIENT,
    a.CREATEDDATE,
    a.SUBMITTEDDATE,
    a.RECEIVEDDATE,
    a.DATECOMPLETED
FROM vfcbc_replication.vfcbc_application_vw AS a
LEFT JOIN vfcbc_replication.vfcbc_atsnumber_vw AS b
    ON a.ATSLINKID = b.ATSLINKID
WHERE a.BUSINESSAREA = 'Mines Competitiveness and Authorizations Division';


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW vfcbc_staging.vfcbc_lands_auths_vw AS
SELECT
        a.JOBID,
        a.TRACKINGNUMBER,
        a.ATSLINKID,
        b.ATSAUTHORIZATIONNUMBER,
        a.ATSPROJECTNUMBER,
        b.LANDSFILENUMBER,
        b.DISPOSITION_TRANSACTION_STRING,
        a.BUSINESSAREA,
        a.BUSINESSAREANUMBER,
        a.OFFICE,
        a.APPLICANT,
        a.SUBMITTER,
        a.AUTHORIZATIONTYPE,
        a.APPLICATIONTYPE,
        a.STATUS,
        a.STATUSSHOWNTOCLIENT,
        a.CREATEDDATE,
        a.SUBMITTEDDATE,
        a.RECEIVEDDATE,
        a.DATECOMPLETED
    FROM vfcbc_replication.vfcbc_application_vw a
    LEFT JOIN vfcbc_replication.vfcbc_atsnumber_vw b
        ON a.ATSLINKID = b.ATSLINKID
    LEFT JOIN vfcbc_replication.vfcbc_crownlandtenureapparea c
        ON b.ATSAUTHORIZATIONNUMBER = c.ATSAUTHORIZATIONNUMBER
    WHERE a.BUSINESSAREA = 'Crown Land Adjudication (vFCBC)';

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW vfcbc_staging.vfcbc_all_auths_vw AS

WITH base AS (
    SELECT
        a.JOBID,
        a.TRACKINGNUMBER,
        a.ATSLINKID,
        b.ATSAUTHORIZATIONNUMBER,
        a.ATSPROJECTNUMBER,
        b.LANDSFILENUMBER,
        b.DISPOSITION_TRANSACTION_STRING,
        a.EXTERNALREFERENCE,
        a.EXTERNALJOBNUMBER,
        a.EXTERNALJOBNUMBER2,
        a.MINENUMBER,
        a.BUSINESSAREA,
        a.BUSINESSAREANUMBER,
        a.OFFICE,
        a.APPLICANT,
        a.SUBMITTER,
        a.AUTHORIZATIONTYPE,
        a.APPLICATIONTYPE,
        a.STATUS,
        a.STATUSSHOWNTOCLIENT,
        a.CREATEDDATE,
        a.SUBMITTEDDATE,
        a.RECEIVEDDATE,
        a.DATECOMPLETED
    FROM vfcbc_replication.vfcbc_application_vw a
    LEFT JOIN vfcbc_replication.vfcbc_atsnumber_vw b
        ON a.ATSLINKID = b.ATSLINKID
    LEFT JOIN vfcbc_replication.vfcbc_crownlandtenureapparea c
        ON b.ATSAUTHORIZATIONNUMBER = c.ATSAUTHORIZATIONNUMBER
),

exploded AS (
    SELECT
        base.*,
        LTRIM(RTRIM(s.value)) AS disposition_value
    FROM base
    CROSS APPLY STRING_SPLIT(base.DISPOSITION_TRANSACTION_STRING, ',') s
)

SELECT
    JOBID,
    TRACKINGNUMBER,
    ATSLINKID,
    ATSAUTHORIZATIONNUMBER,
    ATSPROJECTNUMBER,
    LANDSFILENUMBER,
    CAST(disposition_value AS INT) AS DISPOSITION_TRANSACTION_SID,
    EXTERNALREFERENCE,
    EXTERNALJOBNUMBER,
    EXTERNALJOBNUMBER2,
    MINENUMBER,
    BUSINESSAREA,
    BUSINESSAREANUMBER,
    OFFICE,
    APPLICANT,
    SUBMITTER,
    AUTHORIZATIONTYPE,
    APPLICATIONTYPE,
    STATUS,
    STATUSSHOWNTOCLIENT,
    CREATEDDATE,
    SUBMITTEDDATE,
    RECEIVEDDATE,
    DATECOMPLETED
FROM exploded
WHERE disposition_value IS NOT NULL
  AND disposition_value <> ''
  AND TRY_CAST(disposition_value AS INT) IS NOT NULL;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
