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

CREATE OR ALTER VIEW vfcbc_staging.vfcbc_ats_all_auths_vw AS
SELECT
    a.JOBID,
    a.TRACKINGNUMBER,
    a.ATSLINKID,
    b.ATSAUTHORIZATIONNUMBER,
    a.ATSPROJECTNUMBER,
    d.PROJECT_ID,
    b.LANDSFILENUMBER,
    d.FILE_NUMBER,
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
    d.AUTHORIZATION_STATUS_CODE,
    f.DESCRIPTION AS AUTHORIZATION_STATUS_DESC,
    d.ATHN_CLOSE_REASON_CODE,
    e.DESCRIPTION AS ATHN_CLOSE_REASON_DESC,
    a.CREATEDDATE,
    a.SUBMITTEDDATE,
    d.APPLICATION_RECEIVED_DATE,
    a.RECEIVEDDATE,
    d.APPLICATION_ACCEPTED_DATE,
    d.APPLICATION_REJECTED_DATE,
    a.DATECOMPLETED,
    d.FIRST_NATION_START_DATE,
    d.FIRST_NATION_COMPLETION_DATE,
    d.TECH_REVIEW_COMPLETION_DATE,
    d.ADJUDICATION_DATE
FROM vfcbc_replication.vfcbc_application_vw a
LEFT JOIN vfcbc_replication.vfcbc_atsnumber_vw b
    ON a.ATSLINKID = b.ATSLINKID
LEFT JOIN vfcbc_replication.vfcbc_crownlandtenureapparea c
    ON b.ATSAUTHORIZATIONNUMBER = c.ATSAUTHORIZATIONNUMBER
LEFT JOIN ats_replication.ats_authorizations d
    ON b.ATSAUTHORIZATIONNUMBER = d.AUTHORIZATION_ID
LEFT JOIN ats_replication.ats_athn_close_reason_codes e
    ON d.ATHN_CLOSE_REASON_CODE = e.ATHN_CLOSE_REASON_CODE
LEFT JOIN ats_replication.ats_authorization_status_codes f 
    ON d.AUTHORIZATION_STATUS_CODE = f.AUTHORIZATION_STATUS_CODE;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
