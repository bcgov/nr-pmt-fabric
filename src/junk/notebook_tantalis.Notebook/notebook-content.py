# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "201b9b7d-ca11-46b6-81e8-feeba4c99642",
# META       "default_lakehouse_name": "permitting_lakehouse",
# META       "default_lakehouse_workspace_id": "79dafa8a-b966-4240-80f7-2e9d46baa5c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "201b9b7d-ca11-46b6-81e8-feeba4c99642"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.functions import col
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Run the query
result_df = spark.sql("""
SELECT DISTINCT
       DT.APPLICATION_TYPE_CDE,
       DT.COMMENCEMENT_DAT,
       DT.COMPLEXITY_LEVEL_CDE,
       DT.CROWN_GRANT_YRN,
       DT.DISPOSITION_SID,
       DT.DISPOSITION_TRANSACTION_SID,
       DT.EXPIRY_DAT,
       DT.MANAGING_AGENCY,
       DT.ORG_UNIT_SID,
       DT.RECEIVED_DAT,
       DT.REPLMNT_FOR_DISP_TRANS_SID,
       DT.REVIEW_DAT,
       DT.SUBTYPE_SID,
       DT.TYPE_SID,
       O.UNIT_NAME,
       TD.ACTIVATION_CDE,
       TD.FILE_CHR,
       DTS.CODE_CHR_STATUS,
       DTS.CODE_CHR_STAGE,
       DTS.EFFECTIVE_DAT,
       DTS.CODE_CHR_REASON,
       TAV.TYPE_NME,
       TAS.SUBTYPE_NME,
       TAP.PURPOSE_NME,
       TASU.SUBPURPOSE_NME,
       TDO.NAME AS `DISTRICT_OFFICE`,
       TASKS.LAND_STATUS_DAT,
       TASKS.REPORTED_DAT,
       TASKS.ADJUDICATED_DAT,
       TASKS.DISALLOWED_DAT,
       TASKS.OFFERED_DAT,
       TASKS.OFFER_NOT_ACCEPTED_DAT,
       TASKS.CANCELED_DAT
FROM tantalis_replication.ta_disposition_transactions DT
JOIN tantalis_replication.ta_organization_units O
     ON O.ORG_UNIT_SID = DT.ORG_UNIT_SID
JOIN tantalis_replication.ta_dispositions TD
     ON TD.DISPOSITION_SID = DT.DISPOSITION_SID
JOIN tantalis_replication.ta_disp_trans_statuses DTS
     ON DTS.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
    AND DTS.EXPIRY_DAT IS NULL
LEFT JOIN (
    SELECT
        DISPOSITION_TRANSACTION_SID,
        MAX(LAND_STATUS_DAT) AS LAND_STATUS_DAT,
        MAX(REPORTED_DAT) AS REPORTED_DAT,
        MAX(ADJUDICATED_DAT) AS ADJUDICATED_DAT,
        MAX(DISALLOWED_DAT) AS DISALLOWED_DAT,
        MAX(OFFERED_DAT) AS OFFERED_DAT,
        MAX(OFFER_NOT_ACCEPTED_DAT) AS OFFER_NOT_ACCEPTED_DAT,
        MAX(CANCELED_DAT) AS CANCELED_DAT
    FROM tantalis_replication.ta_disp_trans_tasks
    GROUP BY DISPOSITION_TRANSACTION_SID
) TASKS ON TASKS.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
LEFT JOIN tantalis_replication.ta_available_types TAV
     ON TAV.TYPE_SID = DT.TYPE_SID
LEFT JOIN tantalis_replication.ta_available_subtypes TAS
     ON TAS.TYPE_SID = DT.TYPE_SID
    AND TAS.SUBTYPE_SID = DT.SUBTYPE_SID
LEFT JOIN tantalis_replication.ta_available_purposes TAP
     ON TAP.PURPOSE_SID = DT.PURPOSE_SID
LEFT JOIN tantalis_replication.ta_available_subpurposes TASU
     ON TASU.PURPOSE_SID = DT.PURPOSE_SID
    AND TASU.SUBPURPOSE_SID = DT.SUBPURPOSE_SID
LEFT JOIN tantalis_replication.ta_district_offices TDO
     ON DT.DISTRICT_OFFICE_SID = TDO.DISTRICT_OFFICE_SID
WHERE TO_DATE(DT.RECEIVED_DAT) >= DATE('2014-04-01')
  AND DT.APPLICATION_TYPE_CDE = 'NEW'
  AND DT.TYPE_SID <> '51'
  AND NOT (DT.TYPE_SID = '1' AND DT.SUBTYPE_SID = '101')
""")
int_cols = [
    "DISPOSITION_SID",
    "REPLMNT_FOR_DISP_TRANS_SID",
    "DISPOSITION_TRANSACTION_SID",
    "ORG_UNIT_SID",
    "TYPE_SID",
    "SUBTYPE_SID",
    "PURPOSE_SID",
    "SUBPURPOSE_SID",
    "DISTRICT_OFFICE_SID"
]

# Cast all of them to long (integer)
for c in int_cols:
    if c in result_df.columns:
          result_df = result_df.withColumn(c, col(c).cast("long"))
# Show results
result_df.show()
result_df.write.mode("overwrite").saveAsTable("tantalis_staging.ta_disposition_merged")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
