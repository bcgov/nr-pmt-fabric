# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

IF SCHEMA_ID('ats_staging') IS NULL
BEGIN
    EXEC('CREATE SCHEMA ats_staging');
END;
GO

DROP VIEW IF EXISTS ats_staging.ats_authorizations_on_hold_staging;
GO

CREATE VIEW ats_staging.ats_authorizations_on_hold_staging
AS
WITH deduped AS (
    SELECT
        AUTHORIZATION_ON_HOLD_ID,
        ATHN_ON_HOLD_REASON_CODE,
        AUTHORIZATION_ID,
        CURRENT_ON_HOLD_IND,
        WHO_CREATED,
        WHEN_CREATED,
        ON_HOLD_START_DATE,
        ON_HOLD_END_DATE,
        ON_HOLD_NOTES,
        REMOVED_BY,
        WHO_UPDATED,
        WHEN_UPDATED,
        PARTNER_AGENCY_ID,
        ROW_NUMBER() OVER (
            PARTITION BY
                ATHN_ON_HOLD_REASON_CODE,
                AUTHORIZATION_ID,
                ON_HOLD_START_DATE,
                ON_HOLD_END_DATE
            ORDER BY
                AUTHORIZATION_ON_HOLD_ID
        ) AS rn
    FROM ats_replication.ats_authorizations_on_hold
)
SELECT
    AUTHORIZATION_ON_HOLD_ID,
    ATHN_ON_HOLD_REASON_CODE,
    AUTHORIZATION_ID,
    CURRENT_ON_HOLD_IND,
    WHO_CREATED,
    WHEN_CREATED,
    ON_HOLD_START_DATE,
    ON_HOLD_END_DATE,
    ON_HOLD_NOTES,
    REMOVED_BY,
    WHO_UPDATED,
    WHEN_UPDATED,
    PARTNER_AGENCY_ID
FROM deduped
WHERE rn = 1;
GO

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
