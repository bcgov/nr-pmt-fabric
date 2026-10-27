# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
%%pyspark
 
# Fix legacy datetime issues when reading old Parquet files
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW rarn_sharing.rar_dev_assessments AS
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
