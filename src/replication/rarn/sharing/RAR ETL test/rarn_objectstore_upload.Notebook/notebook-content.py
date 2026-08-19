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
# META     },
# META     "environment": {
# META       "environmentId": "fefdba65-6f7a-9179-42a3-e5d7e20a9617",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import os
import boto3

from datetime import datetime
from notebookutils import mssparkutils

print(boto3.__version__) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

KEYVAULT_URL = keyvault_url

#KEYVAULT_URL = "https://kv-citz-pmt-dev.vault.azure.net/"

print(f"Using Key Vault: {KEYVAULT_URL}")

today = datetime.now().strftime("%Y%m%d")

filename = f"rar_extract_{today}.xlsx"

output_dir = "/lakehouse/default/Files/RAR_exports"
output_file = f"{output_dir}/{filename}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = spark.sql("""
SELECT
    rda.LOCATION_LEGAL_DSC,
    rstc.DESCRIPTION AS STREAM_TYPE,
    rda.WHEN_CREATED,
    rda.LOCATION_LATITUDE,
    rda.PROPOSED_END_DATE,
    rda.LOT_AREA,
    rda.DEV_CODE,
    rda.ASSESSMENT_METHOD_FOLLOWED_YN,
    rda.LOCATION_LONGITUDE,
    rda.WHO_CREATED,
    rda.SECTION_9_PART_7_ACTIVITIES_YN,
    ra.COUNTRY,
    ra.POSTAL_CODE,
    rda.ALL_PROFESSIONALS_QUALIFIED_YN,
    rda.CERTIFIED_NO_HADD_YN,
    rdsc.DESCRIPTION AS FILE_STATUS,
    rda.WHO_UPDATED,
    rda.PROPOSED_START_DATE,
    rda.LOCATION_STREAM_NAME,
    rda.DEV_NATURE_CODE,
    ra.ADDRESS_LINE_1,
    rda.REGION_CODE,
    rda.DFO_AREA_CODE,
    ra.ADDRESS_LINE_3,
    ra.ADDRESS_LINE_2,
    ra.PROVINCE_STATE,
    rda.RETAIN_ASSESS_REPORT_YN,
    rdac.DESCRIPTION AS DFO_AREA_DESCRIPTION,
    rda.DEV_ASSESSMENT_ID AS FILE_ID,
    rda.MUNICIPALITY_CODE,
    rda.WHEN_UPDATED,
    rrc.DESCRIPTION AS REGION,
    rm.DESCRIPTION AS MUNICIPALITY,
    rda.DEV_AREA,
    rda.LOCATION_NEW_WATERSHED_CODE,
    ra.CITY,
    rdc.DESCRIPTION AS DEVELOPMENT,
    rda.RIPARIAN_AREA_LENGTH,
    rda.ADDRESS_ID,
    rda.COMPLETE_REPORT_ATTACHED_YN
FROM rarn_replication.rar_dev_assessments rda
JOIN rarn_replication.rar_addresses ra
    ON rda.ADDRESS_ID = ra.ADDRESS_ID
JOIN rarn_replication.rar_region_cds rrc
    ON rda.REGION_CODE = rrc.REGION_CODE
JOIN rarn_replication.rar_dev_cds rdc
    ON rda.DEV_CODE = rdc.DEV_CODE
JOIN rarn_replication.rar_municipalities rm
    ON rda.MUNICIPALITY_CODE = rm.MUNICIPALITY_CODE
JOIN rarn_replication.rar_dev_status_cds rdsc
    ON rda.DEV_STATUS_CODE = rdsc.DEV_STATUS_CODE
JOIN rarn_replication.rar_stream_type_cds rstc
    ON rda.STREAM_CODE = rstc.STREAM_TYPE_CODE
JOIN rarn_replication.rar_dfo_area_cds rdac
    ON rda.DFO_AREA_CODE = rdac.DFO_AREA_CODE""")

    print(f"Retrieved {df.count()} rows")

except Exception as ex:
    raise RuntimeError(f"Failed to extract RAR data: {ex}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    pdf = df.toPandas()

    pdf.to_excel(
        output_file,
        index=False
    )

    print(f"Excel file created: {output_file}")

except Exception as ex:
    raise RuntimeError(f"Failed to create Excel file: {ex}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    bucket = mssparkutils.credentials.getSecret(
        KEYVAULT_URL,
        "bucket"
    )

    endpoint = mssparkutils.credentials.getSecret(
        KEYVAULT_URL,
        "endpoint"
    )

    objid = mssparkutils.credentials.getSecret(
        KEYVAULT_URL,
        "objid"
    )

    objkey = mssparkutils.credentials.getSecret(
        KEYVAULT_URL,
        "objkey"
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=objid,
        aws_secret_access_key=objkey
    )

    print("Object Store connection established")

except Exception as ex:
    raise RuntimeError(f"Failed to connect to Object Store: {ex}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response = s3_client.list_objects_v2(
    Bucket=bucket,
    Prefix=f"data_extracts/riparian_areas/{filename}"
)

key_count = response.get("KeyCount", 0)

mssparkutils.notebook.exit(
    f"""
SUCCESS
FILE={filename}
KEYCOUNT={key_count}
"""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
