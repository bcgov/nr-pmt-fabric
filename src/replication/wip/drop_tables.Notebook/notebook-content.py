# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "201b9b7d-ca11-46b6-81e8-feeba4c99642",
# META       "default_lakehouse_name": "pmt_lakehouse",
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

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("DROP TABLE IF EXISTS rrs_replication.resource_road_client_sdw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS rrs_replication.resource_road_tenure")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS rrs_replication.road_appl_map_feature")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_application_status_code")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_client_status_code")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_external_submsn_sdw")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_feature_class_sdw")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_forest_mgt_unit_sdw")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_org_unit_sdw")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_request")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_section")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_section_status_code")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_submission")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS rrs_replication.road_tenure_client")
spark.sql("DROP TABLE IF EXISTS rrs_replication.road_use_permit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA fta_DEMO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
