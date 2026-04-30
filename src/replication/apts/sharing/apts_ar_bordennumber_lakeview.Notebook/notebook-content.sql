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
