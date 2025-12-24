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


from pyspark.sql import functions as F

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


# Load all tables from the lakehouse (under tantalis_replication schema)
DT = spark.table("tantalis_replication.TA_DISPOSITION_TRANSACTIONS")
O = spark.table("tantalis_replication.TA_ORGANIZATION_UNITS")
DT.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ShowDataFrame") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("David", 4), ("Eve", 5)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, columns)

# Display the DataFrame
df.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
