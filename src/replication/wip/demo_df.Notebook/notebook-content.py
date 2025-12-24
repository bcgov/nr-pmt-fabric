# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fb096953-0286-4e16-99f7-b58a586f2bfd",
# META       "default_lakehouse_name": "demo_DF",
# META       "default_lakehouse_workspace_id": "79dafa8a-b966-4240-80f7-2e9d46baa5c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "fb096953-0286-4e16-99f7-b58a586f2bfd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("CREATE SCHEMA IF NOT EXISTS fta_demo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Switch to the sandbox schema
spark.sql("USE fta_demo")

# Create a table inside sandbox
spark.sql("""
CREATE TABLE fta_demo.fta_demo_test_table (
    id INT,
    name STRING,
    value DOUBLE
)
""")

# Verify
spark.sql("SHOW TABLES IN fta_demo").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
