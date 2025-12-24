# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03759f8c-a2bd-42db-b3bd-c98b5d3ad239",
# META       "default_lakehouse_name": "testETL_lakehouse",
# META       "default_lakehouse_workspace_id": "79dafa8a-b966-4240-80f7-2e9d46baa5c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ===========================================================
# push_all_pies_outputs_to_api_fabric.py
# Fabric-compatible PySpark version (no dbutils/mssparkutils)
# ===========================================================

from pyspark.sql import SparkSession
import gzip, json, requests, os
from pathlib import Path

spark = SparkSession.builder.getOrCreate()

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------
API_URL = "https://nr-peach-test-main-api.azurewebsites.net/api/v1/"

ABFS_DIR = (
    "abfss://CITZ_PMT_NonProd@onelake.dfs.fabric.microsoft.com/"
    "testETL_lakehouse.Lakehouse/Files/pies_output"
)
LOCAL_DIR = "/tmp/pies_output"
os.makedirs(LOCAL_DIR, exist_ok=True)


# ----------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------
def list_existing_files() -> list[str]:
    """List all .jsonl.gz files in the ABFS pies_output folder."""
    print(f"📂 Scanning for files in: {ABFS_DIR}")
    file_df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.jsonl.gz")
        .load(ABFS_DIR)
    )
    files = [row.path for row in file_df.collect()]
    print(f"Found {len(files)} file(s):")
    for f in files:
        print("  •", f)
    return files


def copy_from_abfs(file_uri: str) -> Path:
    """Copy a file from ABFS to /tmp using Spark."""
    filename = os.path.basename(file_uri)
    local_path = Path(f"{LOCAL_DIR}/{filename}")
    print(f"📥 Copying {filename} → {local_path}")
    # Use Spark to read bytes and write to local disk
    data = spark.read.format("binaryFile").load(file_uri).collect()
    if not data:
        raise FileNotFoundError(f"No data found at {file_uri}")
    content = data[0].content
    with open(local_path, "wb") as f:
        f.write(content)
    return local_path


def stream_jsonl_to_api(file_path: Path, api_url: str):
    """
    Stream JSONL.GZ file to API **one record per request**.
    Each line is parsed and sent as a single JSON object.
    """
    print(f"📤 Uploading {file_path.name} → {api_url}")
    total = 0
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"⚠️ Skipping invalid JSON at line {line_no}: {e}")
                continue

            resp = requests.post(api_url, json=record)
            total += 1
            print(f"→ Sent record #{total} (line {line_no}) | Status {resp.status_code}")

    print(f"✅ Uploaded {total} records from {file_path.name}\n")


# ----------------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------------
def main():
    print("🚀 Starting Fabric → API upload pipeline")
    files = list_existing_files()
    if not files:
        print("⚠️ No .jsonl.gz files found — nothing to upload.")
        return

    for uri in files:
        try:
            local_path = copy_from_abfs(uri)
            stream_jsonl_to_api(local_path, API_URL)
        except Exception as e:
            print(f"⚠️ Failed to process {uri}: {e}")

    print("🎉 All available files processed successfully.")


# ----------------------------------------------------------------
# RUN
# ----------------------------------------------------------------
if __name__ == "__main__":
    main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
