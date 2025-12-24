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

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# push_to_peach_standalone.py
# Stand-alone uploader for NR-PEACH JSONL outputs
#
# Reads a JSONL file (each line = one ProcessEventSet record)
# and posts each JSON object individually to the PEACH API.
# ---------------------------------------------------------------------------

import json
import time
import requests
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
API_URL = "https://nr-peach-test-main-api.azurewebsites.net/api/v1/process-events/"  # endpoint
JSONL_PATH = Path("/lakehouse/default/Files/pies_output/events_trackingnumber.jsonl")
POST_ENABLED = True        # set False to just test reading without posting
REQUEST_TIMEOUT = 30       # seconds
RETRY_DELAY = 2            # seconds between retries on failure
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# CORE LOGIC
# ---------------------------------------------------------------------------
def post_record(record: dict) -> int:
    """Send one record to the API, with simple retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(API_URL, json=record, timeout=REQUEST_TIMEOUT)
            return resp.status_code
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            time.sleep(RETRY_DELAY)
    return -1  # failed


def main():
    if not JSONL_PATH.exists():
        print(f"❌ File not found: {JSONL_PATH}")
        return

    print(f"🚀 Starting upload from {JSONL_PATH}")
    total, success = 0, 0

    with JSONL_PATH.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"⚠️ Invalid JSON at line {line_no}: {e}")
                continue

            total += 1
            if POST_ENABLED:
                status = post_record(record)
                ok = 200 <= status < 300
                print(f"→ Line {line_no:6d} | Status {status}{' ✅' if ok else ' ❌'}")
                if ok:
                    success += 1
            else:
                print(f"→ Line {line_no:6d} (skipped, POST_DISABLED)")

    print(
        f"\n✅ Completed. {success}/{total} records posted "
        f"({total - success} failed)."
    )


# ---------------------------------------------------------------------------
# RUN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
