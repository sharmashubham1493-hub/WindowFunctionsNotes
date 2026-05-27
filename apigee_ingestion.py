# Databricks notebook source

# COMMAND ----------

import requests
import json
import datetime
from datetime import timedelta
import time
import os
import urllib3
import pytz
import ssl
from io import StringIO
import pandas
import collections
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql import functions as f

# COMMAND ----------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
spark = SparkSession.builder.appName("APiGef_Ingestion_Daily").getOrCreate()

# COMMAND ----------

# Bootstrap the batch id for this job
batch_id = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")
print(batch_id)

# COMMAND ----------

journey_name = [
    "apigee-update-succ-logs",
    "apigee-update-tdcc-logs",
    "apigee-update-ptcc-logs",
    "apigee-update-ptcc-logs",
]

# COMMAND ----------

SCOPE_JOURNEY_MAP = {
    "TDCC": "apigee-update-tdcc-logs",
    "SMCC": "apigee-update-succ-logs",
    "PRCC": "apigee-update-ptcc-logs",
    "PFCC": "apigee-update-ptcc-logs",
}

# COMMAND ----------

user = "apigee"
pwd = "apigee@23"

kolkata_tz = pytz.timezone("Asia/Kolkata")
url = "https://10.222.72.188:9200/apigee-updated-*/_search"

# COMMAND ----------

# yesterday (r#)

headers = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
}

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
print(f"all_scopes: {all_scopes}")
# scope = ['TDCC', 'SMCC', 'PRCC', 'PFCC']

file_date = (
    datetime.datetime.now(kolkata_tz).date() - timedelta(days=1)
).strftime("%Y-%m-%d")
print(file_date)

# --- MODIFIED: ingest in 48 x 30-minute intervals (two API requests per hour) ---
all_hits = []

for interval in range(48):
    start_hour   = interval // 2          # 0,0,1,1,2,2,...,23,23
    start_minute = (interval % 2) * 30    # 0,30,0,30,...
    end_minute   = start_minute + 29      # 29,59,29,59,...

    start_time = f"{file_date}T{start_hour:02d}:{start_minute:02d}:00.000"
    end_time   = f"{file_date}T{start_hour:02d}:{end_minute:02d}:59.999"

    print(f"Fetching interval {interval + 1}/48: {start_time} to {end_time}")

    response = requests.post(
        url,
        headers=headers,
        auth=(user, pwd),
        json={
            "size": 10000,
            "fields": ["*"],
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_time,
                                    "lte": end_time,
                                }
                            }
                        },
                        {"terms": {"scope.keyword": all_scopes}},
                    ]
                }
            },
        },
        verify=False,
        timeout=120,
    )

    if response.status_code != 200:
        raise Exception(
            f"API call failed for interval {start_time} - {end_time}: "
            f"{response.text[:100]}"
        )

    # ------------------------------------------------------------------
    # IMPORTANT: parse THIS interval's response — do NOT use a variable
    # called response_json from a previous cell; that would be stale.
    # Everything below must stay inside the for-loop (indented 4 spaces).
    # ------------------------------------------------------------------
    resp_json   = response.json()
    total_in_es = resp_json.get("hits", {}).get("total", {}).get("value", "?")
    hits        = resp_json.get("hits", {}).get("hits", [])

    all_hits.extend(hits)                          # ← INSIDE the loop

    print(
        f"  HTTP {response.status_code} | "
        f"ES total for this window: {total_in_es} | "
        f"returned: {len(hits)} | "
        f"running total: {len(all_hits)}"
    )

# outside the loop
print(f"\nAll 48 intervals fetched successfully.")
print(f"Total records fetched: {len(all_hits)}")
