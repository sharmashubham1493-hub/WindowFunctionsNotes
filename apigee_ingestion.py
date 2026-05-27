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

# --- MODIFIED: ingest in 12 x 2-hour intervals instead of one 24-hour request ---
all_hits = []

for interval in range(12):
    start_hour = interval * 2        # 0, 2, 4, ..., 22
    end_hour = start_hour + 1        # 1, 3, 5, ..., 23  (covers HH:00:00 – HH+1:59:59)

    start_time = f"{file_date}T{start_hour:02d}:00:00.000"
    end_time   = f"{file_date}T{end_hour:02d}:59:59.999"

    print(f"Fetching interval {interval + 1}/12: {start_time} to {end_time}")

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

    print(f"HTTP status : {response.status_code}")
    if response.status_code != 200:
        raise Exception(
            f"API call failed for interval {start_time} - {end_time}: "
            f"{response.text[:100]}"
        )

    hits = response.json().get("hits", {}).get("hits", [])
    all_hits.extend(hits)
    print(f"  -> Records in this interval: {len(hits)} | Total so far: {len(all_hits)}")

print(f"\nAll 12 intervals fetched successfully.")
print(f"Total records fetched: {len(all_hits)}")
