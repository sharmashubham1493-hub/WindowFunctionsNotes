# Databricks notebook source

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
import time
import boto3
import urllib3
from pytz import timezone
import re
import os
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("APiGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------

# Bootstrap the batch_id for this job
kolkata_tz = timezone("Asia/Kolkata")
batch_id = datetime.now(kolkata_tz).strftime('%Y%m%d%H%M%S')
print(batch_id)

# COMMAND ----------

# Journey name mapping: scope acronym -> Apigee log index name
SCOPE_JOURNEY_MAP = {
    'TDCC': 'apigee-update-tdcc-logs',
    'SMCC': 'apigee-update-succ-logs',
    'PMCC': 'apigee-update-pmcc-logs',
    'PTCC': 'apigee-update-ptcc-logs',
    'PBCC': 'apigee-update-pbcc-logs',
}

# COMMAND ----------

# Credentials and endpoint
user = dbutils.secrets.get(scope="apigee_obj", key="user")
pwd  = dbutils.secrets.get(scope="apigee_obj", key="pwd")
url  = "https://10.222.72.188:9200/apigee-updated*/_search"

# COMMAND ----------

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
print(f'all_scopes: {all_scopes}')

file_date = (datetime.now(kolkata_tz).date() - timedelta(days=1)).strftime('%Y-%m-%d')
print(file_date)

# COMMAND ----------

headers = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept":        "application/vnd.elasticsearch+json; compatible-with=8",
}

# ---------------------------------------------------------------------------
# FIX: use search_after pagination to retrieve ALL records.
# The previous "size": 10000 hard-cap silently truncated results to 10 k rows.
# Elasticsearch's search_after requires a stable sort; we sort by timestamp +
# _id so that every page starts exactly where the previous one ended.
# ---------------------------------------------------------------------------

PAGE_SIZE  = 10000
all_hits   = []
search_after = None

while True:
    query_body = {
        "size": PAGE_SIZE,
        "sort": [
            {"@timestamp": {"order": "asc"}},
            {"_id":         {"order": "asc"}},
        ],
        "query": {
            "bool": {
                "must": [
                    {"terms": {"scope.keyword": all_scopes}},
                    {
                        "range": {
                            "@timestamp": {
                                "gte": f"{file_date}T00:00:00",
                                "lte": f"{file_date}T23:59:59",
                            }
                        }
                    },
                ]
            }
        },
    }

    if search_after:
        query_body["search_after"] = search_after

    response = requests.post(
        url,
        headers=headers,
        auth=(user, pwd),
        json=query_body,
        verify=False,
        timeout=120,
    )

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code}")

    response_json = response.json()
    hits = response_json.get("hits", {}).get("hits", [])

    if not hits:
        break

    all_hits.extend(hits)
    print(f"Page fetched: {len(hits)} records | Running total: {len(all_hits)}")

    if len(hits) < PAGE_SIZE:
        # Fewer records than page size means this was the last page
        break

    # Advance the cursor to the sort values of the last hit
    search_after = hits[-1]["sort"]

print(f"Total records fetched across all pages: {len(all_hits)}")

if not all_hits:
    raise Exception(f"No records returned for date: {file_date}")

# COMMAND ----------

# Build Spark DataFrame from all collected hits
source_records = [json.dumps(hit["_source"]) for hit in all_hits]
rdd = spark.sparkContext.parallelize(source_records)
df  = spark.read.json(rdd)

print(f"DataFrame row count: {df.count()}")
display(df)
