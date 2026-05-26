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
# search_after pagination to retrieve ALL records beyond the 10k ES limit.
#
# Performance fixes vs. previous version:
#   1. Tiebreaker changed from "_id" -> "_shard_doc"
#      _id sort forces expensive field-data loading for every doc.
#      _shard_doc is a built-in ES 7.12+ tiebreaker with zero overhead.
#   2. Query clauses moved to "filter" context (was "must").
#      filter skips relevance scoring and enables shard-level caching.
#   3. "track_total_hits": false — stops ES counting every matching doc
#      per page, which was adding significant overhead for large indices.
# ---------------------------------------------------------------------------

PAGE_SIZE    = 10000
all_hits     = []
search_after = None

while True:
    query_body = {
        "size": PAGE_SIZE,
        "track_total_hits": False,
        "sort": [
            {"@timestamp": {"order": "asc"}},
            {"_shard_doc":  "asc"},
        ],
        "query": {
            "bool": {
                "filter": [
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
