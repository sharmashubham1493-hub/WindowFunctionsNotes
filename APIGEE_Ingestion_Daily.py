# Databricks notebook source

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from requests.auth import HTTPBasicAuth
from io import StringIO
import pandas as pd
import urllib3
import os

# COMMAND ----------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("APIGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------

# Setup the batch id for this job
batch_id = int(datetime.now(pytz.timezone("Asia/Kolkata")).strftime('%Y%m%d%H%M%S'))
print(batch_id)

# COMMAND ----------

# Scope → GroupID (journey) mapping — 5 journeys
# Each scope key matches the Scope.keyword value in Elasticsearch records
# Each journey value matches the GroupID in the metadata config table
SCOPE_JOURNEY_MAP = {
    'SWCC': 'apigee-update-swcc-logs',
    'TDCC': 'apigee-update-tdcc-logs',
    'PPCC': 'apigee-update-ppcc-logs',
    'PBCC': 'apigee-update-pbcc-logs',
    'PTCC': 'apigee-update-ptcc-logs',
}

# COMMAND ----------

user = "apigee_obp"
pwd  = "apigee@123"

# COMMAND ----------

kolkata_tz = pytz.timezone("Asia/Kolkata")
url = "https://10.227.12.188:9201/apigee_updated-*/search"

headers = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept":        "application/vnd.elasticsearch+json; compatible-with=8",
}

file_date = (datetime.now(kolkata_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
print(file_date)

# COMMAND ----------

# ── Main ingestion loop ─────────────────────────────────────────────────────
# Each iteration: one API call per scope, parse → DataFrame → write to ADLS
# This guarantees every journey lands its own data at its own ADLS raw path.

job_results = []

for scope_val, journey in SCOPE_JOURNEY_MAP.items():
    print(f"\n{'='*60}")
    print(f"Processing scope={scope_val}  journey={journey}")

    # ── 1. Fetch data from Elasticsearch for this scope only ──────────────
    try:
        response = requests.post(
            url,
            headers=headers,
            auth=(user, pwd),
            json={
                "size": 10000,
                "fields": ["*"],
                "_source": False,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": f"{file_date}T00:00:00",
                                        "lte": f"{file_date}T23:59:59",
                                    }
                                }
                            },
                            {"terms": {"Scope.keyword": [scope_val]}},
                        ]
                    }
                },
            },
            verify=False,
            timeout=120,
        )
        print(f"  HTTP status : {response.status_code}")
    except Exception as e:
        print(f"  API call failed for {journey}: {e}")
        job_results.append({"journey": journey, "status": "API_ERROR", "error": str(e)})
        continue

    if response.status_code != 200:
        print(f"  Non-200 response for {journey}: {response.text[:500]}")
        job_results.append({"journey": journey, "status": f"HTTP_{response.status_code}"})
        continue

    # ── 2. Parse and flatten Elasticsearch hits ───────────────────────────
    response_json = response.json()
    hits = response_json.get("hits", {}).get("hits", [])
    print(f"  Records found: {len(hits)}")

    if not hits:
        print(f"  No data for {journey} on {file_date}. Skipping.")
        job_results.append({"journey": journey, "status": "NO_DATA"})
        continue

    flattened_records = []
    for record in hits:
        flat = {}
        for key, value in record.get("fields", {}).items():
            # Elasticsearch returns every field as a list; unwrap single-value lists
            flat[key] = value[0] if isinstance(value, list) and len(value) > 0 else value
        flattened_records.append(flat)

    # ── 3. Build Spark DataFrame ──────────────────────────────────────────
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in flattened_records])
    df  = spark.read.options(mergeSchema=True).json(rdd)
    print(f"  DataFrame shape: {df.count()} rows × {len(df.columns)} columns")

    # ── 4. Resolve ADLS raw path from the metadata config table ──────────
    metadata_df = spark.sql(f"""
        SELECT *
        FROM   sindhu_db.prod.ddi_metadata_db.api_config_table
        WHERE  Pipeline = 'APIGEE_Ingestion_Daily'
        AND    GroupID  = '{journey}'
    """)
    api_metadata = [row.asDict() for row in metadata_df.collect()]

    if not api_metadata:
        print(f"  No metadata config found for journey={journey}. Skipping write.")
        job_results.append({"journey": journey, "status": "NO_METADATA"})
        continue

    # ── 5. Write to every configured ADLS path for this journey ──────────
    for item in api_metadata:                          # iterate the list, NOT .items()
        adls_raw_path = (
            f"abfss://raw@ddiprodvyapaaradlstd.dfs.core.windows.net"
            f"/{item['ADLS_Path']}/{batch_id}/"
        )
        print(f"  Writing to: {adls_raw_path}")
        df.write.mode("overwrite").parquet(adls_raw_path)
        print(f"  Write complete for {journey}.")

    job_results.append({"journey": journey, "status": "SUCCESS", "records": len(hits)})

# COMMAND ----------

# ── Summary ───────────────────────────────────────────────────────────────────
print("\nAPGEE ingestion completed for all journeys\n")
for r in job_results:
    status  = r["status"]
    journey = r["journey"]
    records = r.get("records", "-")
    print(f"  {status:<15} journey={journey}  records={records}")
