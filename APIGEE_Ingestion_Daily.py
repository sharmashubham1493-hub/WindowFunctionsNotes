# Databricks notebook source

# COMMAND ----------
import warnings
import time
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col
import os
import pandas as pd
import urllib3
import json
import requests

# COMMAND ----------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize Spark session
spark = SparkSession.builder.appName("APIGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------
# Setup the batch id for this job
print(f"Setting up the batch id for this job")
batch_id = str(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S"))

# COMMAND ----------
SCOPE_JOURNEY_MAP = {
    "SWCC": "apigee-update-swcc-log",
    "TDCC": "apigee-update-tdcc-log",
    "PPCC": "apigee-update-ppcc-log",
    "PBCC": "apigee-update-pbcc-log",
    "PTCC": "apigee-update-ptcc-log",
}

# COMMAND ----------
kolkata_tz = pytz.timezone("Asia/Kolkata")

# URL_1 serves SWCC and TDCC data
url_1 = "https://10.227.12.188:9201/apigee_updated_solace/_search"

# URL_2 serves PPCC, PBCC and PTCC data  -- replace with actual endpoint
url_2 = "https://<URL_2_HOST>/apigee_updated_solace/_search"

headers = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
}

file_date = (datetime.now(kolkata_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
print(file_date)

# Scopes split by URL
scope_url1 = ["SWCC", "TDCC"]
scope_url2 = ["PPCC", "PBCC", "PTCC"]

print(f"URL_1 scopes : {scope_url1}")
print(f"URL_2 scopes : {scope_url2}")

# COMMAND ----------
# ── Fetch from URL_1 (SWCC + TDCC) ──────────────────────────────────────────
all_hits_1 = []

for interval in range(24):
    start_hour = interval
    start_time = f"{file_date}T{start_hour:02d}:00:00.000"
    end_time   = f"{file_date}T{start_hour:02d}:59:59.999"

    print(f"[URL_1] Fetching interval {(interval + 1):02d}/24: {start_time} to {end_time}")

    response = requests.post(
        url_1,
        headers=headers,
        auth=(user, psd),
        json={
            "size": 10000,
            "fields": ["*"],
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                        {"terms": {"Scope.keyword": scope_url1}},
                    ]
                }
            },
        },
        verify=False,
        timeout=120,
    )

    if response.status_code != 200:
        raise Exception(
            f"[URL_1] API call failed for interval {start_time} - {end_time}: "
            f"{response.text[:100]}"
        )

    resp_json = response.json()
    hits = resp_json.get("hits", {}).get("hits", [])
    all_hits_1.extend(hits)

print(f"\n[URL_1] All 24 intervals fetched. Total records: {len(all_hits_1)}")

# COMMAND ----------
# ── Fetch from URL_2 (PPCC + PBCC + PTCC) ───────────────────────────────────
all_hits_2 = []

for interval in range(24):
    start_hour = interval
    start_time = f"{file_date}T{start_hour:02d}:00:00.000"
    end_time   = f"{file_date}T{start_hour:02d}:59:59.999"

    print(f"[URL_2] Fetching interval {(interval + 1):02d}/24: {start_time} to {end_time}")

    response = requests.post(
        url_2,
        headers=headers,
        auth=(user, psd),
        json={
            "size": 10000,
            "fields": ["*"],
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                        {"terms": {"Scope.keyword": scope_url2}},
                    ]
                }
            },
        },
        verify=False,
        timeout=120,
    )

    if response.status_code != 200:
        raise Exception(
            f"[URL_2] API call failed for interval {start_time} - {end_time}: "
            f"{response.text[:100]}"
        )

    resp_json = response.json()
    hits = resp_json.get("hits", {}).get("hits", [])
    all_hits_2.extend(hits)

print(f"\n[URL_2] All 24 intervals fetched. Total records: {len(all_hits_2)}")

# COMMAND ----------
# ── Build df_1 from URL_1 hits (SWCC + TDCC) ────────────────────────────────
def hits_to_dataframe(all_hits, label):
    flattened_records = []
    for record in all_hits:
        flat = {}
        for key, value in record["fields"].items():
            flat[key] = value[0] if isinstance(value, list) and len(value) > 0 else value
        flattened_records.append(flat)

    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in flattened_records])
    df = spark.read.option("mergeSchema", "true").json(rdd)
    print(f"[{label}] total columns: {len(df.columns)}, total rows: {df.count()}")
    return df


df_1 = hits_to_dataframe(all_hits_1, "URL_1 / SWCC+TDCC")
df_2 = hits_to_dataframe(all_hits_2, "URL_2 / PPCC+PBCC+PTCC")

# COMMAND ----------
# ── Combine df_1 and df_2 ────────────────────────────────────────────────────
combined_df = df_1.unionByName(df_2, allowMissingColumns=True)
print(f"Combined DataFrame — rows: {combined_df.count()}, columns: {len(combined_df.columns)}")

# COMMAND ----------
from pyspark.sql.types import StringType

MASTER_COLUMNS = [
    "@timestamp",
    "APIProxyType",
    "Apigee.apiproduct.name",
    "Apigee.apiproduct.name.keyword",
    "Apigee.developer.app.name",
    "Apigee.developer.app.name.keyword",
    "BackendContentType",
    "BackendContentType.keyword",
    "BackendErrorMessage",
    "BackendErrorMessage.keyword",
    "ClientIP",
    "ClientIP.keyword",
    "Criticality",
    "Criticality.keyword",
    "Environment.name",
    "Environment.name.keyword",
    "GatewayErrorCode",
    "GatewayErrorCode.keyword",
    "GatewayErrorMessage",
    "GatewayErrorMessage.keyword",
    "Host",
    "Host.keyword",
    "Platform",
    "Platform.keyword",
    "Proxy.basepath",
    "Proxy.basepath.keyword",
    "Proxy.pathsuffix",
    "Proxy.pathsuffix.keyword",
    "RoutingStatusCode",
    "RoutingStatusCode.keyword",
    "Scope",
    "Scope.keyword",
    "Service-Virtualization",
    "Service-Virtualization.keyword",
    "Transaction_id",
    "apigee_update",
    "apiproxyname",
    "apiproxyname.keyword",
    "backend_response_time",
    "client_received_start.timestamp",
    "elkGatewayErrorMessage",
    "elkGatewayErrorMessage.keyword",
    "elkRoutingStatusCode",
    "elkRoutingStatusCode.keyword",
    "req-journeyID",
    "req-journeyID.keyword",
    "requestcontent_length",
    "requestcontentlength.keyword",
    "resp-responseCode",
    "resp-responseCode.keyword",
    "response",
    "response_time",
    "target.received.end.timestamp",
    "target.sent.start.timestamp",
]

existing_cols = set(combined_df.columns)
aligned_df = combined_df.select(
    [
        F.col(f"`{c}`") if c in existing_cols else F.lit(None).cast(StringType()).alias(c)
        for c in MASTER_COLUMNS
    ]
)

print(f"Source columns   : {len(combined_df.columns)}")
print(f"Aligned columns  : {len(aligned_df.columns)}")
missing = [c for c in MASTER_COLUMNS if c not in existing_cols]
print(f"Null-filled ({len(missing)}) : {missing}")

# COMMAND ----------
# ── Per-journey write to ADLS raw path ──────────────────────────────────────
job_results = []

for scope_val, journey in SCOPE_JOURNEY_MAP.items():
    print(f"\nscope={scope_val}  journey={journey}")

    journey_df = aligned_df.filter(col("Scope.keyword") == scope_val)
    journey_df = journey_df.withColumn("file_date", lit(file_date))
    count = journey_df.count()
    print(f"Records for this scope: {count}")

    if count == 0:
        print(f"No data for {journey} on {file_date}. Skipping.")
        job_results.append({"journey": journey, "status": "no_data"})
        continue

    metadata_rows = spark.sql(
        f"""
        SELECT ADLS_Path
        FROM   sindhu_db_prod.ddl_metadata_db.api_config_table
        WHERE  Pipeline = 'APIGEE_Ingestion_Daily'
        AND    GroupID  = '{journey}'
        """
    ).collect()

    if not metadata_rows:
        print(f"No metadata found for {journey}. Skipping.")
        job_results.append({"journey": journey, "status": "No_metadata"})
        continue

    adls_path_base = metadata_rows[0]["ADLS_Path"]
    adls_raw_path = (
        f"abfss://raw@ddiprodvyapaaradlsstd.dfs.core.windows.net"
        f"/{adls_path_base}/landing/{batch_id}/"
    )

    print(f"Writing to: {adls_raw_path}")
    journey_df.write.mode("overwrite").parquet(adls_raw_path)
    print(f"Write complete for {journey}.")
    job_results.append({"journey": journey, "status": "SUCCESS", "records": count})

# COMMAND ----------
# ── Summary ──────────────────────────────────────────────────────────────────
print("\n===== Job Results =====")
for r in job_results:
    print(r)
