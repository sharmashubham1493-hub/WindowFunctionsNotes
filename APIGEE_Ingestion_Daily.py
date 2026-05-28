# Databricks notebook source

# COMMAND ----------
import warnings
import os
import json
import shutil
from datetime import datetime, timedelta
import pytz
import requests
import urllib3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType

# COMMAND ----------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

spark = SparkSession.builder.appName("APIGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------
print("Setting up batch id...")
batch_id = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")

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
print(f"file_date : {file_date}")

scope_url1 = ["SWCC", "TDCC"]
scope_url2 = ["PPCC", "PBCC", "PTCC"]

print(f"URL_1 scopes : {scope_url1}")
print(f"URL_2 scopes : {scope_url2}")

# Temp DBFS paths — one folder per URL, each interval written as a separate JSON part
tmp_base   = f"/dbfs/tmp/apigee_ingestion/{batch_id}"
dbfs_url1  = f"dbfs:/tmp/apigee_ingestion/{batch_id}/url1"
dbfs_url2  = f"dbfs:/tmp/apigee_ingestion/{batch_id}/url2"

os.makedirs(f"{tmp_base}/url1", exist_ok=True)
os.makedirs(f"{tmp_base}/url2", exist_ok=True)

# COMMAND ----------
# ── Fetch from URL_1 (SWCC + TDCC) — write each interval straight to DBFS ───
# No Python list accumulation: one hour at a time → disk → next hour.
total_hits_1 = 0

for interval in range(24):
    start_time = f"{file_date}T{interval:02d}:00:00.000"
    end_time   = f"{file_date}T{interval:02d}:59:59.999"
    print(f"[URL_1] interval {interval + 1:02d}/24 : {start_time}  →  {end_time}")

    response = requests.post(
        url_1,
        headers=headers,
        auth=(user, psd),
        json={
            "size": 10000,
            "fields": ["*"],
            "query": {"bool": {"must": [
                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                {"terms": {"Scope.keyword": scope_url1}},
            ]}},
        },
        verify=False,
        timeout=120,
    )

    if response.status_code != 200:
        raise Exception(
            f"[URL_1] API failed {start_time}-{end_time}: {response.text[:100]}"
        )

    hits = response.json().get("hits", {}).get("hits", [])
    total_hits_1 += len(hits)

    # Flatten and write this interval immediately; do not keep in memory
    lines = [
        json.dumps({k: (v[0] if isinstance(v, list) and v else v)
                    for k, v in record["fields"].items()})
        for record in hits
    ]
    with open(f"{tmp_base}/url1/part_{interval:02d}.json", "w") as fh:
        fh.write("\n".join(lines))

print(f"\n[URL_1] Done. Total records written to DBFS: {total_hits_1}")

# COMMAND ----------
# ── Fetch from URL_2 (PPCC + PBCC + PTCC) — same pattern ────────────────────
total_hits_2 = 0

for interval in range(24):
    start_time = f"{file_date}T{interval:02d}:00:00.000"
    end_time   = f"{file_date}T{interval:02d}:59:59.999"
    print(f"[URL_2] interval {interval + 1:02d}/24 : {start_time}  →  {end_time}")

    response = requests.post(
        url_2,
        headers=headers,
        auth=(user, psd),
        json={
            "size": 10000,
            "fields": ["*"],
            "query": {"bool": {"must": [
                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                {"terms": {"Scope.keyword": scope_url2}},
            ]}},
        },
        verify=False,
        timeout=120,
    )

    if response.status_code != 200:
        raise Exception(
            f"[URL_2] API failed {start_time}-{end_time}: {response.text[:100]}"
        )

    hits = response.json().get("hits", {}).get("hits", [])
    total_hits_2 += len(hits)

    lines = [
        json.dumps({k: (v[0] if isinstance(v, list) and v else v)
                    for k, v in record["fields"].items()})
        for record in hits
    ]
    with open(f"{tmp_base}/url2/part_{interval:02d}.json", "w") as fh:
        fh.write("\n".join(lines))

print(f"\n[URL_2] Done. Total records written to DBFS: {total_hits_2}")

# COMMAND ----------
# ── Read df_1 and df_2 from DBFS (Spark reads distributed — not into driver) ─
df_1 = spark.read.option("mergeSchema", "true").json(dbfs_url1)
df_2 = spark.read.option("mergeSchema", "true").json(dbfs_url2)

print(f"[df_1 / SWCC+TDCC]       columns : {len(df_1.columns)}")
print(f"[df_2 / PPCC+PBCC+PTCC]  columns : {len(df_2.columns)}")

# COMMAND ----------
# ── Combine ──────────────────────────────────────────────────────────────────
combined_df = df_1.unionByName(df_2, allowMissingColumns=True)
print(f"Combined DataFrame — columns: {len(combined_df.columns)}, "
      f"total records (from API): {total_hits_1 + total_hits_2}")

# COMMAND ----------
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
# ── Summary ───────────────────────────────────────────────────────────────────
print("\n===== Job Results =====")
for r in job_results:
    print(r)

# COMMAND ----------
# ── Cleanup DBFS temp files ───────────────────────────────────────────────────
shutil.rmtree(f"{tmp_base}", ignore_errors=True)
print(f"Cleaned up temp path: {tmp_base}")
