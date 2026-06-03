# Databricks notebook source
# APIGEE Data Ingestion - 30-Minute Interval with Full Pagination
#
# Key change from 15-min version:
#   - interval_minute changed from 15 → 30  (48 API calls per journey instead of 96)
#   - Added search_after pagination so every window fetches ALL records, not just the first 10,000
#
# Why this matters:
#   Elasticsearch caps a single response at 10,000 hits (the "size" limit).
#   With 30-min windows the record count per window doubles, so any window with
#   >10,000 hits would silently truncate without pagination.
#   search_after pages through results in sorted order until the window is exhausted.

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from requests.auth import HTTPBasicAuth
from io import StringIO
import pandas as pd
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("APIGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------

# Timezone
kolkata_tz = pytz.timezone("Asia/Kolkata")

# COMMAND ----------

# Elasticsearch credentials and endpoint
user = "apigee_dap"
pwd  = dbutils.secrets.get(scope="apigee_dap", key="apigee_dap")   # keep secret out of plain text

update_url = "https://10.227.12.188:9201/apigee_updated_solace/_search"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json; compatible-with=8",
}

# COMMAND ----------

SCOPE_JOURNEY_MAP = {
    "VTCC":  "apigee-update-vtcc-logs",
    "TDC":   "apigee-update-tdc-logs",
    "PFCC":  "apigee-update-pfcc-logs",
    "PBCC":  "apigee-update-pbcc-logs",
    "PTCC":  "apigee-update-ptcc-logs",
}

# COMMAND ----------

MASTER_COLUMNS = [
    "apiproxyname",
    "apiproxyname.keyword",
    "backend_response_time",
    "client.received.start.timestamp",
    "client.sent.end.timestamp",
    "elkGatewayErrorMessage",
    "elkGatewayErrorMessage.keyword",
    "elkGatewayStatusCode",
    "elkRoutingStatusCode",
    "elkRoutingStatusCode.keyword",
    "log_timestamp",
    "req-journeyID",
    "req-journeyID.keyword",
    "requestcontentlength",
    "requestcontentlength.keyword",
    "resp-responseCode",
    "resp-responseCode.keyword",
    "response",
    "response.keyword",
    "response_time",
    "target.received.end.timestamp",
    "target.sent.start.timestamp",
    # add more columns as needed
]

# COMMAND ----------

# ── Helper: fetch ALL records for one time window using search_after pagination ──
#
# Elasticsearch returns at most `page_size` documents per request.
# search_after uses the sort values of the last document to request the next page,
# avoiding the 10,000-document deep-pagination limit of from+size.
#
# Sort order: @timestamp ASC + _id ASC ensures a stable, unique cursor.

PAGE_SIZE = 10_000   # maximum Elasticsearch allows per request

def fetch_all_hits_for_window(start_time: str, end_time: str, all_scopes: list) -> list:
    """Return every ES hit between start_time and end_time across all scopes."""
    hits       = []
    sort_after = None          # None on first page; list of sort values on subsequent pages
    page       = 0

    while True:
        page += 1
        body = {
            "size": PAGE_SIZE,
            "sort": [
                {"@timestamp": {"order": "asc"}},
                {"_id":        {"order": "asc"}},   # tie-break for stable cursor
            ],
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                        {"terms": {"Scope.keyword": all_scopes}},
                    ]
                }
            },
            "_source": False,
            "fields": ["*"],
        }

        # Attach the cursor on pages 2+
        if sort_after is not None:
            body["search_after"] = sort_after

        response = requests.post(
            update_url,
            headers=headers,
            auth=HTTPBasicAuth(user, pwd),
            json=body,
            verify=False,
            timeout=120,
        )

        if response.status_code != 200:
            raise Exception(
                f"API call failed [{response.status_code}] for window "
                f"{start_time} → {end_time}, page {page}: {response.text[:300]}"
            )

        page_hits = response.json().get("hits", {}).get("hits", [])
        hits.extend(page_hits)

        # Stop when Elasticsearch returns fewer records than the page size
        if len(page_hits) < PAGE_SIZE:
            break

        # Advance the cursor to the sort values of the last document
        sort_after = page_hits[-1]["sort"]

    return hits


# COMMAND ----------

# ── Main ingestion loop ──

# Date to ingest (T-2 by default)
file_date = (datetime.now(kolkata_tz).date() - timedelta(days=2)).strftime("%Y-%m-%d")

# ─────────────────────────────────────────────
#  CHANGE: interval_minute  15 → 30
#  This halves the number of API calls (96 → 48 per journey)
#  while the pagination helper above guarantees no records are missed.
# ─────────────────────────────────────────────
interval_minute  = 30
total_intervals  = (24 * 60) // interval_minute   # = 48

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
all_hits   = []

print(f"Ingesting date : {file_date}")
print(f"Interval       : {interval_minute} min  ({total_intervals} windows/day)")
print(f"Journeys       : {all_scopes}\n")

for i in range(total_intervals):
    start_total_min = i * interval_minute
    end_total_min   = start_total_min + interval_minute - 1   # inclusive end

    start_hour, start_min_part = divmod(start_total_min, 60)
    end_hour,   end_min_part   = divmod(end_total_min,   60)

    start_time = f"{file_date}T{start_hour:02d}:{start_min_part:02d}:00.000"
    end_time   = f"{file_date}T{end_hour:02d}:{end_min_part:02d}:59.999"

    print(f"Fetching window {i+1:>2}/{total_intervals}: {start_time}  →  {end_time}", end="  ")

    window_hits = fetch_all_hits_for_window(start_time, end_time, all_scopes)
    all_hits.extend(window_hits)

    print(f"| {len(window_hits):>7,} records  (running total: {len(all_hits):>10,})")

print(f"\nAll {total_intervals} windows fetched.  Total records: {len(all_hits):,}")

# COMMAND ----------

# ── Flatten hits and build Spark DataFrame ──

import json

flattened_records = []
for record in all_hits:
    flat = {}
    for key, value in record.get("fields", {}).items():
        flat[key] = (
            json.dumps(value, list) if isinstance(value, list) and len(value) > 0
            else value[0]            if isinstance(value, list) and len(value) > 0
            else value
        )
    flattened_records.append(flat)

pdf = pd.DataFrame(flattened_records)
print(f"Total columns: {len(pdf.columns)}")

# Align to MASTER_COLUMNS schema (fill missing cols with None)
existing_cols  = set(pdf.columns)
aligned_pdf    = pdf.reindex(columns=MASTER_COLUMNS)   # keeps only master cols, fills missing with NaN

df = spark.createDataFrame(aligned_pdf)
df = spark.sparkContext.parallelize(flattened_records).toDF()

existing_cols = set(df.columns)
aligned_df = df.select(
    [
        F.col(f"`{c}`") if c in existing_cols else F.lit(None).cast(StringType()).alias(c)
        for c in MASTER_COLUMNS
    ]
)

print(f"Source columns  : {len(df.columns)}")
print(f"Aligned columns : {len(aligned_df.columns)}")
missing = [c for c in MASTER_COLUMNS if c not in existing_cols]
print(f"Null-filled ({len(missing)}): {missing}")

# COMMAND ----------

# (5) Spark jobs — write to ADLS

job_results = []

for scope_val, journey in SCOPE_JOURNEY_MAP.items():
    print(f"\n{'='*60}")
    print(f"scope={scope_val}   journey={journey}")

    journey_df = aligned_df.filter(col("Scope.keyword") == scope_val)
    count = journey_df.count()

    if count == 0:
        print(f"  No data for {journey}. Skipping.")
        job_results.append({"journey": journey, "status": "no_data"})
        continue

    print(f"  Records for this scope: {count}")

    # Look up ADLS path from metadata table
    metadata_rows = spark.sql(f"""
        select ADLS_Path from sindhu_db_prod.ddi_metadata_db.api_config_table
        where Pipeline = 'APIGEE_Ingestion_Daily' and Journey = '{journey}'
    """).collect()

    if not metadata_rows:
        print(f"  No metadata found for {journey}. Skipping.")
        job_results.append({"journey": journey, "status": "no_metadata"})
        continue

    api_metadata  = {row["GroupID"]: row.asDict() for row in metadata_rows}
    adls_path_base = metadata_rows[0]["ADLS_Path"]
    adls_raw_path  = (
        f"abfss://rawddiprodvyaparaadlsstd.dfs.core.windows.net"
        f"{adls_path_base}/landing/{file_date}/"
    )

    # Write parquet
    journey_df_dated = journey_df.withColumn("file_date", lit(file_date))
    journey_df_dated.write.mode("overwrite").parquet(adls_raw_path)

    print(f"  Writing to: {adls_raw_path}")
    print(f"  Write complete for {journey}.  records: {count}")
    job_results.append({"journey": journey, "status": "SUCCESS", "records": count})

print("\n\nIngestion summary:")
for r in job_results:
    print(f"  {r}")
