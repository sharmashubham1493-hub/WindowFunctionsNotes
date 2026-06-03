# Databricks notebook source

# COMMAND ----------
# Cell 1 – Imports (image 1)

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
# Cell 2 – Suppress SSL warnings (image 1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------
# Cell 3 – Initialise Spark session (image 1)

# Initialize Spark session
spark = SparkSession.builder.appName("APIGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------
# Cell 4 – Timezone (referenced in cell 8 via kolkata_tz)

kolkata_tz = pytz.timezone("Asia/Kolkata")

# COMMAND ----------
# Cell 5 – Journey map (image 2)

SCOPE_JOURNEY_MAP = {
    "VTCC": "apigee-update-vtcc-logs",
    "TDC":  "apigee-update-tdc-logs",
    "PFCC": "apigee-update-pfcc-logs",
    "PBCC": "apigee-update-pbcc-logs",
    "PTCC": "apigee-update-ptcc-logs",
}

# COMMAND ----------
# Cell 6 – Credentials (image 2)

user = "apigee_dap"
pwd  = dbutils.secrets.get(scope="apigee_dap", key="apigee_dap")

# COMMAND ----------
# Cell 7 – Elasticsearch endpoint + headers (image 2)

update_url = "https://10.227.12.188:9201/apigee_updated_solace/_search"

headers = {
    "Content-Type": "application/json",
    "Accept":        "application/json; compatible-with=8",
}

# COMMAND ----------
# Cell 8 – Fetch loop  (images 3 & 4)
#
# CHANGES vs original:
#   1. interval_minute : 15  →  30   (48 API calls/day instead of 96)
#   2. Single requests.post replaced with a search_after pagination loop so
#      every record in a 30-min window is fetched even when it exceeds 10 000 hits.

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
# scope = ['VTCC', 'TDC', 'PFCC']
# scope = ['PBCC', 'PTCC']

file_date = (datetime.now(kolkata_tz).date() - timedelta(days=2)).strftime("%Y-%m-%d")
batch_id  = datetime.now(kolkata_tz).strftime("%Y%m%d%H%M%S")
print(file_date)

# ── CHANGED 15 → 30 ──────────────────────────────────────────────────────────
interval_minute = 30                               # was 15
total_intervals = (24 * 60) // interval_minute     # 48 windows  (was 96)
# ─────────────────────────────────────────────────────────────────────────────

all_hits        = []
start_total_min = 0

for i in range(total_intervals):
    end_total_min  = start_total_min + interval_minute

    start_hour, start_min_part = divmod(start_total_min,      60)
    end_hour,   end_min_part   = divmod(end_total_min - 1,    60)   # -1 → inclusive end minute

    start_time = f"{file_date}T{start_hour:02d}:{start_min_part:02d}:00.000"
    end_time   = f"{file_date}T{end_hour:02d}:{end_min_part:02d}:59.999"

    print(f"Fetching interval {i + 1}/{total_intervals}: {start_time} to {end_time}")

    # ── ADDED: search_after pagination ───────────────────────────────────────
    # Elasticsearch returns at most 10 000 hits per request (the "size" cap).
    # With 30-min windows a single call may silently truncate.
    # search_after pages through all results using the last document's sort
    # values as a cursor; it has no depth limit.
    sort_after  = None
    page        = 0
    window_hits = 0

    while True:
        page += 1
        body = {
            "size": 10000,
            "sort": [
                {"@timestamp": {"order": "asc"}},
                {"_id":        {"order": "asc"}},   # tie-break for a stable cursor
            ],
            "fields":  ["*"],
            "_source": False,
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                        {"terms": {"Scope.keyword": all_scopes}},
                    ]
                }
            },
        }

        if sort_after is not None:
            body["search_after"] = sort_after          # advance cursor on page 2+

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
                f"API call failed for interval {start_time} : {end_time}, "
                f"page {page}: {response.text[:200]}"
            )

        page_hits    = response.json().get("hits", {}).get("hits", [])
        all_hits.extend(page_hits)
        window_hits += len(page_hits)

        if len(page_hits) < 10000:
            break                                      # last page – no more records

        sort_after = page_hits[-1]["sort"]             # move cursor forward
    # ─────────────────────────────────────────────────────────────────────────

    start_total_min = end_total_min                    # advance to next window

print(f"{total_intervals} intervals fetched successfully, total records: {len(all_hits)}")

# COMMAND ----------
# Cell 9 – Flatten ES hits into a list of plain dicts  (image 5)

import json
from pyspark.sql import functions as F

flattened_records = []
for record in all_hits:
    flat = {}
    for key, value in record.get("fields", {}).items():
        flat[key] = value[0] if isinstance(value, list) and len(value) > 0 else value
    flattened_records.append(flat)

# COMMAND ----------
# Cell 10 – Create Spark DataFrame  (image 5)

# (1) Spark jobs
rdd = spark.sparkContext.parallelize([json.dumps(r) for r in flattened_records])
df  = spark.read.option("mergeSchema", "true").json(rdd)
print(f"total columns: {len(df.columns)}")

# COMMAND ----------
# Cell 20 – Master column list + align DataFrame  (images 6, 7, 8)

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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
    "BackendContentType.keyword",
    "BackendErrorMessage",
    "BackendErrorMessage.keyword",
    "ClientIP.keyword",
    "Environment.name.keyword",
    "Environment_name",
    "GatewayErrorCode",
    "GatewayErrorCode.keyword",
    "GatewayErrorMessage",
    "GatewayErrorMessage.keyword",
    "host",
    "host.keyword",
    "@timestamp",
    "Scope.keyword",
    "req-journeyID.keyword",
    "resp-responseCode.keyword",
    "APIProxyType",
    "APIProxyType.keyword",
    "BackendComp",
    "BackendComp.keyword",
    "CriticalityFlag",
    "CriticalityFlag.keyword",
    "Environment_name.keyword",
    "GatewayErrorCode",
    "GatewayErrorMessage.keyword",
    "GatewayMappedErrorMessage",
    "GatewayMappedErrorMessage.keyword",
    "Platform",
    "Platform.keyword",
    "RequestType",
    "RequestType.keyword",
    "Scope",
    "StatusCode",
    "StatusCode.keyword",
    "apigee-developer-app-name.keyword",
    "apigee-developer-app-name",
    "apigee-antispoofcomp.name",
    "apigee-antispoofcomp.name.keyword",
    "@ProxyType",
    "@ProxyType.keyword",
    "elkRoutingStatusCode",
    "elkGatewayErrorMessage",
    "response_time",
]

existing_cols = set(df.columns)
aligned_df    = df.select(
    [
        F.col(f"`{c}`").alias(c) if c in existing_cols else F.lit(None).cast(StringType()).alias(c)
        for c in MASTER_COLUMNS
    ]
)

print(f"Source columns  : {len(df.columns)}")
print(f"Aligned columns : {len(aligned_df.columns)}")
missing = [c for c in MASTER_COLUMNS if c not in existing_cols]
print(f"Null-filled ({len(missing)}): {missing}")

# COMMAND ----------
# Cell 22 – Write one Parquet file per journey to ADLS  (images 9 & 10)

job_results = []

for scope_val, Journey in SCOPE_JOURNEY_MAP.items():
    print(f"\n{'='*60}")
    print(f"\nscope_val={scope_val}  Journey={Journey}")

    # filter to this journey's records
    journey_df = aligned_df.filter(col("Scope.keyword") == scope_val)
    count      = journey_df.count()
    print(f"Records for this scope: {count}")

    if count == 0:
        print(f"No data for {Journey} on {file_date}. Skipping.")
        job_results.append({"Journey": Journey, "status": "no_date"})
        continue

    # lookup ADLS landing path from metadata table
    metadata_rows = spark.sql(f"""
        select ADLS_Path
        from   sindhu_db_prod.ddi_metadata_db.api_config_table
        where  Pipeline = 'APIGEE_Ingestion_Daily'
        and    Journey  = '{Journey}'
    """).collect()

    if not metadata_rows:
        print(f"No metadata found for {Journey}. Skipping.")
        job_results.append({"Journey": Journey, "status": "NO_metadata"})
        continue

    # api_metadata  = {row["GroupID"]: row.asDict() for row in metadata_rows}
    adls_path_base = metadata_rows[0]["ADLS_Path"]
    adls_raw_path  = (
        f"abfss://rawddiprodvyaparaadlsstd.dfs.core.windows.net"
        f"{adls_path_base}/landing/{batch_id}/"
    )

    # write to ADLS
    journey_df.write.mode("overwrite").parquet(adls_raw_path)
    print(f"Writing to: {adls_raw_path}")
    print(f"Write complete for {Journey}. records: {count}")
    job_results.append({"Journey": Journey, "status": "SUCCESS", "records": count})
