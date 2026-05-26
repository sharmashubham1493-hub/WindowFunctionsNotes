# Databricks notebook source
# MAGIC %md
# MAGIC ## Fix: Ensure all 65 MASTER_COLUMNS are always present (missing ones filled with null)

# COMMAND ----------
# Cell 1 – imports (unchanged)
import requests
import json
import os
from datetime import datetime, timedelta
import pytz
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# Cell 2 – Spark session (unchanged)
# spark = SparkSession.builder.appName("APIDFE_Ingestion_Daily").getOrCreate()

# COMMAND ----------
# Cell 3 – MASTER_COLUMNS: the full list of 65 expected columns
# Replace / extend with your actual 65 column names as they appear in the source data.
MASTER_COLUMNS = [
    "timestamp",
    "@timestamp",
    "Apigee-apiproduct-name",
    "Apigee-developer-app-name",
    "BackendContentType",
    "BackendErrorMessage",
    "ClientIP",
    "Criticality",
    "EnvironmentName",
    "GatewayErrorCode",
    "GatewayErrorMessage",
    "Apigee-developer-app-name-keyword",
    "Apigee-apiproduct-name-keyword",
    "BackendContentType-keyword",
    "BackendErrorMessage-keyword",
    "ClientIP-keyword",
    "EnvironmentName-keyword",
    "GatewayErrorCode-keyword",
    "GatewayErrorMessage-keyword",
    "Apigee-developer-app-name",
    "Apigee-developer-app-app-name",
    "BackendContentType",
    "BackendErrorMessage-type",
    "BackendStatusCode",
    "ClientID",
    "Criticality",
    "Environment",
    "GatewayErrorCode",
    "GatewayErrorFromMessage",
    "Platform",
    "Proxy_basepath",
    "Proxy_basepath-keyword",
    "Proxy_pathsuffix",
    "Proxy_pathsuffix-keyword",
    "RequestStatusCode",
    "RequestStatusLength",
    "ResponseStatusCode",
    "ResponseStatusLength",
    "RoutingStatusCode",
    "Scope",
    "Scope-keyword",
    "Service-Virtualization",
    "Service-Virtualization-keyword",
    "Transaction_id",
    "allBlockingStatusCode",
    "allNonBlockingStatusCode",
    "backend_received_start-timestamp",
    "client_sent_end-timestamp",
    "log_timestamp",
    "reg_journeyID",
    "requestcontentlength",
    "requestcontenttype",
    "requestcontenttype-keyword",
    "req_journeyID",
    "response_keyword",
    "response_time",
    "responsecode",
    "target_received_end-timestamp",
    "target_sent_start-timestamp",
    "allowGatewayErrorCode",
    "allowGatewayErrorCode-keyword",
    "client_sent_end_timestamp",
    "GatewayErrorFromCode",
    "GatewayErrorFromCode-keyword",
    "file_date",
    # Add any remaining columns up to your 65 total here
]

# COMMAND ----------
# Cell 4 – After building df from the API response, align columns ONCE before the loop.
#
# *** THIS IS THE KEY FIX ***
#
# Your original code (cell 13) already creates aligned_df correctly:
#
#   existing_cols = set(df.columns)
#   aligned_df = df.select(
#       [F.col(f"`{c}`") if c in existing_cols
#        else F.lit(None).cast(StringType()).alias(c)
#        for c in MASTER_COLUMNS]
#   )
#
# The only problem was that cell 15 (the per-journey loop) still used `df`
# instead of `aligned_df`.  The two-line fix below is all you need:

# ── in cell 13, keep your existing aligned_df definition as-is ──────────────
existing_cols = set(df.columns)
aligned_df = df.select(
    [
        F.col(f"`{c}`") if c in existing_cols
        else F.lit(None).cast(StringType()).alias(c)
        for c in MASTER_COLUMNS
    ]
)

print(f"Source columns  : {len(df.columns)}")
print(f"Aligned columns : {len(aligned_df.columns)}")
missing = [c for c in MASTER_COLUMNS if c not in existing_cols]
print(f"Null-filled ({len(missing)}): {missing}")

# COMMAND ----------
# Cell 5 – Per-journey loop
# *** CHANGE: use `aligned_df` instead of `df` when filtering per journey ***

job_results = []

for scope_val, journey in SCOPE_JOURNEY_MAP.items():

    # ▼▼▼  ONLY THIS LINE CHANGED  ▼▼▼
    journey_df = aligned_df.filter(F.col("Scope-keyword").isin([scope_val])).withColumn("journey", F.lit(journey))
    # ▲▲▲  was:  df.filter(...)    ▲▲▲

    count = journey_df.count()
    if count == 0:
        print(f"No data for {journey} on {file_date}. Skipping.")
        job_results.append({"journey": journey, "status": "No_data"})
        continue

    _apl_metadata = spark.sql(
        f"SELECT * FROM indx.db.prod_ddi_metadata_db.apl_config_table "
        f"WHERE Pipeline = '$PFIDFE_Ingestion_Daily' AND GroupID = '{journey}'"
    ).collect()

    if not _apl_metadata:
        print(f"No metadata found for {journey}. Skipping.")
        job_results.append({"journey": journey, "status": "No_metadata"})
        continue

    adls_path_base = _apl_metadata[0]["ADLS_Path"]
    adls_raw_path = (
        f"adfs://nw@ddproxyeaannadlsstd.dfs.core.windows.net"
        f"/{adls_path_base}/landing/{batch_id}"
        f"/{file_date}/{journey}/"
    )

    print(f"Writing to: {adls_raw_path}")
    journey_df.write.mode("Overwrite").parquet(adls_raw_path)
    job_results.append({"journey": journey, "status": "SUCCESS", "records": count})
