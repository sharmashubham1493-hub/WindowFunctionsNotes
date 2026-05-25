# Databricks notebook source

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import urllib3

# COMMAND ----------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

spark = SparkSession.builder.appName("APIGEE_Ingestion_Daily").getOrCreate()

# COMMAND ----------

batch_id = int(datetime.now(pytz.timezone("Asia/Kolkata")).strftime('%Y%m%d%H%M%S'))
print(batch_id)

# COMMAND ----------

# Scope value (in Elasticsearch) → GroupID (in metadata table)
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
url     = "https://10.227.12.188:9201/apigee_updated-*/search"
headers = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept":        "application/vnd.elasticsearch+json; compatible-with=8",
}

file_date  = (datetime.now(kolkata_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
all_scopes = list(SCOPE_JOURNEY_MAP.keys())   # ['SWCC','TDCC','PPCC','PBCC','PTCC']
print(f"file_date  : {file_date}")
print(f"all_scopes : {all_scopes}")

# COMMAND ----------
# ── STEP 1 : Single API call for ALL scopes (same as original working query) ──

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
                                "gte": f"{file_date}T00:00:00",
                                "lte": f"{file_date}T23:59:59",
                            }
                        }
                    },
                    {"terms": {"Scope.keyword": all_scopes}},
                ]
            }
        },
    },
    verify=False,
    timeout=120,
)

print(f"HTTP status : {response.status_code}")

if response.status_code != 200:
    raise Exception(f"API call failed: {response.text[:500]}")

# COMMAND ----------
# ── STEP 2 : Parse and flatten ALL hits ──────────────────────────────────────

response_json = response.json()
hits = response_json.get("hits", {}).get("hits", [])
print(f"Total records fetched : {len(hits)}")

if not hits:
    raise Exception(f"No records returned for date={file_date}. Check the date or source data.")

flattened_records = []
for record in hits:
    flat = {}
    for key, value in record.get("fields", {}).items():
        flat[key] = value[0] if isinstance(value, list) and len(value) > 0 else value
    flattened_records.append(flat)

rdd = spark.sparkContext.parallelize([json.dumps(r) for r in flattened_records])
df  = spark.read.options(mergeSchema=True).json(rdd)
df.cache()
print(f"DataFrame : {df.count()} rows  x  {len(df.columns)} columns")

# COMMAND ----------
# ── STEP 3 : Write each journey's slice to its own ADLS raw path ─────────────

job_results = []

for scope_val, journey in SCOPE_JOURNEY_MAP.items():
    print(f"\n{'='*60}")
    print(f"scope={scope_val}  journey={journey}")

    # Filter the already-fetched DataFrame for this scope only
    journey_df = df.filter(col("`Scope.keyword`") == scope_val)
    count = journey_df.count()
    print(f"  Records for this scope : {count}")

    if count == 0:
        print(f"  No data for {journey} on {file_date}. Skipping.")
        job_results.append({"journey": journey, "status": "NO_DATA"})
        continue

    # Resolve ADLS path from metadata config table
    metadata_rows = spark.sql(f"""
        SELECT ADLS_Path
        FROM   sindhu_db_prod.ddi_metadata_db.api_config_table
        WHERE  Pipeline = 'APIGEE_Ingestion_Daily'
        AND    GroupID  = '{journey}'
    """).collect()

    if not metadata_rows:
        print(f"  No metadata row found for {journey}. Skipping.")
        job_results.append({"journey": journey, "status": "NO_METADATA"})
        continue

    adls_path_base = metadata_rows[0]["ADLS_Path"]
    adls_raw_path  = (
        f"abfss://raw@ddiprodvyapaaradlstd.dfs.core.windows.net"
        f"/{adls_path_base}/{batch_id}/"
    )
    print(f"  Writing to : {adls_raw_path}")
    journey_df.write.mode("overwrite").parquet(adls_raw_path)
    print(f"  Write complete.")

    job_results.append({"journey": journey, "status": "SUCCESS", "records": count})

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────────────────────

print("\nAPIGEE ingestion finished\n")
for r in job_results:
    print(f"  {r['status']:<15}  journey={r['journey']}  records={r.get('records','-')}")
