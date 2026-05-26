# Databricks notebook source

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import urllib3
from pytz import timezone

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

# MASTER_COLUMNS must be defined HERE (before the ES query) so we can pass
# it as "_source" to Elasticsearch — this is the biggest performance fix:
# ES will return ONLY these 61 fields per doc instead of the full _source
# (which includes large request/response body text and other heavy fields).
# *** Verify these names match exactly what is stored in your ES index. ***

MASTER_COLUMNS = [
    "@timestamp",
    "@ProxyType",
    "Apigee-developer-app.name.keyword",
    "Apigee-product-name.keyword",
    "BackendContentType.keyword",
    "BackendErrorMessage",
    "BackendErrorMessage.keyword",
    "BackendFrontMessage.keyword",
    "ClientIP",
    "Criticality",
    "Criticality.keyword",
    "Environment.name",
    "GatewayErrorCode",
    "GatewayErrorCode.keyword",
    "GatewayErrorMessage",
    "GatewayErrorMessage.keyword",
    "GatewayFrontMessage",
    "GatewayFrontMessage.keyword",
    "host",
    "host.keyword",
    "Mock",
    "Mock.keyword",
    "platform",
    "Platform.keyword",
    "Proxy.basepath",
    "Proxy.basepath.keyword",
    "Proxy.pathsuffix",
    "Proxy.pathsuffix.keyword",
    "RequestContentType.keyword",
    "RequestFilingLogs",
    "RoutingStatusCode",
    "RoutingStatusCode.keyword",
    "Scope",
    "scope",
    "ServiceVirtualization.keyword",
    "Transaction_id",
    "apigee-index.keyword",
    "appgronymname.keyword",
    "micronym",
    "client.received.start.timestamp",
    "client.sent.end.timestamp",
    "target.received.end.timestamp",
    "target.sent.start.timestamp",
    "req-journeyID",
    "req-journeyID.keyword",
    "req-responseCode",
    "req-responseCode.keyword",
    "response",
    "response.keyword",
    "RequestStatusCode",
    "RequestStatusCode.keyword",
    "BackendStatusCode",
    "BackendStatusCode.keyword",
    "RoutingStatus",
    "RoutingStatus.keyword",
    "Environment",
    "Environment.keyword",
    "apiproxymame",
    "apiproxymame.keyword",
    "ClientID",
    "ClientID.keyword",
    "apiproduct",
]
# ^^^ If your notebook already has a MASTER_COLUMNS list, replace the above
#     with that exact list and delete any duplicates.

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
# Performance fixes (in order of impact):
#
#   1. "_source": MASTER_COLUMNS  <-- BIGGEST FIX for the 1-hour timeout
#      API gateway logs store full request/response bodies in _source.
#      30k docs x ~50KB body = ~1.5 GB of data we were fetching but
#      never using.  Now ES returns only the 61 columns we actually need.
#
#   2. "_shard_doc" tiebreaker (was "_id")
#      _id sort loads field-data for every doc — very expensive.
#      _shard_doc is a zero-cost built-in tiebreaker (ES 7.12+).
#
#   3. "filter" context (was "must")
#      Skips relevance scoring + enables shard-level query caching.
#
#   4. "track_total_hits": false
#      Stops ES counting every matching doc on each page request.
# ---------------------------------------------------------------------------

PAGE_SIZE    = 10000
all_hits     = []
search_after = None

while True:
    query_body = {
        "size": PAGE_SIZE,
        "track_total_hits": False,
        "_source": MASTER_COLUMNS,          # <-- only fetch the 61 needed fields
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

# COMMAND ----------

# Align DataFrame to MASTER_COLUMNS — fill any missing cols with null
existing_cols = set(df.columns)
df = df.select([
    F.col(c) if c in existing_cols else F.lit(None).cast(StringType()).alias(c)
    for c in MASTER_COLUMNS
])

print(f"Source columns : {len(df.columns)}")
print(f"Missing columns: {[c for c in MASTER_COLUMNS if c not in existing_cols]}")
display(df)
