import requests
import json
import pytz
from datetime import datetime, timedelta
import warnings
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Spark session ────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
spark = SparkSession.builder.appName("APiGEE_Ingestion_Daily").getOrCreate()

# ── Config ───────────────────────────────────────────────────────────────────
user = "apigee_obj"
pwd  = "<<PASSWORD>>"   # replace with actual secret / dbutils.secrets.get(...)

kolkata_tz = pytz.timezone("Asia/Kolkata")

url = "https://10.222.72.188:9200/apigee_updated/_search"

headers = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept":        "application/vnd.elasticsearch+json; compatible-with=8",
}

SCOPE_JOURNEY_MAP = {
    "TDCC": "apigee-update-tdcc-logs",
    "SMCC": "apigee-update-smcc-logs",
    "PRCC": "apigee-update-prcc-logs",
    "PTCC": "apigee-update-ptcc-logs",
    "PBCC": "apigee-update-pbcc-logs",
}

# Elasticsearch hard cap per request — do NOT raise above index max_result_window
PAGE_SIZE = 10_000

# ── Date ─────────────────────────────────────────────────────────────────────
file_date = (datetime.now(kolkata_tz).date() + timedelta(days=-1)).strftime("%Y-%m-%d")
print(f"file_date: {file_date}")

# ── Fetch all records using per-journey + paginated requests ──────────────────
# WHY separate per journey:
#   Querying all scopes together with size=10000 returns at most 10k records
#   total across all journeys — leaving ~20k records unfetched.
#   By querying one journey at a time we stay within the 10k window per scope,
#   and we add from-based pagination so we also handle journeys that themselves
#   exceed 10k records.

all_hits = []

for scope in SCOPE_JOURNEY_MAP:
    from_offset = 0
    journey_hits = []

    while True:
        query_body = {
            "size": PAGE_SIZE,
            "from": from_offset,
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
                        {
                            "term": {
                                "SCOPE_KEYWORD": scope
                            }
                        },
                    ]
                }
            },
        }

        response = requests.post(
            url,
            headers=headers,
            auth=(user, pwd),
            json=query_body,
            verify=False,
            timeout=120,
        )

        if response.status_code != 200:
            raise Exception(
                f"API call failed for scope={scope}, from={from_offset}: "
                f"HTTP {response.status_code} — {response.text}"
            )

        data      = response.json()
        hits      = data["hits"]["hits"]
        total     = data["hits"]["total"]["value"]

        journey_hits.extend(hits)
        print(f"  scope={scope} | fetched {len(hits)} | cumulative {len(journey_hits)} / {total}")

        from_offset += PAGE_SIZE

        # Stop when we've fetched everything, or the page was empty
        if not hits or from_offset >= total:
            break

        # Guard: Elasticsearch max_result_window is typically 10 000.
        # If a journey has >10 000 records, from+size pagination will fail
        # beyond that boundary.  Raise early with a clear message so the fix
        # (switch to Scroll or Search-After API) is obvious.
        if from_offset >= 10_000:
            raise RuntimeError(
                f"scope={scope} has {total} records, which exceeds "
                "Elasticsearch's default max_result_window=10000. "
                "Switch this loop to the Scroll API or Search-After API."
            )

    print(f"scope={scope}: {len(journey_hits)} records fetched (total in index: {total})")
    all_hits.extend(journey_hits)

print(f"\nTotal records fetched across all journeys: {len(all_hits)}")

# ── Convert to Spark DataFrame ────────────────────────────────────────────────
records = [hit["_source"] for hit in all_hits]
df = spark.createDataFrame(records)
print(f"DataFrame row count: {df.count()}")
df.show(5, truncate=False)
