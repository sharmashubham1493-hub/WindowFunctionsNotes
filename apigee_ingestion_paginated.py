import requests
import pytz
from datetime import datetime, timedelta
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Spark session ────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
spark = SparkSession.builder.appName("APiGEE_Ingestion_Daily").getOrCreate()

# ── Config ───────────────────────────────────────────────────────────────────
user = "apigee_obj"
pwd  = "<<PASSWORD>>"   # replace with dbutils.secrets.get(...)

kolkata_tz = pytz.timezone("Asia/Kolkata")

SEARCH_URL = "https://10.222.72.188:9200/apigee_updated/_search"
SCROLL_URL = "https://10.222.72.188:9200/_search/scroll"
CLEAR_URL  = "https://10.222.72.188:9200/_search/scroll"

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

# ── Date ─────────────────────────────────────────────────────────────────────
file_date = (datetime.now(kolkata_tz).date() + timedelta(days=-1)).strftime("%Y-%m-%d")
print(f"file_date: {file_date}")

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
print(f"all_scopes: {all_scopes}")

# ── Fetch ALL records for all 5 journeys in one scroll loop ──────────────────
# The Scroll API works like a cursor: the first request opens a scroll context
# and returns a scroll_id; subsequent requests pass that scroll_id to get the
# next page — no max_result_window limit applies.  We stop when a page is empty.

PAGE_SIZE   = 10_000   # records per page (max Elasticsearch will return at once)
SCROLL_TTL  = "2m"     # how long the server keeps the scroll context alive

# ── Step 1: open scroll with the initial search ──────────────────────────────
initial_response = requests.post(
    SEARCH_URL,
    params={"scroll": SCROLL_TTL},
    headers=headers,
    auth=(user, pwd),
    json={
        "size": PAGE_SIZE,
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
                        "terms": {
                            "SCOPE_KEYWORD": all_scopes   # all 5 journeys in one query
                        }
                    },
                ]
            }
        },
    },
    verify=False,
    timeout=120,
)

if initial_response.status_code != 200:
    raise Exception(f"Initial scroll request failed: HTTP {initial_response.status_code} — {initial_response.text}")

data      = initial_response.json()
scroll_id = data["_scroll_id"]
total     = data["hits"]["total"]["value"]
all_hits  = data["hits"]["hits"]

print(f"Total records in index for {file_date}: {total}")
print(f"Page 1: fetched {len(all_hits)} records")

# ── Step 2: keep scrolling until no more hits ────────────────────────────────
page = 2
while True:
    scroll_response = requests.post(
        SCROLL_URL,
        headers=headers,
        auth=(user, pwd),
        json={"scroll": SCROLL_TTL, "scroll_id": scroll_id},
        verify=False,
        timeout=120,
    )

    if scroll_response.status_code != 200:
        raise Exception(f"Scroll page {page} failed: HTTP {scroll_response.status_code} — {scroll_response.text}")

    scroll_data = scroll_response.json()
    scroll_id   = scroll_data["_scroll_id"]   # server may rotate the ID
    hits        = scroll_data["hits"]["hits"]

    if not hits:
        print(f"No more records. Scroll complete.")
        break

    all_hits.extend(hits)
    print(f"Page {page}: fetched {len(hits)} | cumulative {len(all_hits)} / {total}")
    page += 1

# ── Step 3: clean up the scroll context on the server ────────────────────────
requests.delete(
    CLEAR_URL,
    headers=headers,
    auth=(user, pwd),
    json={"scroll_id": scroll_id},
    verify=False,
    timeout=30,
)

print(f"\nTotal records fetched: {len(all_hits)} (expected: {total})")

# ── Convert to Spark DataFrame ────────────────────────────────────────────────
records = [hit["_source"] for hit in all_hits]
df = spark.createDataFrame(records)
print(f"DataFrame row count: {df.count()}")
df.show(5, truncate=False)
