import requests
import pytz
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# ─── CONFIG ───────────────────────────────────────────────────────────────────

kolkata_tz      = pytz.timezone("Asia/Kolkata")
url             = "https://10.227.12.188:9201/apigee_updated_solace/_search"
headers         = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept":       "application/vnd.elasticsearch+json; compatible-with=8",
}

interval_minute = 30                                  # changed from 10 → 48 intervals/day
total_intervals = (24 * 60) // interval_minute        # 48
CHUNK_SIZE      = 2000                                # records per ES request
BATCH_SIZE      = 50000                               # write to ADLS every 50K records
file_date       = (datetime.now(kolkata_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
batch_id        = datetime.now(kolkata_tz).strftime("%Y%m%d%H%M%S")
adls_path_base  = "abfss://rawdatabricksproxyapiealistd.dfs.core.windows.net"

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
print(f"all_scopes : {all_scopes}")
print(f"file_date  : {file_date}")
print(f"batch_id   : {batch_id}")
print(f"intervals  : {total_intervals} × {interval_minute} min")

# ─── FIELDS TO FETCH FROM ES ──────────────────────────────────────────────────
# Explicitly list only required fields — avoids fetching all 69 columns every request

FIELDS_NEEDED = [
    "@timestamp",
    "APIProxyType",
    "APIProxyType.keyword",
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
    "RequestContentType",
    "RequestContentType.keyword",
    "RequestFieldLogs",
    "ResponseFieldLogs",
    "ResponseFieldLogs.keyword",
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
    "client.received.start.timestamp",
    "client.sent.end.timestamp",
    "elkGatewayErrorMessage",
    "elkGatewayErrorMessage.keyword",
    "elkRoutingStatusCode",
    "elkRoutingStatusCode.keyword",
    "log_timestamp",
    "req_journeyID",
    "req_journeyID.keyword",
    "requestcontentlength",
    "response",
    "response.keyword",
    "response_time",
    "target.received.end.timestamp",
    "target.sent.start.timestamp",
]

# ─── SESSION — reuses TCP connection across all requests ──────────────────────

session = requests.Session()
session.auth = (user, psd)
session.headers.update(headers)
session.verify = False

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def flatten_hits(hits):
    """Convert ES hits list into flat dict records."""
    records = []
    for record in hits:
        flat = {}
        for key, value in record["fields"].items():
            flat[key] = value[0] if isinstance(value, list) and len(value) > 0 else value
        records.append(flat)
    return records


def write_batch_to_adls(records, journey, adls_raw_path, batch_num):
    """Write a batch of records to ADLS as parquet."""
    df = spark.createDataFrame(records)
    mode = "overwrite" if batch_num == 1 else "append"
    df.write.mode(mode).parquet(adls_raw_path)
    print(f"    Batch {batch_num} written → {len(records)} records → {adls_raw_path}")


def fetch_all_for_scope(scope_filter):
    """
    Fetch all records for given scope(s) across the full day using:
      - 30-min intervals
      - search_after pagination within each interval
      - CHUNK_SIZE = 2000 per ES request
    Returns a flat list of record dicts.
    """
    all_hits = []

    for i in range(total_intervals):
        start_total_min = i * interval_minute
        end_total_min   = start_total_min + interval_minute - 1

        start_time = (
            f"{file_date}T"
            f"{start_total_min // 60:02d}:{start_total_min % 60:02d}:00.000"
        )
        end_time = (
            f"{file_date}T"
            f"{end_total_min // 60:02d}:{end_total_min % 60:02d}:59.999"
        )

        print(f"  Interval ({i+1}/{total_intervals}): {start_time} → {end_time}")

        search_after  = None
        interval_hits = []
        page          = 0

        while True:
            query = {
                "size":    CHUNK_SIZE,
                "fields":  FIELDS_NEEDED,
                "_source": False,                     # don't return raw _source, saves bandwidth
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                            {"terms": {"Scope.keyword": scope_filter}},
                        ]
                    }
                },
                "sort": [
                    {"@timestamp": "asc"},
                    {"_id": "asc"},                   # _id as tiebreaker — never loads fielddata
                ],
            }

            if search_after:
                query["search_after"] = search_after

            response = session.post(url, json=query, timeout=120)

            if response.status_code != 200:
                raise Exception(
                    f"API failed [{response.status_code}] "
                    f"interval {start_time}→{end_time}: {response.text[:150]}"
                )

            hits = response.json().get("hits", {}).get("hits", [])

            if not hits:
                break

            interval_hits.extend(hits)
            page        += 1
            search_after = hits[-1]["sort"]

            print(f"    page {page}: +{len(hits)} | interval total: {len(interval_hits)}")

            if len(hits) < CHUNK_SIZE:
                break                                 # last page

        all_hits.extend(interval_hits)
        print(f"  Interval {i+1} done: {len(interval_hits)} | Running total: {len(all_hits)}\n")

    return all_hits

# ─── MAIN LOOP — per journey ──────────────────────────────────────────────────

job_results = []

for journey in SCOPE_JOURNEY_MAP:
    print(f"\n{'='*60}")
    print(f"Processing journey: {journey}")
    print(f"{'='*60}")

    groupID      = f"{journey}"
    api_metadata = [row.asDict() for row in metadata_df.filter(
        F.col("journey") == journey
    ).collect()]

    if not api_metadata:
        print(f"No metadata found for {journey}. Skipping.")
        job_results.append({"journey": journey, "status": "No metadata"})
        continue

    adls_raw_path = f"{adls_path_base}/landing/{batch_id}/{journey}/"
    scope_filter  = [journey]

    print(f"ADLS path : {adls_raw_path}")
    print(f"Fetching  : {total_intervals} intervals × {interval_minute} min")

    # ── fetch all records for this journey ──
    raw_hits = fetch_all_for_scope(scope_filter)

    if not raw_hits:
        print(f"No records found for {journey}.")
        job_results.append({"journey": journey, "status": "No records", "records": 0})
        continue

    # ── batch write to ADLS ──
    flat_records = flatten_hits(raw_hits)
    total        = len(flat_records)
    batch_num    = 0

    print(f"\nWriting {total} records in batches of {BATCH_SIZE}...")

    for start in range(0, total, BATCH_SIZE):
        batch       = flat_records[start: start + BATCH_SIZE]
        batch_num  += 1
        write_batch_to_adls(batch, journey, adls_raw_path, batch_num)

    print(f"Write complete for {journey}. Total records: {total}")
    job_results.append({"journey": journey, "status": "SUCCESS", "records": total})

# ─── SUMMARY ──────────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
print("INGESTION SUMMARY")
print(f"{'='*60}")
for result in job_results:
    status  = result["status"]
    records = result.get("records", "-")
    print(f"  {result['journey']:<10} | {status:<12} | records: {records}")
