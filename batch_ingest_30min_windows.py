"""
Batch ingest from Elasticsearch in 30-minute time windows.

Instead of fetching all ~10 lakh records at once (which overloads the driver),
this splits the full time range into 30-minute slices. Each slice is:
  fetched  →  flattened  →  written to Delta table

The cluster never holds more than one window's worth of records in memory.
"""

import json
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

# ── Config ──────────────────────────────────────────────────────────────────
ES_URL        = "https://<your-elasticsearch-host>"
ES_INDEX      = "<your-index>"
ES_HEADERS    = {"Content-Type": "application/json"}
ES_AUTH       = ("<user>", "<password>")   # or use API key header

DELTA_TABLE   = "catalog.schema.your_delta_table"
TIMESTAMP_FIELD = "@timestamp"             # field used for time-range filtering
WINDOW_MINUTES  = 30
PAGE_SIZE       = 10_000                   # ES max hits per request

# Full range to ingest — adjust as needed
RANGE_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
RANGE_END   = datetime(2024, 1, 2, tzinfo=timezone.utc)


# ── Helpers ──────────────────────────────────────────────────────────────────

def fetch_window(start: datetime, end: datetime) -> list[dict]:
    """Fetch all hits from ES for [start, end) using search_after pagination."""
    hits = []
    search_after = None

    query_base = {
        "size": PAGE_SIZE,
        "sort": [{TIMESTAMP_FIELD: "asc"}, {"_id": "asc"}],
        "query": {
            "range": {
                TIMESTAMP_FIELD: {
                    "gte": start.isoformat(),
                    "lt":  end.isoformat(),
                }
            }
        },
    }

    while True:
        query = dict(query_base)
        if search_after:
            query["search_after"] = search_after

        resp = requests.post(
            f"{ES_URL}/{ES_INDEX}/_search",
            headers=ES_HEADERS,
            auth=ES_AUTH,
            json=query,
            timeout=120,
        )
        resp.raise_for_status()
        page_hits = resp.json()["hits"]["hits"]
        if not page_hits:
            break
        hits.extend(page_hits)
        search_after = page_hits[-1]["sort"]   # cursor for next page

    return hits


def flatten_hits(hits: list[dict]) -> list[dict]:
    """Flatten the 'fields' dict: unwrap single-element lists."""
    records = []
    for record in hits:
        flat = {}
        for key, value in record.get("fields", {}).items():
            flat[key] = value[0] if isinstance(value, list) and len(value) > 0 else value
        records.append(flat)
    return records


# ── Main loop ────────────────────────────────────────────────────────────────

window_start = RANGE_START
window_delta = timedelta(minutes=WINDOW_MINUTES)
total_written = 0

while window_start < RANGE_END:
    window_end = min(window_start + window_delta, RANGE_END)

    print(f"Processing window: {window_start.isoformat()} → {window_end.isoformat()}")

    hits     = fetch_window(window_start, window_end)
    records  = flatten_hits(hits)

    if records:
        df = spark.createDataFrame(records)
        (
            df.write
              .format("delta")
              .mode("append")             # append each window to the Delta table
              .option("mergeSchema", "true")
              .saveAsTable(DELTA_TABLE)
        )
        total_written += len(records)
        print(f"  → wrote {len(records):,} records  (running total: {total_written:,})")
    else:
        print("  → no records in this window, skipping")

    # release references so GC can reclaim memory before next window
    del hits, records

    window_start = window_end

print(f"\nDone. Total records written: {total_written:,}")
