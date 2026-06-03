# Databricks notebook source
import requests
import pytz
from datetime import datetime, timedelta

kolkata_tz = pytz.timezone("Asia/Kolkata")

url = "https://10.227.12.188:9201/apigee_inferred/search"

headers = {
    "Content-Type": "application/json; compatible-with=8",
    "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
}

# COMMAND ----------

all_scopes = list(SCOPE_JOURNEY_MAP.keys())
# scope = ['TDCC', 'GMCC', 'PRCC', 'PTCC']
# scope = ['GMCC']
print("all_scopes:", (all_scopes))

DELTA_TABLE  = "your_catalog.your_schema.apigee_raw"   # <-- update before running
interval_minute  = 30
total_intervals  = (24 * 60) // interval_minute        # 48 windows per day

# Date range: 1 May 2026 to 26 May 2026
start_date = datetime(2026, 5, 1).date()
end_date   = datetime(2026, 5, 26).date()

grand_total = 0

current_date = start_date
while current_date <= end_date:
    file_date = current_date.strftime("%Y-%m-%d")
    print(f"\n=== {file_date} ===")

    for interval in range(total_intervals):
        start_total_min = interval * interval_minute
        end_total_min   = start_total_min + interval_minute - 1

        start_hour   = start_total_min // 60
        start_minute = start_total_min % 60
        end_hour     = end_total_min   // 60
        end_minute   = end_total_min   % 60

        start_dt = datetime(current_date.year, current_date.month, current_date.day,
                            start_hour, start_minute, 0, tzinfo=pytz.utc)
        end_dt   = datetime(current_date.year, current_date.month, current_date.day,
                            end_hour,   end_minute,   59, tzinfo=pytz.utc)

        start_epoch = int(start_dt.timestamp() * 1000)
        end_epoch   = int(end_dt.timestamp()   * 1000)

        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
        end_time   = end_dt.strftime('%Y-%m-%dT%H:%M:%S')

        print(f"  Interval {interval + 1}/{total_intervals}: {start_time} to {end_time}")

        # --- pagination via search_after (no 10k ES limit) ---
        interval_hits = []
        last_sort     = None
        page          = 1

        while True:
            body = {
                "query": {"bool": {"must": [
                    {"range": {"@timestamp": {"gte": start_epoch, "lte": end_epoch, "format": "epoch_millis"}}},
                    {"terms": {"Scope Keyword": all_scopes}},
                ]}},
                "fields": ["*"],
                "size": 10000,
                "sort": [{"@timestamp": "asc"}, {"_id": "asc"}],
                "timeout": "120s",
            }
            if last_sort:
                body["search_after"] = last_sort

            response = requests.post(
                url,
                headers=headers,
                auth=(user, pwd),
                json=body,
                verify=False,
                timeout=120,
            )

            if response.status_code != 200:
                raise Exception(
                    f"API call failed — interval {interval + 1}/{total_intervals} "
                    f"page {page}: {response.text[:1000]}"
                )

            resp_json  = response.json()
            total_in_es = resp_json.get("hits", {}).get("total", {}).get("value", "?")
            page_hits   = resp_json.get("hits", {}).get("hits", [])

            if not page_hits:
                break

            interval_hits.extend(page_hits)
            last_sort = page_hits[-1]["sort"]
            print(f"    page {page} | fetched {len(page_hits)} | ES total: {total_in_es} | interval running: {len(interval_hits)}")
            page += 1

        # --- write this interval's batch to Delta immediately (avoid OOM) ---
        if interval_hits:
            rows = [hit.get("fields", hit.get("_source", {})) for hit in interval_hits]
            df   = spark.createDataFrame(rows)
            df.write.format("delta").mode("append").saveAsTable(DELTA_TABLE)

        grand_total += len(interval_hits)
        print(f"  Interval {interval + 1}/{total_intervals} done — {len(interval_hits)} records written | grand total so far: {grand_total}")

    current_date += timedelta(days=1)

print(f"\nAll 26 dates fetched and written to Delta. Grand total records: {grand_total}")
