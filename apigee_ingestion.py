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

# Date range: 1 May 2026 to 26 May 2026
start_date = datetime(2026, 5, 1).date()
end_date = datetime(2026, 5, 26).date()

all_hits = []

current_date = start_date
while current_date <= end_date:
    file_date = current_date.strftime("%Y-%m-%d")
    print(file_date)

    # 144 intervals of 10 minutes each
    interval_minute = 10
    total_intervals = (24 * 60) // interval_minute

    for interval in range(total_intervals):
        start_total_min = interval * interval_minute
        end_total_min   = start_total_min + interval_minute - 1

        start_hour   = start_total_min // 60
        start_minute = start_total_min % 60
        end_hour     = end_total_min // 60
        end_minute   = end_total_min % 60

        # Use UTC so epoch_millis matches how ES interprets plain date strings
        # (no timezone = UTC). pytz.utc is already available from the import above.
        start_dt = datetime(current_date.year, current_date.month, current_date.day,
                            start_hour, start_minute, 0, tzinfo=pytz.utc)
        end_dt   = datetime(current_date.year, current_date.month, current_date.day,
                            end_hour, end_minute, 59, tzinfo=pytz.utc)

        start_epoch = int(start_dt.timestamp() * 1000)
        end_epoch   = int(end_dt.timestamp() * 1000)

        # Human-readable label for logging only
        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
        end_time   = end_dt.strftime('%Y-%m-%dT%H:%M:%S')

        print(f"Fetching interval {interval + 1}/{total_intervals}: {start_time} to {end_time}")

        response = requests.post(
            url,
            headers=headers,
            auth=(user, pwd),
            json={
                "query": {"bool": {"must": [
                    {"range": {"@timestamp": {"gte": start_epoch, "lte": end_epoch, "format": "epoch_millis"}}},
                    {"terms": {"Scope Keyword": all_scopes}},
                ]}},
                "fields": ["*"],
                "size": 10000,
                "timeout": "120s",
            },
            verify=False,
            timeout=120,
        )

        if response.status_code != 200:
            raise Exception(
                f"API call failed for interval {interval + 1}/{total_intervals}: {response.text[:1000]}"
            )

        hits = response.json().get("hits", {}).get("hits", [])
        all_hits.extend(hits)
        print(f"{interval + 1}/{total_intervals} interval records: {len(hits)} | running total: {len(all_hits)}")

    current_date += timedelta(days=1)

print(f"All 26 dates fetched successfully. Total records: {len(all_hits)}")
