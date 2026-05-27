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

    # 24 intervals of 1 hour each
    for interval in range(24):
        hour = interval

        # Build datetime objects in IST and convert to epoch milliseconds
        # to avoid Elasticsearch date-format parse errors
        start_dt = datetime(current_date.year, current_date.month, current_date.day,
                            hour, 0, 0, tzinfo=kolkata_tz)
        end_dt   = datetime(current_date.year, current_date.month, current_date.day,
                            hour, 59, 59, tzinfo=kolkata_tz)

        start_epoch = int(start_dt.timestamp() * 1000)
        end_epoch   = int(end_dt.timestamp() * 1000)

        # Human-readable label for logging only
        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
        end_time   = end_dt.strftime('%Y-%m-%dT%H:%M:%S')

        print(f"Fetching interval {interval + 1}/24: {start_time} to {end_time}")

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
                f"API call failed for interval {interval + 1}/24: {response.text[:1000]}"
            )

        hits = response.json().get("hits", {}).get("hits", [])
        all_hits.extend(hits)
        print(f"{interval + 1} interval records: {len(hits)} | running total: {len(all_hits)}")

    current_date += timedelta(days=1)

print(f"All 26 dates fetched successfully. Total records: {len(all_hits)}")
