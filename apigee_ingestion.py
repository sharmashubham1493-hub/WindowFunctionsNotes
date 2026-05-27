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

    # 48 intervals of 30 minutes each (instead of 24 hourly)
    for interval in range(48):
        hour = interval // 2
        minute_start = (interval % 2) * 30
        minute_end = minute_start + 29

        start_time = f'{file_date}T{hour:02d}:{minute_start:02d}:00.000'
        end_time   = f'{file_date}T{hour:02d}:{minute_end:02d}:59.999'

        print(f"Fetching interval {interval + 1}/48: {start_time} to {end_time}")

        response = requests.post(
            url,
            headers=headers,
            auth=(user, pwd),
            json={
                "query": {"bool": {"must": [
                    {"range": {"@timestamp": {"gte": start_time, "lte": end_time, "format": "strict_date_hour_minute_second_fraction"}}},
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
                f"API call failed for interval {interval + 1}/48: {response.text[:1000]}"
            )

        hits = response.json().get("hits", {}).get("hits", [])
        all_hits.extend(hits)
        print(f"{interval + 1} interval records: {len(hits)} | running total: {len(all_hits)}")

    current_date += timedelta(days=1)

print(f"All 26 dates fetched successfully. Total records: {len(all_hits)}")
