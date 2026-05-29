# Apigee Ingestion Record Count Mismatch — Root Cause Analysis

**Observed**: Source (Elasticsearch) shows **155,737** records for journey SWCC on 2026-05-26.  
**Actual after ingestion**: Only **3,603** records fetched by the Databricks notebook.

---

## Root Causes

### 1. Wrong value passed to the `Scope.keyword` filter

The notebook defines:
```python
SCOPE_JOURNEY_MAP = {
    'SWCC': 'apigee-update-swcc-logs',
    'TDCC': 'apigee-update-tdcc-logs',
    ...
}
scope = ['SWCC']
```

If `scope_url` is built from the **map values** (log topic paths like `apigee-update-swcc-logs`), the Elasticsearch filter becomes:
```json
{"terms": {"Scope.keyword": ["apigee-update-swcc-logs"]}}
```
But the actual `Scope` field in Elasticsearch stores the **code** (`SWCC`), not the log path.  
This mismatch causes ~98% of records to be silently excluded — explaining why only ~150 records/hour come through instead of ~6,500.

**Fix**: Pass scope codes directly to the filter:
```python
# Wrong
scope_url = [SCOPE_JOURNEY_MAP[s] for s in scope]   # ['apigee-update-swcc-logs']

# Correct
scope_url = scope                                    # ['SWCC']
```

---

### 2. `"size": 10000` per interval with no pagination

Each hourly query is capped at 10,000 documents. There is no `search_after` or scroll loop to retrieve records beyond this limit. Any hour with more than 10,000 records is **silently truncated**.

Maximum possible records with current code: 24 hours × 10,000 = **240,000**.  
In practice, with the wrong scope filter, only ~150/hour are returned.

**Fix**: Add a `search_after` pagination loop inside each interval:
```python
all_hits_1 = []
for interval in range(24):
    start_time = f"{file_date}T{interval:02d}:00:00.000"
    end_time   = f"{file_date}T{interval:02d}:59:59.999"
    last_sort  = None
    while True:
        body = {
            "size": 10000,
            "sort": [{"@timestamp": "asc"}, {"_id": "asc"}],
            "query": {"bool": {"must": [
                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                {"terms": {"Scope.keyword": scope}}
            ]}}
        }
        if last_sort:
            body["search_after"] = last_sort
        resp = requests.post(url_1, headers=headers, auth=(user, psd),
                             json=body, verify=False, timeout=120)
        hits = resp.json()["hits"]["hits"]
        if not hits:
            break
        all_hits_1.extend(hits)
        last_sort = hits[-1]["sort"]
```

---

### 3. Index name mismatch

| Context | Index used |
|---|---|
| Elasticsearch source query (UI) | `apigee_updated` |
| Notebook URL_1 | `apigee-updated+` (wildcard, dashes) |
| Notebook URL_2 | `apigee_updated_solace` |

The `+` wildcard suffix and the dash-vs-underscore difference mean URL_1 may query a different or overlapping set of indices compared to what the source count was run against.

**Fix**: Confirm the exact index name by running `GET /_cat/indices/apigee*` against both ES endpoints and align the notebook URL to the same index the source query targets.

---

### 4. Source metric is `COUNT_DISTINCT` — not raw record count

The source Elasticsearch query is:
```sql
FROM apigee_updated
WHERE Scope == 'SWCC'
STATS distinct_transaction_count = COUNT_DISTINCT(Transaction_Id)
```
Result: **155,737**

This counts **unique Transaction_Ids**, not raw log records. The notebook fetches raw hits. If one transaction generates multiple log entries, these numbers are not directly comparable. To do a fair comparison, run:
```sql
-- In ES: count raw records
FROM apigee_updated
WHERE Scope == 'SWCC'
STATS raw_count = COUNT(*)

-- In Databricks: count distinct transactions
len(set(h["_source"]["Transaction_Id"] for h in all_hits_1))
```

---

## Priority Order for Fixes

1. **Fix the `Scope.keyword` filter value** — most likely cause of the 43× undercount
2. **Add `search_after` pagination** — prevents silent truncation on high-volume hours
3. **Verify index names match** between source and ingestion endpoints
4. **Align comparison metric** (raw count vs. distinct transaction count)
