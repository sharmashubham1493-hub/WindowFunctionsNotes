# ── How many duplicate records are in df? ──────────────────────────────────

# 1. Total rows vs distinct rows
total_count    = df.count()
distinct_count = df.distinct().count()
duplicate_count = total_count - distinct_count

print(f"Total records   : {total_count}")
print(f"Distinct records: {distinct_count}")
print(f"Duplicate records: {duplicate_count}")

# ── 2. See which rows are duplicated and how many times ────────────────────
from pyspark.sql import functions as F

duplicates_df = (
    df.groupBy(df.columns)          # group by ALL columns
      .agg(F.count("*").alias("occurrence_count"))
      .filter(F.col("occurrence_count") > 1)   # keep only duplicated rows
      .orderBy(F.col("occurrence_count").desc())
)

print(f"\nRows that appear more than once: {duplicates_df.count()}")
display(duplicates_df)

# ── 3. (Optional) Drop duplicates and keep only unique rows ────────────────
# df_clean = df.dropDuplicates()
# print(f"After deduplication: {df_clean.count()} rows")


# ── Distinct & duplicate count based on transaction_id ────────────────────

total_count                = df.count()
distinct_transaction_count = df.select("transaction_id").distinct().count()
duplicate_transaction_count = total_count - distinct_transaction_count

print(f"Total records              : {total_count}")
print(f"Distinct transaction_ids   : {distinct_transaction_count}")
print(f"Duplicate txn_id records   : {duplicate_transaction_count}")

# Which transaction_ids appear more than once, and how many times?
dup_txn_df = (
    df.groupBy("transaction_id")
      .agg(F.count("*").alias("occurrence_count"))
      .filter(F.col("occurrence_count") > 1)
      .orderBy(F.col("occurrence_count").desc())
)

print(f"\nTransaction IDs seen more than once: {dup_txn_df.count()}")
display(dup_txn_df)

# Full rows for those duplicate transaction_ids
dup_txn_ids = dup_txn_df.select("transaction_id")
display(df.join(dup_txn_ids, on="transaction_id", how="inner").orderBy("transaction_id"))


# ── Investigate count mismatch vs source (Elastic = 177,195) ─────────────
# Databricks shows 180,348 vs Elastic 177,195 → 3,153 extra records
# Root cause 1: time range mismatch — Elastic query ends at 23:30, not midnight

# Check the actual time range in df
print("=== Time range in Databricks df ===")
df.select(
    F.min("timestamp").alias("min_timestamp"),
    F.max("timestamp").alias("max_timestamp")
).show(truncate=False)

# Count records WITHIN the same window Elastic used (00:00 → 23:30)
ELASTIC_START = "2026-05-31T00:00:00"
ELASTIC_END   = "2026-05-31T23:30:00"

df_elastic_window = df.filter(
    (F.col("timestamp") >= ELASTIC_START) &
    (F.col("timestamp") <= ELASTIC_END)
)

elastic_window_distinct = df_elastic_window.select("transaction_id").distinct().count()
print(f"\nDistinct transaction_ids in Elastic window ({ELASTIC_START} → {ELASTIC_END}): {elastic_window_distinct}")
ELASTIC_DISTINCT = 177195
DATABRICKS_TOTAL = 180348
print(f"Elastic portal shows : {ELASTIC_DISTINCT}")
print(f"Databricks total     : {DATABRICKS_TOTAL}")
print(f"Gap                  : {DATABRICKS_TOTAL - ELASTIC_DISTINCT}")   # 3,153
print(f"Remaining gap after window filter: {elastic_window_distinct - ELASTIC_DISTINCT}")

# Root cause 2: records ingested more than once (same txn_id, different ingest time)
# Check if timestamp column is the event time or ingest time
# If there's a separate ingest/processing timestamp column, compare it
print("\n=== Sample columns to check for ingest timestamp ===")
print([c for c in df.columns if any(k in c.lower() for k in ["ingest", "load", "insert", "process", "created", "received"])])
