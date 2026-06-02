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


# ── Count comparison: Databricks vs Elastic source ───────────────────────
# Confirmed counts (full day: May 31 00:00 → 23:59:59):
#   Databricks distinct Transaction_id : 180,348
#   Elastic COUNT_DISTINCT(Transaction_id): 180,593
#   → Databricks is MISSING 245 records from the source

DATABRICKS_DISTINCT = 180_348
ELASTIC_DISTINCT    = 180_593
MISSING             = ELASTIC_DISTINCT - DATABRICKS_DISTINCT   # 245

print(f"Databricks distinct Transaction_ids : {DATABRICKS_DISTINCT}")
print(f"Elastic distinct Transaction_ids    : {ELASTIC_DISTINCT}")
print(f"Records MISSING from Databricks     : {MISSING}")

# ── Find the timestamp range of what IS in Databricks ─────────────────────
# This helps confirm whether the 245 are late-arriving records or lost ones.
print("\n=== Timestamp range in Databricks df ===")
df.select(
    F.min("timestamp").alias("min_timestamp"),
    F.max("timestamp").alias("max_timestamp")
).show(truncate=False)

# ── Investigate WHERE the gap is — which part of the day is thin? ─────────
# Bucket by hour to see if a particular hour is under-ingested
print("\n=== Record count per hour (Databricks) ===")
(
    df.withColumn("hour", F.date_format(F.col("timestamp").cast("timestamp"), "HH"))
      .groupBy("hour")
      .agg(F.count("*").alias("record_count"),
           F.countDistinct("Transaction_id").alias("distinct_txn_ids"))
      .orderBy("hour")
      .show(24, truncate=False)
)

# ── Possible root causes for 245 missing records ──────────────────────────
# 1. Late-arriving events: records written to Elastic after the Databricks
#    ingestion job ran (last few minutes of the day)
# 2. Ingestion job timeout/failure on a micro-batch
# 3. Records filtered out by a transformation in the pipeline
