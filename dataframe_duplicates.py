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
