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
