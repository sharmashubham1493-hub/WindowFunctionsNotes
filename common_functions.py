import re
from datetime import datetime
from pyspark.sql.functions import lit


def check_missing_columns(df, col_names, allow_missing_columns, is_mandatory_column):
    """
    Checks if dataframe is missing any columns and whether missing ones can be replaced with Null.

    Parameters:
    - df: PySpark DataFrame to validate
    - col_names: list of column names required in the dataframe
    - allow_missing_columns: Boolean; if True, non-mandatory missing columns get a Null column added
    - is_mandatory_column: list of column names that are mandatory (cannot be missing)

    Returns:
    - PySpark DataFrame with exactly col_names columns

    Raises:
    - ValueError if any mandatory column is missing, or if allow_missing_columns is False and
      any column is missing
    """
    missing_cols = [col for col in col_names if col not in df.columns]

    if len(missing_cols) == 0:
        return df.select(col_names)

    if allow_missing_columns:
        missing_mandatory_columns = [col for col in missing_cols if col in is_mandatory_column]
        if missing_mandatory_columns:
            raise ValueError(
                f"Following mandatory columns are missing in the dataframe: {missing_mandatory_columns}"
            )
        for col in missing_cols:
            df = df.withColumn(col, lit(None))
        return df.select(col_names)
    else:
        raise ValueError(
            f"Following columns are missing in the dataframe: {missing_cols}"
        )


# ── Cell 16: Check for Missing Columns ───────────────────────────────────────
# Bug fixes applied:
#   1. Regex updated to match BOTH error formats:
#      "Following columns are missing..."         (allow_missing_columns=False)
#      "Following mandatory columns are missing..." (allow_missing_columns=True)
#   2. Typo fixed: "coulmes" → "columns"
#   3. bare `raise` preserves the original stack trace instead of `raise e`

try:
    col_checked_df = check_missing_columns(
        transformed_df, col_names, allow_missing_columns, is_mandatory_column
    )
except Exception:
    schema_end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    import sys
    error_text = str(sys.exc_info()[1])

    # matches both:  "Following columns are missing..."
    #                "Following mandatory columns are missing..."
    match = re.search(
        r"Following (?:mandatory )?columns are missing in the dataframe: \[(.*?)\]",
        error_text,
    )

    if match:
        raw_cols = match.group(1)
        missing_cols_list = [c.strip().strip("'") for c in raw_cols.split(",")]
        missing_col = ", ".join(missing_cols_list)
        custom_error = f"The following columns are missing in the dataframe: {missing_col}"
    else:
        custom_error = "Unknown column error"

    spark.sql(
        f"UPDATE ddi_metadata.db_job_details "
        f"SET schema_end_time = '{schema_end_timestamp}', "
        f"schema_val_status = 'Failed', "
        f"process_stage = 'Schema Validation', "
        f"error_message = '{custom_error}' "
        f"WHERE job_id = {job_id}"
    )
    raise  # bare raise — preserves original traceback
