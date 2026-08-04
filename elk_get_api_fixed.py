# =============================================================================
# Corrected ELK -> raw-layer write logic for Flutter_ELK_Ingestion_Daily_new
# -----------------------------------------------------------------------------
# Problem this fixes:
#   Records are visible in Elasticsearch / Kibana for the "special date", but the
#   raw layer (ADLS) shows no data when you browse by date.
#
# Root causes addressed here:
#   1. The raw path was keyed only by {batch_id} (a run timestamp), so the raw
#      layer was NOT organised by date. The date lived only as a *column*
#      (file_date) inside the parquet, so any date-folder lookup came up empty.
#      -> We now partition the output by file_date, producing real
#         .../file_date=YYYY-MM-DD/ folders you can browse and filter on.
#
#   2. `df.repartition(1)` was called but its result was thrown away, so the
#      "single file" intent never took effect.
#      -> We now reassign: df = df.repartition(1).
#
#   3. When the CSV report download returned only the header row, the code
#      silently skipped the write ("only headers found ..."). Because Kibana
#      _search still shows the rows, this looks like "data exists but raw layer
#      is empty". This is the reporting/CSV endpoint returning an empty job, not
#      Elasticsearch. It is now surfaced loudly and the function reports back
#      what happened so the caller can retry / alert instead of moving on blind.
#
# NOTE: This was reconstructed from screenshots of the Databricks notebook.
#       The original notebook is not in this repo. Paste this back into the
#       notebook (or import it) and adjust names to match your cell if needed.
# =============================================================================

import time
from io import StringIO

import pandas as pd
from pyspark.sql.functions import col, lit


def elk_get_api(
    report_id,
    journey_name,
    base_url,
    headers,
    auth,
    verify,
    adls_raw_path,
    file_date,
    start_time_str,
    end_time_str,
    max_attempts: int = 100,
):
    """Download a generated ELK CSV report and land it in the raw layer.

    Returns a short status string so the caller can react:
        "written"      -> rows were written to the raw layer
        "empty_report" -> report came back with only a header row (no data
                          landed even though Kibana may show records)
        "failed"       -> gave up after max_attempts (transient 5xx / errors)
    """
    download_url = f"{base_url}api/reporting/jobs/download/{report_id}"

    for attempt in range(max_attempts):
        try:
            elk_get_response = requests.get(  # noqa: F821 (requests imported in notebook)
                url=download_url,
                headers=headers,
                auth=auth,
                verify=verify,
            )

            if elk_get_response.status_code == 200:
                data = elk_get_response.text
                lines = data.splitlines()

                # ---- Guard: report download returned headers only -----------
                # This is the #1 reason for "visible in Kibana, empty in raw".
                # The rows come from Elasticsearch _search, but the raw layer is
                # fed by the CSV *report* job, which can be empty while _search
                # is not. Surface it clearly instead of skipping silently.
                if len(lines) <= 1:
                    print(
                        f"[WARN] Empty report for journey='{journey_name}' "
                        f"report_id={report_id} window=[{start_time_str} -> {end_time_str}] "
                        f"file_date={file_date}: CSV download returned header only "
                        f"({len(lines)} line(s)). Nothing written to the raw layer. "
                        f"Check the report job's saved-search / time filter — the "
                        f"data may still exist in Elasticsearch."
                    )
                    return "empty_report"

                # ---- Build the Spark DataFrame ------------------------------
                pandas_df = pd.read_csv(StringIO(data), low_memory=False)
                df = spark.createDataFrame(pandas_df)  # noqa: F821 (spark from notebook)

                # Cast every column to string (handles column names with dots).
                column_names = df.columns
                df = df.select(
                    [col(f"`{c}`").cast("string").alias(c) for c in column_names]
                )

                # Stamp the logical date this data belongs to.
                df = df.withColumn("file_date", lit(file_date))

                # FIX: actually reassign the repartition result (was a no-op).
                df = df.repartition(1)

                # FIX: partition the raw output BY DATE so the raw layer is
                # browsable/filterable by date. This creates real folders like
                #   {adls_raw_path}/file_date=2026-08-02/part-*.parquet
                # instead of hiding the date inside a {batch_id} folder.
                record_count = df.count()
                (
                    df.write.mode("append")
                    .partitionBy("file_date")
                    .parquet(adls_raw_path)
                )

                print(
                    f"[OK] journey='{journey_name}' file_date={file_date} "
                    f"window=[{start_time_str} -> {end_time_str}]: "
                    f"wrote {record_count} record(s) to {adls_raw_path}"
                )
                return "written"

            elif elk_get_response.status_code == 503:
                # Report not ready yet — back off and retry.
                time.sleep(15)
                continue

            elif elk_get_response.status_code == 500:
                # Transient server error — retry.
                continue

            else:
                print(
                    f"[WARN] Unexpected status {elk_get_response.status_code} for "
                    f"journey='{journey_name}' report_id={report_id}; retrying."
                )
                time.sleep(5)
                continue

        except Exception as e:
            print(
                f"[ERROR] journey='{journey_name}' report_id={report_id} "
                f"attempt={attempt + 1}/{max_attempts}: {str(e)}"
            )
            time.sleep(5)
            continue

    # Exhausted all attempts.
    print(
        f"[FAIL] Gave up after {max_attempts} attempts for journey='{journey_name}' "
        f"report_id={report_id} window=[{start_time_str} -> {end_time_str}]. "
        f"Nothing written to the raw layer for this interval."
    )
    return "failed"


# =============================================================================
# CALLER-SIDE CHANGES (in the main loop cell)
# -----------------------------------------------------------------------------
# 1. Make adls_raw_path an f-string. In the screenshot it was:
#        adls_raw_path = ".../landing/{batch_id}/"
#    If that line has no `f` prefix, {batch_id} is written literally (a folder
#    named "{batch_id}" with braces) — an easy-to-miss cause of "no data".
#    Since we now partition BY DATE inside elk_get_api, you can drop batch_id
#    from the path entirely so all runs for a date land together:
#
#        adls_raw_path = (
#            "abfss://raw@ddiprodvyapaaradlsstd.dfs.core.windows.net/"
#            "raw_data/business_banking/merchant/vyapaar/elastic/"
#            "GL-lead-Journey-logs/landing/"
#        )
#    Final layout becomes:  .../landing/file_date=2026-08-02/part-*.parquet
#
# 2. Make file_date match the data you are pulling. Today it is derived from
#    datetime.now() (yesterday), while the window is hard-coded to the special
#    date. If you are backfilling 2026-08-02, set file_date to that date so the
#    partition folder matches the data:
#
#        file_date = "2026-08-02"   # <- uncomment / set for the backfill
#
# 3. Act on the returned status instead of moving on blind:
#
#        status = elk_get_api(report_id, journey, base_url, headers, auth,
#                             verify, adls_raw_path, file_date,
#                             start_time_str, end_time_str)
#        if status == "empty_report":
#            # report job returned no rows for this window — log / re-trigger
#            ...
# =============================================================================
