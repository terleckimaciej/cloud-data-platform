from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import date
import sys

# === CONFIG ===
PROJECT_ID  = "pracuj-pl-pipeline"
LOCATION    = "europe-central2"
DATASET_ID  = "jobs_dw"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.curated_jobs"
BUCKET_PATH = "gs://pracuj-pl-data-lake/curated"

# Day parameter (optional): python load_bq.py 2025-10-23
SCRAP_DATE  = (sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
PARTITION   = SCRAP_DATE.replace("-", "")     # YYYYMMDD
URI         = f"{BUCKET_PATH}/{SCRAP_DATE}/*.parquet"

client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

def ensure_dataset():
    ds_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(ds_id)
    except NotFound:
        ds = bigquery.Dataset(ds_id)
        ds.location = LOCATION
        client.create_dataset(ds)
        print(f"✅ Created dataset {ds_id}")

def get_table_or_none():
    try:
        return client.get_table(FINAL_TABLE)
    except NotFound:
        return None

def is_partitioned_by_scrap_date(tbl: bigquery.Table) -> bool:
    tp = tbl.time_partitioning
    return bool(tp and tp.field == "scrap_date")

def first_load_creates_partitioned_table():
    """
    First load: creates a table partitioned by scrap_date
    using time_partitioning + autodetect 
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
        time_partitioning=bigquery.TimePartitioning(field="scrap_date"),
    )
    job = client.load_table_from_uri(URI, FINAL_TABLE, job_config=job_config)
    job.result()
    print(f"✅ [FIRST LOAD] Created and loaded {FINAL_TABLE} from {URI} (partitioned by scrap_date).")

def load_only_partition():
    """
    Daily idempotence: overwrite ONLY the YYYYMMDD partition.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # can be True – for an existing table, its schema will prevail
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    )
    dest = f"{FINAL_TABLE}${PARTITION}"
    job = client.load_table_from_uri(URI, dest, job_config=job_config)
    job.result()
    print(f"✅ Loaded {URI} → {dest} (truncate only this partition).")

def main():
    ensure_dataset()
    tbl = get_table_or_none()

    if tbl is None:
        # Table does not exist → first load will create it with partitioning
        first_load_creates_partitioned_table()
    else:
        if not is_partitioned_by_scrap_date(tbl):
            # One-time migration (manually/SQL): move old data to a partitioned table
            msg = f"""
⚠️ Table {FINAL_TABLE} exists but is NOT partitioned by scrap_date.
Run a one-time migration (in the BQ console):
------------------------------------------------
CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.curated_jobs_v2`
PARTITION BY DATE(scrap_date) AS
SELECT * FROM `{FINAL_TABLE}`;

-- (optionally) verify the data, then rename tables:
DROP TABLE `{FINAL_TABLE}`;
ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.curated_jobs_v2`
RENAME TO curated_jobs;
------------------------------------------------
Then rerun the script.
"""
            raise RuntimeError(msg)
        # OK, the table is partitioned → load only the partition for the given day
        load_only_partition()

    # Info
    table = client.get_table(FINAL_TABLE)
    print(f" Rows: {table.num_rows}")
    print(f" Last modified: {table.modified}")

if __name__ == "__main__":
    main()
