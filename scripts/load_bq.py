from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import date
import sys

# === KONFIG ===
PROJECT_ID  = "pracuj-pl-pipeline"
LOCATION    = "europe-central2"
DATASET_ID  = "jobs_dw"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.curated_jobs"
BUCKET_PATH = "gs://pracuj-pl-data-lake/curated"

# Parametr dnia (opcjonalnie): python load_bq.py 2025-10-23
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
        print(f"✅ Utworzono dataset {ds_id}")

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
    Pierwszy załadunek: tworzy tabelę partycjonowaną po scrap_date
    dzięki time_partitioning + autodetect (bez dekoratora).
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
    print(f"✅ [FIRST LOAD] Utworzono i załadowano {FINAL_TABLE} z {URI} (partycjonowanie po scrap_date).")

def load_only_partition():
    """
    Idempotencja dzienna: nadpisujemy WYŁĄCZNIE partycję YYYYMMDD.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # może być True – dla istniejącej tabeli i tak wygra jej schemat
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    )
    dest = f"{FINAL_TABLE}${PARTITION}"
    job = client.load_table_from_uri(URI, dest, job_config=job_config)
    job.result()
    print(f"✅ Załadowano {URI} → {dest} (truncate tylko tej partycji).")

def main():
    ensure_dataset()
    tbl = get_table_or_none()

    if tbl is None:
        # Tabela nie istnieje → pierwszy load stworzy ją z partycjonowaniem
        first_load_creates_partitioned_table()
    else:
        if not is_partitioned_by_scrap_date(tbl):
            # Jednorazowa migracja (ręcznie/SQL): przenieś stare dane do partycjonowanej
            msg = f"""
⚠️ Tabela {FINAL_TABLE} istnieje, ale NIE jest partycjonowana po scrap_date.
Uruchom jednorazowo (w konsoli BQ) migrację:
------------------------------------------------
CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.curated_jobs_v2`
PARTITION BY DATE(scrap_date) AS
SELECT * FROM `{FINAL_TABLE}`;

-- (opcjonalnie) zweryfikuj dane, a potem podmień nazwy:
DROP TABLE `{FINAL_TABLE}`;
ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.curated_jobs_v2`
RENAME TO curated_jobs;
------------------------------------------------
Następnie uruchom skrypt ponownie.
"""
            raise RuntimeError(msg)
        # OK, tabela jest partycjonowana → ładujemy tylko partycję danego dnia
        load_only_partition()

    # Info
    table = client.get_table(FINAL_TABLE)
    print(f"📊 Wiersze: {table.num_rows}")
    print(f"📅 Ostatnia modyfikacja: {table.modified}")

if __name__ == "__main__":
    main()
