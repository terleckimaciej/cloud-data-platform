from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from datetime import datetime, timedelta
import pendulum
import subprocess

# --- KONFIG ---
PROJECT_ID = "pracuj-pl-pipeline"
REGION = "europe-central2"
BUCKET_NAME = "pracuj-pl-data-lake"

# Dataproc ephemeral
DATAPROC_CLUSTER_NAME = "pracuj-curated-ephemeral"
PYSPARK_JOB = f"gs://{BUCKET_NAME}/pyspark_jobs/curated_transform.py"

# Enriched output expected
ENRICHED_PATH = "enriched/job_details_enriched_" + pendulum.now("Europe/Warsaw").to_date_string() + ".parquet"

# Skrypt do ładowania danych do BigQuery (Composer mount)
LOAD_SCRIPT = "/home/airflow/gcs/data/scripts/load_bq.py"


# --- FUNKCJA POMOCNICZA ---
def run_load_bq():
    """Ładuje przetworzone dane z curated do BigQuery."""
    subprocess.run(["python", LOAD_SCRIPT], check=True)


# --- USTAWIENIA OGÓLNE DAG ---
default_args = {
    "owner": "maciek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # domyślnie bez retry
}

with DAG(
    dag_id="pracuj_pl_daily",
    description="Codzienny ETL: scraper → enricher → Spark transform → load BQ",
    start_date=datetime(2025, 10, 1),
    schedule_interval="0 7 * * *",  # codziennie o 7:00 CET
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    # === 1️⃣ SCRAPER JOB (Cloud Run) ===
    run_scraper = BashOperator(
        task_id="run_scraper",
        bash_command=f"gcloud run jobs execute pracuj-scraper-job --region {REGION} --project {PROJECT_ID} --wait",
        retries=1,
        retry_delay=timedelta(minutes=10),
    )

    # === 2️⃣ ENRICHER JOB (Cloud Run) ===
    run_enricher = BashOperator(
        task_id="run_enricher",
        bash_command=f"gcloud run jobs execute pracuj-enricher-job --region {REGION} --project {PROJECT_ID} --wait",
        retries=2,  # najcięższy i najbardziej wrażliwy job
        retry_delay=timedelta(minutes=15),
    )

    # === 3️⃣ SENSOR: czekaj aż ENRICHED pojawi się w GCS ===
    wait_for_enriched_file = GCSObjectExistenceSensor(
        task_id="wait_for_enriched_file",
        bucket=BUCKET_NAME,
        object=ENRICHED_PATH,
        poke_interval=120,      # co 2 minuty
        timeout=60 * 60 * 6,    # maks 6 godzin
        mode="reschedule",      # nie blokuje workera
    )

    # === 4️⃣ SPARK TRANSFORMACJA NA DATAPROC (ephemeral) ===
    create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_name=DATAPROC_CLUSTER_NAME,
    region=REGION,
    cluster_config={
        # MASTER — Spark driver (zarządza jobem)
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-2",        # lekka maszyna: 2 vCPU, 8 GB RAM
            "disk_config": {"boot_disk_size_gb": 100},  # 100 GB dysk — wystarczy w zupełności
        },
        # WORKERS — Spark executors (faktyczna moc obliczeniowa)
        "worker_config": {
            "num_instances": 2,                         # ← teraz 2 maszyny robocze
            "machine_type_uri": "e2-standard-2",
            "disk_config": {"boot_disk_size_gb": 100},
        },
        # Oprogramowanie Dataproc + Spark 3.x
        "software_config": {"image_version": "2.2-debian12"},
        # Region / strefa
        "gce_cluster_config": {"zone_uri": f"{REGION}-a"},
    },
    retries=1,
    retry_delay=timedelta(minutes=5),
)


    # Definicja joba PySpark
    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_JOB,  # np. gs://pracuj-pl-data-lake/pyspark_jobs/curated_transform.py
        },
    }

    # Uruchomienie PySpark na klastrze
    run_pyspark = DataprocSubmitJobOperator(
        task_id="run_pyspark",
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # Usunięcie klastra (zawsze — nawet po błędzie)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",  # usuwa klaster nawet jeśli Spark się wywali
    )

    # === 5️⃣ ŁADOWANIE DO BIGQUERY ===
    load_bq = PythonOperator(
        task_id="load_bq",
        python_callable=run_load_bq,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # === KOLEJNOŚĆ ===
    run_scraper >> run_enricher >> wait_for_enriched_file >> create_cluster >> run_pyspark >> delete_cluster >> load_bq
