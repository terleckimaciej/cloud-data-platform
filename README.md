# Pracuj.pl Data Pipeline

A fully orchestrated data engineering pipeline on **Google Cloud Platform** using **Docker, Airflow, Dataproc (Spark), BigQuery, Looker Studio, Vertex AI** and **Terraform**.

---

## 🧭 Description

This project builds a complete **end-to-end data platform** that automatically extracts, processes, and analyzes job listings from **pracuj.pl**. It showcases the full lifecycle of a modern data pipeline — from web scraping to dashboard and ML modeling — designed for scalability, automation, and reproducibility within the GCP ecosystem.

---

## 🎯 Objective

The pipeline scrapes job postings from pracuj.pl using Cloud Run jobs, enriches them with additional attributes (salary ranges, company info, specialization, etc.), stores the raw and processed data in a **GCS Data Lake**, transforms them via **PySpark on Dataproc**, loads clean data into **BigQuery Data Warehouse**, and finally exposes insights through **Looker Studio dashboards** and **Vertex AI models** predicting salary or job category.

This project demonstrates the practical integration of **data engineering components**: ingestion → transformation → orchestration → visualization → ML.

---

## 🧩 Dataset

The dataset consists of job postings scraped from [pracuj.pl](https://www.pracuj.pl/).
Each record contains fields such as:

* job title, company, location
* salary information (parsed and normalized)
* job category and seniority
* publication date, offer ID, description text

The scraper and enricher components handle deduplication and normalization, producing **daily incremental batches** of new listings.

---

## ⚙️ Tools & Technologies

* **Cloud:** Google Cloud Platform
* **IaC:** Terraform
* **Containerization:** Docker, Cloud Run Jobs
* **Data Orchestration:** Airflow (Cloud Composer)
* **Data Processing:** Apache Spark (Dataproc)
* **Storage:** Google Cloud Storage (Data Lake)
* **Data Warehouse:** BigQuery
* **Machine Learning:** Vertex AI / BigQuery ML
* **Visualization:** Looker Studio
* **Language:** Python (Requests, BeautifulSoup, PySpark, google-cloud-bigquery)

---

## 🏗️ Architecture

The pipeline follows the **ELT** pattern and is fully orchestrated in Airflow:

1. **Scraper Job (Cloud Run + Docker)** → collects raw job listings (HTML/JSON)
2. **Enricher Job (Cloud Run + Docker)** → parses, enriches, and standardizes data
3. **Data Lake (GCS)** → stores raw and enriched Parquet data
4. **Spark Cleaning (Dataproc)** → performs transformations, deduplication, and schema normalization
5. **Load to BigQuery (DW)** → appends daily partitions into curated tables
6. **Looker Studio Dashboard** → visualizes aggregated metrics (salary distribution, job count by region, tech stack demand)
7. **Vertex AI Modeling** → builds ML models on job features (salary prediction, classification)

All steps are orchestrated via **Cloud Composer DAG**, ensuring dependency management, retries, and idempotent runs.

---

## 📊 Final Result

The final dashboard built in **Looker Studio** includes:

* salary ranges by technology and location
* daily new job postings
* most frequent job titles and categories
* ML-based predictions for salary range

---

## 📁 Repository Structure

```
pracuj-pl-pipeline/
│
├── dags/                  # Airflow DAG definitions (orchestrating the pipeline)
├── spark_jobs/            # PySpark transformation jobs for Dataproc
├── docker/                # Dockerfiles for Cloud Run jobs (scraper & enricher)
├── scripts/               # Utility Python scripts (load to BQ, test jobs)
├── terraform/             # Infrastructure as Code (GCS buckets, Composer, Dataproc, BQ datasets)
├── looker/                # Dashboard templates or query sources
└── README.md              # Project documentation
```

---

## 🚀 Setup

### ⚠️ Costs Warning

Running this pipeline in GCP will incur costs. You can use **$300 free credit** by creating a new GCP account.

### ✅ Pre-requisites

* GCP account with project access
* gcloud CLI installed and authenticated
* Terraform installed
* Docker installed
* Enable required APIs (Cloud Run, BigQuery, Dataproc, Composer, Storage, Vertex AI)

---

## 🧱 Deployment Steps

### 1️⃣ Provision Infrastructure

Use Terraform scripts from `/terraform` to automatically create:

* Cloud Storage buckets (`raw`, `enriched`, `curated`)
* BigQuery dataset and table (`jobs_dw.curated_jobs`)
* Cloud Composer environment
* Artifact Registry & Cloud Run Jobs for scraper and enricher

```bash
cd terraform
terraform init
terraform apply
```

---

### 2️⃣ Build & Deploy Docker Containers

Each job (scraper, enricher) has its own `Dockerfile`.
Build and push them to Artifact Registry:

```bash
gcloud builds submit --tag REGION-docker.pkg.dev/PROJECT_ID/pracuj-pl/scraper:latest
gcloud builds submit --tag REGION-docker.pkg.dev/PROJECT_ID/pracuj-pl/enricher:latest
```

Deploy them as Cloud Run Jobs:

```bash
gcloud run jobs create pracuj-scraper --image=... --max-retries=3 --region=europe-central2
gcloud run jobs create pracuj-enricher --image=... --max-retries=3 --region=europe-central2
```

---

### 3️⃣ Orchestrate with Airflow (Cloud Composer)

Upload your DAG (`pracuj_pipeline_dag.py`) to the Composer bucket.
The DAG executes: Scraper → Enricher → Spark Job → BigQuery Load → Dashboard Refresh

---

### 4️⃣ Analyze in BigQuery & Looker Studio

Connect Looker Studio directly to BigQuery and build visualizations.

Example metrics:

* average salary by city and technology
* number of offers per company
* time trends in new job postings

---

### 5️⃣ (Optional) Model with Vertex AI / BigQuery ML

Use the curated dataset for training regression models predicting salary ranges or classification models for job type segmentation.

---

## 🧩 Debug

If you encounter issues:

* check Airflow logs per task instance
* verify IAM permissions for Composer, Dataproc, and BigQuery
* ensure bucket paths match your environment variables
* use `gcloud dataproc jobs describe` for Spark failures

---

## 💡 Future Improvements

* Use **managed Kafka or Pub/Sub** for near-real-time ingestion
* Add **CI/CD** with Cloud Build triggers for automatic deployments
* Implement **data quality tests** (Great Expectations or dbt)
* Introduce **dimension & fact tables** in BigQuery
* Enable **monitoring and alerting** via Cloud Logging
* Use **Soda** or **dbt tests** for schema validation

---

## 🙏 Acknowledgements

This project was inspired by **Adi Wijaya’s *Data Engineering with Google Cloud Platform* (2024)** and structured following best practices from real GCP portfolio projects.
