# **Pracuj.pl IT Job Offers Pipeline**

A fully orchestrated data engineering pipeline on **Google Cloud Platform** using **Docker, Airflow, Dataproc (Spark), BigQuery, Looker Studio, Vertex AI** and **Terraform**.

---

## 🧭 Description

This project builds a complete **end-to-end data platform** that automatically scrapes, processes, and analyzes job listings from **pracuj.pl**. The data are **independently acquired through a custom-built, advanced web scraper** developed from scratch in Python and containerized for Cloud Run execution. The scraper collects raw HTML data, which are then parsed, cleaned, and enriched before being loaded into structured data layers. The cleaned and curated datasets are stored in a **BigQuery Data Warehouse**, which serves as the central analytical layer of the project. From there, data are continuously updated and made available for exploration and visualization through automated Airflow DAGs and the Looker Studio dashboard. 

---

## 🎯 Objective

The objective of this project was to **apply industry-standard data engineering practices** using the most current open-source and managed frameworks within the GCP ecosystem. 
It aimed to consolidate the author’s skills through a **real-case scenario implementation**, demonstrate understanding of **serverless design patterns**, **modular IaC with Terraform**, and **pipeline orchestration** with Airflow — all while producing a portfolio project that showcases technical competence to potential employers.

---

## 🏗️ Architecture

<img width="1280" height="528" alt="wykres readme flat" src="https://github.com/user-attachments/assets/83bcaa1e-ed76-4e4a-8fa0-8312d7a06f95" />


The pipeline follows the **ETL pattern and is fully orchestrated in Airflow:**

1. **Scraper Job (Cloud Run + Docker)** → daily job that collects raw job listings (HTML/JSON), saves a parquet with offer links in a data lake folder "raw"
2. **Enricher Job (Cloud Run + Docker)** → skipes offers already present in a data warehouse, then iterates through listings left, scrapes all the desired details from each listing page, parses and enriches data, saves it in a data lake folder "enriched"
3. **Data Lake (GCS)** → stores raw and enriched Parquet data, organized by scrape date (daily partitions) to maintain a clear data lineage and guarantee reproducible, idempotent loads across pipeline runs
4. **Spark Cleaning (Dataproc)** → performs large-scale data transformations and feature engineering (e.g. extracting categories, seniorities via regex) using an ephemeral Dataproc cluster. The use of Spark ensures scalability and fault-tolerant processing of large datasets, while ephemeral clusters minimize cost by provisioning compute only during job execution.
5. **Load to BigQuery (DW)** → appends daily partitions into a date-partitioned table, improving query efficiency and supporting incremental ingestion logic that prevents re-scraping or re-loading listings already present in the warehouse.
6. **Looker Studio Dashboard** → visualizes aggregated metrics (salary distribution, job count by region, tech stack demand)
7. **Vertex AI Modeling (IN PROGRESS) → builds ML models on job features (salary prediction, classification)**

All steps are orchestrated via **Cloud Composer DAG**, ensuring dependency management, retries, and idempotent runs. The deployment is automaed using **Terraform** layer in the repo.

---

## 🧩 Dataset

The dataset contains job postings scraped from pracuj.pl
 — the largest Polish job marketplace, offering a rich and representative view of the IT job market.
Data are collected via two Dockerized Cloud Run jobs (scraper and enricher) producing daily incremental batches of new listings, ensuring traceability and reproducibility across runs.

The dataset was engineered to be ML-ready and analytically consistent.
Data transformations were implemented in PySpark (Dataproc ephemeral cluster), leveraging distributed processing for scalable and idempotent ETL.
Key preprocessing and feature-engineering steps include:

Text normalization & parsing – extraction and standardization of salary ranges (salary_min, salary_max, salary_avg, salary_equiv_monthly) from noisy raw text, unified across formats and units.

Categorical standardization – normalization of contract types, working modes, and seniority mapping (intern → junior → mid → senior → manager) into consistent taxonomies (position_level, position_rank).

Feature extraction – transformation of nested HTML/JSON fields into structured lists of skills, technologies, and responsibilities usable for downstream ML features.

Semantic classification – automatic grouping of roles into unified specializations (e.g., data, backend, ux_ui, helpdesk_admin) based on keyword mapping logic.

Geolocation cleaning – regex-based extraction and standardization of cities and regions, including multilingual variants (PL/EN).

Temporal versioning – data stored as daily Parquet partitions in GCS for deterministic reprocessing and reproducible historical snapshots.

Final outputs are stored in Parquet format in the GCS Data Lake and loaded into a partitioned BigQuery Data Warehouse, powering analytical dashboards (Looker Studio) and machine-learning models (Vertex AI).

---

## 📊 Final Result

<img width="1280" alt="final_dashboard" src="https://github.com/terleckimaciej/cloud-data-engineering-portfolio-pipeline/blob/main/assets/final_dahsboard.jpg" /> 


---

## 📁 Repository Structure

```
pracuj-pl-pipeline/
│
├── cloudrun_jobs/          # Cloud Run Jobs (Dockerized) – web scraper & enricher fetching job listings
│   ├── scraper/            # Extracts job listing URLs from pracuj.pl
│   └── enricher/           # Visits individual listings and extracts detailed attributes
│
├── dags/                   # Apache Airflow DAGs – orchestration of the entire pipeline
│   └── pracuj_pipeline_dag.py
│
├── pyspark_jobs/           # PySpark jobs executed on ephemeral Dataproc clusters
│   └── transform_job.py    # Data cleaning, normalization & feature engineering logic
│
├── scripts/                # Utility scripts (e.g. BigQuery loader)
│   └── load_bq.py          # Loads curated data into partitioned BigQuery tables
│
├── terraform/              # Infrastructure as Code (Terraform)
│   ├── main.tf             # Core GCP resources: GCS, Dataproc, Composer, BigQuery
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/            # Modular definitions for each GCP service
│
├── .gitignore              # Git exclusions for logs, data, and local configs
├── LICENSE                 # Open-source license (MIT)
├── README.md               # Project documentation (architecture, dataset, usage)
└── requirements.txt        # Python dependencies for development environment
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

* Cloud Storage buckets (to store `raw`, `enriched`, `curated` data folders)
* BigQuery dataset and table 
* Cloud Composer environment
* Artifact Registry & Cloud Run Jobs with Docker contenerization
* Pyspark code located in bucket directory specified in DAG.

```bash
cd terraform
terraform init
terraform apply
```

---

### 2️⃣ Orchestrate with Airflow (Cloud Composer)

Upload your DAG (`pracuj_pipeline_dag.py`) to the Composer bucket. Also remember to feed newly created composer environment bucket with load_bq.py script - directory specified in a DAG code (the bucket name is dynamic, so Terraform can't handle this).
The DAG executes: Scraper → Enricher → Spark Job → BigQuery Load → Dashboard Refresh

---

### 3️⃣ Analyze in BigQuery & Looker Studio

Connect Looker Studio directly to BigQuery and build visualizations.

Example metrics:

* average salary by city and technology
* number of offers per company
* time trends in new job postings

---

### 4️⃣ (Optional) Model with Vertex AI 

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

* Train a Vertex AI model for salary prediction - IN PROGRESS
* Implement **data quality tests** 
* Enable **monitoring and alerting** via Cloud Logging

---

## 🙏 Acknowledgements

This project was inspired by **Adi Wijaya’s *****************************************************************************************************************************************************************************************************************Data Engineering with Google Cloud Platform***************************************************************************************************************************************************************************************************************** (2024)** and structured following best practices mentioned in the book and from real GCP portfolio projects.
