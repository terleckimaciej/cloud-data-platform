<img width="1280" height="720" alt="image" src="https://github.com/user-attachments/assets/21dda2d6-068d-48fe-bdd3-a76ef147cd65" /># Pracuj.pl Data Pipeline

A fully orchestrated data engineering pipeline on **Google Cloud Platform** using **Docker, Airflow, Dataproc (Spark), BigQuery, Looker Studio, Vertex AI** and **Terraform**.

---

## 🧭 Description

This project builds a complete **end-to-end data platform** that automatically scrapes, processes, and analyzes job listings from **pracuj.pl**. The data are **independently acquired through a custom-built, advanced web scraper** developed from scratch in Python and containerized for Cloud Run execution. The scraper collects raw HTML data, which are then parsed, cleaned, and enriched before being loaded into structured data layers. The cleaned and curated datasets are stored in a **BigQuery Data Warehouse**, which serves as the central analytical layer of the project. From there, data are continuously updated and made available for exploration and visualization through automated Airflow DAGs and the Looker Studio dashboard. This dashboard presents key insights such as salary distributions, job demand by region, and technology trends, forming the final stage of the pipeline and demonstrating a complete data-to-insight workflow.

---

## 🎯 Objective

The objective of this project was to **apply industry-standard data engineering practices** using the most current open-source and managed frameworks within the GCP ecosystem. 
It aimed to consolidate the author’s skills through a **real-case scenario implementation**, demonstrate understanding of **serverless design patterns**, **modular IaC with Terraform**, and **pipeline orchestration** with Airflow — all while producing a portfolio project that showcases technical competence to potential employers.

---

## 🏗️ Architecture

<img width="1280" height="720" alt="image" src="https://github.com/user-attachments/assets/058c8c30-e524-41a2-9ff4-f4393ebb7434" />

The pipeline follows the **ETL pattern and is fully orchestrated in Airflow:**

1. **Scraper Job (Cloud Run + Docker)** → collects raw job listings (HTML/JSON)
2. **Enricher Job (Cloud Run + Docker)** → iterates through listings, skipes offers already present in a dw, scrapes details from each listing, parses and enriches data
3. **Data Lake (GCS)** → stores raw and enriched Parquet data
4. **Spark Cleaning (Dataproc)** → performs transformations, creates categories, seniorities (mainly using regex) etc, normalizes normalization
5. **Load to BigQuery (DW)** → appends daily partitions into **partitioned** table
6. **Looker Studio Dashboard** (IN PROGRESS) → visualizes aggregated metrics (salary distribution, job count by region, tech stack demand)
7. **Vertex AI Modeling (IN PROGRESS) → builds ML models on job features (salary prediction, classification)**

All steps are orchestrated via **Cloud Composer DAG**, ensuring dependency management, retries, and idempotent runs. The deployment is automaed using **Terraform** layer in the repo.

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
├── scripts/               # Utility Python scripts (load to BQ)
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

This project was inspired by **Adi Wijaya’s *****************************************************************************************************************************************************************************************************************Data Engineering with Google Cloud Platform***************************************************************************************************************************************************************************************************************** (2024)** and structured following best practices from real GCP portfolio projects.
