
# 🏦 Real-Time Banking Data Platform on Google Cloud Platform

A production-style banking data platform built on GCP that ingests batch + CDC data from Cloud SQL, streams real-time transaction events from Pub/Sub, and processes everything through a **Bronze → Silver → Gold** medallion architecture in BigQuery — fully orchestrated by **Cloud Composer (Airflow)** via a Master DAG.

---

## 📌 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [GCP Services Used](#-gcp-services-used)
- [Repository Structure](#-repository-structure)
- [Source System](#-source-system)
- [BigQuery Datasets](#-bigquery-datasets)
- [Metadata / Control Tables](#-metadata--control-tables)
- [Data Layers](#-data-layers)
  - [Bronze](#bronze-layer)
  - [Silver](#silver-layer)
  - [Gold](#gold-layer)
- [Pipelines](#-pipelines)
- [Airflow / Cloud Composer DAGs](#-airflow--cloud-composer-dags)
- [Key Design Concepts](#-key-design-concepts)
- [Configuration](#-configuration)
- [Setup & Deployment](#-setup--deployment)
- [Known Limitations / Roadmap](#-known-limitations--roadmap)
- [Author](#-author)

---

## 🏗 Architecture Overview

**Orchestration:** Cloud Composer `master_dag` → triggers `ingestion` → `bronze` → `silver` → `gold` sequentially with `wait_for_completion=True`.

---

## ☁️ GCP Services Used

| Service | Purpose |
|---|---|
| **Cloud SQL (MySQL)** | Source system (`banking_db`) |
| **Cloud Storage (GCS)** | Raw Parquet, temp/staging, SQL script storage |
| **Dataflow (Apache Beam)** | CDC batch pipeline + streaming pipeline |
| **Dataproc (PySpark)** | GCS Parquet → BigQuery Bronze loader |
| **BigQuery** | Bronze / Silver / Gold / Metadata datasets |
| **Pub/Sub** | Real-time transaction event stream |
| **Cloud Composer (Airflow)** | End-to-end orchestration |
| **IAM** | Service account auth for Cloud SQL Connector, Dataflow, BQ |

---

## 📂 Repository Structure

---

## 🗄 Source System

**Cloud SQL — MySQL database `banking_db`**

| Table | Key Columns |
|---|---|
| `customers` | `customer_id` PK, `first_name`, `last_name`, `date_of_birth`, `email`, `phone`, `kyc_status`, `created_at`, `updated_at` |
| `accounts` | `account_id` PK, `customer_id` FK, `account_type`, `balance DECIMAL(15,2)`, `currency`, `status`, `opened_date`, `created_at`, `updated_at` |
| `transactions` | `transaction_id` PK, `account_id` FK, `transaction_type`, `amount DECIMAL(15,2)`, `transaction_ts`, `channel`, `status`, `created_at`, `updated_at` |

> **Note:** `DECIMAL` columns are handled as `STRING` in Bronze to avoid float precision loss — cast to `NUMERIC` only in Silver.

---

## 🧱 BigQuery Datasets

| Dataset | Purpose |
|---|---|
| `banking_bronze` | Raw ingested data (no business transforms) |
| `banking_silver` | Cleansed SCD2 dims + unified fact |
| `banking_gold` | Business KPI tables |
| `banking_metadata` | Config, control, DQ rules, audit logs |

---

## ⚙️ Metadata / Control Tables (`banking_metadata`)

These tables **drive the pipelines** — no hardcoded table lists anywhere in code.

| Table | Role |
|---|---|
| `table_ingestion_config` | Which tables Bronze loads, from where, how (`load_type`, `primary_key`, `watermark_column`, `source_path`, `is_active`, …) |
| `cdc_config` | Watermarks for the Dataflow CDC pipeline (`last_success_ts`) |
| `data_quality_rules` | Column-level DQ rules (`rule_type`, `rule_value`, `severity`) |
| `ingestion_audit_log` | Per-run audit: `status`, `records_read`, `records_written`, `start_ts`, `end_ts`, `error_message` |
| `table_details` | Dataset/table catalog + creation timestamps |

---

## 🥉🥈🥇 Data Layers

### Bronze Layer (`banking_bronze`)

Raw + technical enrichment only (`bronze_load_ts`). Partitioned by date, clustered by primary key.

| Table | Partition | Cluster |
|---|---|---|
| `bronze_customers` | `DATE(bronze_load_ts)` | `customer_id` |
| `bronze_accounts` | `DATE(bronze_load_ts)` | `account_id` |
| `bronze_transactions` | `DATE(bronze_load_ts)` | `transaction_id` |
| `bronze_streaming_transactions` | `DATE(event_ts)` | `account_id, channel` |

Amounts in batch Bronze are **STRING** (decimal safety).

### Silver Layer (`banking_silver`)

| Table | Pattern |
|---|---|
| `dim_customer` | SCD Type 2 (`MERGE` on `customer_id` + `is_current`) |
| `dim_account` | SCD Type 2 (`MERGE` on `account_id` + `is_current`) |
| `fact_transactions` | Unified fact — `UNION ALL` batch + streaming, dedupe via `ROW_NUMBER()` with **batch priority over streaming** |

SCD2 convention:
- `effective_start_ts`, `effective_end_ts` (`9999-12-31` for current), `is_current`
- Surrogate keys via `GENERATE_UUID()`

### Gold Layer (`banking_gold`)

| Table | Description |
|---|---|
| `customer_360` | Per-customer rollup: accounts, balance, last txn |
| `daily_account_balance` | End-of-day balance per account |
| `daily_transaction_summary` | Daily totals by channel |
| `fraud_indicators` | Suspicious txn count/amount with reason |

---

## 🔄 Pipelines

### 1. CDC Batch — Cloud SQL → GCS Parquet
**File:** `pipelines/batch/cloudsql_cdc_pipeline.py` (Apache Beam / Dataflow)

- Reads active tables from `cdc_config`
- Infers schema from MySQL `DESCRIBE` (DECIMAL → STRING)
- `SELECT * WHERE watermark_col > last_success_ts`
- Cloud SQL Python Connector + SQLAlchemy (IAM-based, pooled)
- Writes Parquet to `gs://banking-raw-bucket/cloudsql/<table>/ingestion_date=YYYY-MM-DD/`
- Updates `last_success_ts` **only after success**

### 2. Bronze Loader — GCS Parquet → BigQuery
**File:** `pipelines/batch/bronze_gcs_to_bq.py` (PySpark / Dataproc)

- Reads `table_ingestion_config` (`is_active=true`, `target_dataset='banking_bronze'`)
- Explicit `StructType` schema (no drift)
- Fails on empty read
- CDC dedupe: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY watermark DESC) = 1`
- Appends to `banking_bronze.<target_table>`
- **Audit row always written in `finally`** (SUCCESS/FAILED)

### 3. Streaming — Pub/Sub → BigQuery Bronze
**File:** `pipelines/streaming/streaming_transactions_pipeline.py` (Apache Beam / Dataflow, streaming)

- Reads `banking-transactions-sub`
- Validates JSON (required fields), parses `event_ts`
- Invalid records → `deadletter` tagged output
- `FixedWindows(60s)` + `allowed_lateness=300s`
- Per-window dedupe by `event_id`
- Writes to `bronze_streaming_transactions` (`WRITE_APPEND`, `CREATE_NEVER`)
- **Producer:** `banking_transaction_producer.py` — synthetic events at configurable TPS

### 4. Silver Transformations (BigQuery SQL)
**Files:** `sql/silver/01_dim_account.sql`, `02_dim_customer.sql`, `03_fact_transactions.sql`

- `MERGE`-based SCD2 for dims
- `fact_transactions`: batch + streaming `UNION ALL` → `ROW_NUMBER()` dedupe (batch wins) → `MERGE`

### 5. Gold Transformations (BigQuery SQL)
**Files:** `sql/gold/*.sql`

- Discovered dynamically from GCS, run in parallel via dynamic task mapping

---

## 🎼 Airflow / Cloud Composer DAGs

| DAG | Purpose |
|---|---|
| `master_dag` | Sequential trigger: `ingestion → bronze → silver → gold` (each `wait_for_completion=True`, appropriate timeouts) |
| `banking_ingestion_dag` | `BeamRunPythonPipelineOperator` → `cloudsql_cdc_pipeline.py` on Dataflow |
| `banking_bronze_dag` | `DataprocStartClusterOperator` → `DataprocSubmitJobOperator` (`bronze_gcs_to_bq.py`) → `DataprocStopClusterOperator` |
| `banking_streaming_ingestion_dag` | `BeamRunPythonPipelineOperator` → streaming Dataflow job |
| `banking_silver_dag` | Dynamic task mapping over silver `*.sql` |
| `banking_gold_dag` | Dynamic task mapping over gold `*.sql` |

**`master_dag` chain:**
`allowed_states=["success"]`, `failed_states=["failed"]`.

**Shared config (`config/banking_config.py`):** `get_setting()` resolves Airflow Variable → env var → default. Exposes `PROJECT_ID`, `REGION`, `BQ_LOCATION`, `TEMP_BUCKET`, `RUNTIME_SERVICE_ACCOUNT`, `DEFAULT_ARGS`, `TEMP_LOCATION`, `STAGING_LOCATION`.

---

## 💡 Key Design Concepts

- **Medallion architecture** — Bronze (raw), Silver (conformed), Gold (KPIs)
- **CDC with watermark** — incremental, idempotent, watermark advances only on success
- **SCD Type 2 via BigQuery `MERGE`** — dims with `is_current` + `effective_*` columns
- **Batch vs streaming unification** — priority-based dedupe (batch wins on `transaction_id` conflict)
- **Event-time windowing** — `FixedWindows(60s)` + `allowed_lateness=300s` + per-window `event_id` dedupe
- **Metadata-driven ingestion** — config tables, not code, control what loads
- **Explicit schemas** — `StructType` prevents drift at Bronze
- **Audit with `finally` guarantees** — failures always logged
- **Dynamic task mapping** — N SQL files → N parallel Airflow tasks
- **Master DAG orchestration** — sub-DAGs triggered with `wait_for_completion`
- **Decimal-safe handling** — `DECIMAL` kept as `STRING` until Silver casts to `NUMERIC`

---
## Pub/Sub Producer
### Run the sample event producer with explicit parameters:
python pubsub/banking_transaction_producer.py \
  --project-id dev-gcp-100 \
  --topic-id banking-transactions-topic \
  --events-per-second 2 \
  --max-events 100


---

## 🚀 Setup & Deployment

### Prerequisites
- GCP project with billing enabled
- APIs enabled: Cloud SQL, Dataflow, Dataproc, BigQuery, Pub/Sub, Cloud Composer, Cloud Storage
- Service account with roles: `Cloud SQL Client`, `Dataflow Worker`, `Dataproc Worker`, `BigQuery Data Editor`, `BigQuery Job User`, `Pub/Sub Subscriber`, `Storage Object Admin`

