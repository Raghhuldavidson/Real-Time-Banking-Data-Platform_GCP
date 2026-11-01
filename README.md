
# 🏦 Real-Time Banking Data Platform on Google Cloud Platform

A production-style banking data platform built on GCP that ingests batch + CDC data from Cloud SQL, streams real-time transaction events from Pub/Sub, and processes everything through a **Bronze → Silver → Gold** medallion architecture in BigQuery — fully orchestrated by **Cloud Composer (Airflow)** via a Master DAG.

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


## 🗄 Source System

**Cloud SQL — MySQL database `banking_db`**

| Table | Key Columns |
|---|---|
| `customers` | `customer_id` PK, `first_name`, `last_name`, `date_of_birth`, `email`, `phone`, `kyc_status`, `created_at`, `updated_at` |
| `accounts` | `account_id` PK, `customer_id` FK, `account_type`, `balance DECIMAL(15,2)`, `currency`, `status`, `opened_date`, `created_at`, `updated_at` |
| `transactions` | `transaction_id` PK, `account_id` FK, `transaction_type`, `amount DECIMAL(15,2)`, `transaction_ts`, `channel`, `status`, `created_at`, `updated_at` |

> **Note:** `DECIMAL` columns are handled as `STRING` in Bronze to avoid float precision loss — cast to `NUMERIC` only in Silver.

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


---
## Pub/Sub Producer

Run the sample event producer with explicit parameters:

```bash
python pubsub/banking_transaction_producer.py \
  --project-id dev-gcp-100 \
  --topic-id banking-transactions-topic \
  --events-per-second 2 \
  --max-events 100
```


