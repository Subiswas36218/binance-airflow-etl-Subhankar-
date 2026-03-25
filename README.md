# 🚀 Binance Crypto Data Pipeline with Apache Airflow

A **production-ready, end-to-end data engineering project** that ingests, processes, and aggregates real-time cryptocurrency data (Bitcoin - BTC/USDT) using **Apache Airflow**.

This project demonstrates **modern ETL pipeline design**, including **stream ingestion, batch aggregation, historical backfill, and fault-tolerant workflows** — making it ideal for **data engineering portfolios, internships, and cloud projects**.

---

## 📌 Project Overview

This pipeline collects **minute-level Bitcoin price data** from the Binance API and builds a multi-layered data architecture:

```
Raw (Minute) → Hourly Aggregation → Daily Aggregation
```

Additionally, a **backfill DAG** reconstructs historical data using Binance’s Klines API.

---

## 🧱 Architecture

```
                ┌──────────────────────────┐
                │   Binance API (Live)     │
                └────────────┬─────────────┘
                             │
                    (Every Minute)
                             │
               ┌─────────────▼─────────────┐
               │  Minute-Level Ingestion   │
               │  (Raw CSV Storage)       │
               └─────────────┬─────────────┘
                             │
                    (Every Hour)
                             │
               ┌─────────────▼─────────────┐
               │  Hourly Aggregation       │
               │  (avg, min, max, etc.)   │
               └─────────────┬─────────────┘
                             │
                    (Daily)
                             │
               ┌─────────────▼─────────────┐
               │  Daily Aggregation        │
               │  (market trends)         │
               └─────────────┬─────────────┘
                             │
               ┌─────────────▼─────────────┐
               │ Historical Backfill DAG   │
               │ (Binance Klines API)      │
               └──────────────────────────┘
```

---

## ⚙️ Tech Stack

* **Orchestration:** Apache Airflow (v3+)
* **Language:** Python 3.11+
* **Data Processing:** Pandas
* **APIs:** Binance REST API
* **Storage:** Local file system (CSV-based data lake)
* **Scheduling:** Cron / Timedelta schedules
* **Environment:** Virtualenv / macOS / Linux

---

## 📂 Project Structure

```
airflow/
├── dags/
│   ├── 12_binance_fetch_minute.py
│   ├── 13_binance_calculate_hourly.py
│   ├── 14_binance_calculate_daily.py
│   └── 15_binance_backfill_last_month.py
│
├── data/
│   └── binance/
│       ├── raw/
│       ├── hourly/
│       └── daily/
│
├── logs/
├── plugins/
└── README.md
```

---

## 🔄 DAGs Explained

### 1️⃣ Minute-Level Ingestion DAG

**File:** `binance_fetch_minute.py`

* Fetches BTC price every minute
* Calls Binance Avg Price API
* Stores:

  * Individual minute CSV files
  * Daily aggregated raw file

📦 Output:

```
/data/binance/raw/YYYY-MM-DD/
    ├── price_HH_MM.csv
    └── daily_raw.csv
```

---

### 2️⃣ Hourly Aggregation DAG

**File:** `binance_calculate_hourly.py`

* Runs every hour
* Reads minute-level data
* Computes:

  * Average price
  * Min / Max
  * First / Last price
  * Data points count

📦 Output:

```
/data/binance/hourly/YYYY-MM-DD/hourly_avg.csv
```

---

### 3️⃣ Daily Aggregation DAG

**File:** `binance_calculate_daily.py`

* Runs once per day
* Aggregates hourly data
* Computes:

  * Daily average
  * Opening / Closing price
  * Price change (%)
  * Market volatility

📦 Output:

```
/data/binance/daily/daily_avg.csv
```

---

### 4️⃣ Backfill DAG (Historical ETL)

**File:** `binance_backfill_last_month.py`

* Fetches last 30 days using Binance Klines API
* Reconstructs:

  * Raw minute data
  * Hourly aggregates
  * Daily aggregates

📌 Use case:

* Cold start pipelines
* Data recovery
* Historical analysis

---

## 📊 Data Schema

### Raw Data (Minute Level)

| Column      | Description        |
| ----------- | ------------------ |
| price       | BTC price (string) |
| price_float | BTC price (float)  |
| timestamp   | ISO timestamp      |
| fetch_time  | Readable datetime  |
| closeTime   | Binance close time |

---

### Hourly Aggregation

| Column      | Description          |
| ----------- | -------------------- |
| avg_price   | Average hourly price |
| min_price   | Minimum price        |
| max_price   | Maximum price        |
| first_price | Opening price        |
| last_price  | Closing price        |
| data_points | Number of records    |

---

### Daily Aggregation

| Column           | Description        |
| ---------------- | ------------------ |
| avg_price        | Daily average      |
| opening_price    | First hourly price |
| closing_price    | Last hourly price  |
| price_change     | Absolute change    |
| price_change_pct | % change           |
| hours_with_data  | Active hours       |

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/binance-airflow-etl.git
cd binance-airflow-etl
```

---

### 2. Setup Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 3. Install Dependencies

```bash
pip install apache-airflow pandas requests
```

---

### 4. Initialize Airflow

```bash
airflow db migrate
```

---

### 5. Start Airflow

```bash
airflow standalone
```

Access UI:
👉 http://localhost:8080

---

## ▶️ Running the Pipelines

1. Enable DAGs:

   * `binance_fetch_minute`
   * `binance_calculate_hourly`
   * `binance_calculate_daily`

2. Trigger manually or wait for schedules.

3. (Optional) Run backfill:

```bash
Trigger DAG → binance_backfill_last_month
```

---

## ⚠️ Important Notes

* Ensure correct file permissions for `/data/binance/`
* Use absolute paths or update to local writable paths (e.g., `/Users/.../data`)
* API rate limits may apply
* Airflow 3 uses `schedule` instead of `schedule_interval`

---

## 🧠 Key Learnings

* Real-time + batch hybrid pipelines
* DAG orchestration best practices
* Data partitioning strategies
* Handling API failures & retries
* Idempotent ETL design
* Time-based aggregations

---

## 🌟 Future Improvements

* ☁️ Deploy on AWS (S3 + MWAA / EC2)
* 🐳 Dockerize pipeline
* 📊 Add dashboard (Streamlit / Superset)
* 🗄️ Replace CSV with Data Warehouse (BigQuery / Snowflake)
* 🔔 Add alerting (Slack / Email)
* ⚡ Use Kafka for streaming ingestion

---

## 📸 Sample Outputs

```
Hourly Avg:
2026-03-01 10 → $68,245

Daily Avg:
2026-03-01 → $68,100 (+1.2%)
```

---

## 👨‍💻 Author

**Subhankar Biswas**

* Aspiring Data Engineer | Cloud & AI Enthusiast
* Focus: Data Pipelines, ML Systems, Distributed Computing
