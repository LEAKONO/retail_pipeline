#  Modern Retail Analytics Platform

> A production-grade, end-to-end data engineering pipeline built on the **Online Retail II** dataset — simulating a real e-commerce analytics platform used by a UK-based international retailer.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DWH-29B5E8?style=flat&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?style=flat&logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboards-F2C811?style=flat&logo=powerbi&logoColor=black)

---

##  What This Project Does

Every day at **6am**, this pipeline automatically:

1. Reads new retail orders from a source CSV
2. Validates, cleans, and loads them into Snowflake
3. Transforms raw data into analytics-ready tables using dbt
4. Tests data quality across 34 automated checks
5. Serves business dashboards in Power BI

All without a human touching anything.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA                              │
│               Online Retail II CSV (1M+ records)               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Apache Airflow  │  ← Orchestrates everything
                    │  Daily 6am DAG  │     Retries on failure
                    └────────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │     Python ELT Pipeline      │
              │                             │
              │  extract.py                 │  ← Incremental loading
              │  ├── Watermark filtering    │     Only new records
              │  ├── Schema validation      │     each run
              │  └── Encoding fallback      │
              │                             │
              │  transform.py               │  ← Data quality
              │  ├── Type casting           │     Quarantine pattern
              │  ├── Deduplication          │     Audit columns
              │  └── Quality validation     │
              │                             │
              │  load.py                    │  ← Bulk loading
              │  ├── Snowflake Stage        │     COPY INTO
              │  ├── COPY INTO raw_orders   │     Idempotent writes
              │  └── Quarantine table       │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │        SNOWFLAKE            │
              │                             │
              │  RAW Layer                  │
              │  ├── raw_orders             │  ← 1,021,429 rows
              │  ├── raw_orders_quarantine  │  ← Rejected records
              │  ├── pipeline_watermark     │  ← Incremental state
              │  └── pipeline_audit         │  ← Run history
              │                             │
              │  STAGING Layer (dbt views)  │
              │  ├── stg_orders             │
              │  ├── stg_customers          │
              │  └── stg_products           │
              │                             │
              │  MARTS Layer (dbt tables)   │
              │  ├── fct_orders             │  ← Star schema center
              │  ├── dim_customers          │  ← Segmentation
              │  ├── dim_products           │  ← Product tiers
              │  └── dim_date              │  ← Calendar dimension
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │          POWER BI           │
              │                             │
              │  Page 1: Executive Summary  │
              │  ├── £18.93M Total Revenue  │
              │  ├── 5,942 Customers        │
              │  ├── 53,623 Orders          │
              │  └── Revenue Trend 2009-11  │
              │                             │
              │  Page 2: Customer & Geo     │
              │  ├── Customer Segments      │
              │  └── Revenue by Country     │
              └─────────────────────────────┘
```

---

##  Key Engineering Features

###  Incremental Loading with Dual Watermark
No full reloads. The pipeline remembers exactly where it stopped
> **Result:** 90%+ reduction in compute vs full loads as data grows.

###  Quarantine Pattern — Never Silently Drop Bad Data
```
raw_orders          ← clean records (4,950 rows)
raw_orders_quarantine ← rejected records with error_reason column
```
Every rejected record is stored with the exact reason it failed — `invoice_no is null; unit_price is negative;` — so data issues are always traceable and fixable.


###  Star Schema Data Model
```
          dim_customers          dim_date
                    
               │                     │
    customer_id│                     │date_id
               │                     │
dim_products ──┼─────── fct_orders ──┘
   │     
    stock_code │
```

---

##  Project Structure

```
retail_pipeline/
│
├── ingestion/
│   ├── extract.py    
│   ├── transform.py       
│   └── load.py           
│
├── core/
│   ├── config.py           
│   ├── database.py         
│   ├── logger.py           
│   └── exceptions.py       
├── metadata/
│   ├── watermark.py      
│   └── audit.py           
│
├── dbt_retail/
│   └── models/
│       ├── staging/
│       │   ├── stg_orders.sql
│       │   ├── stg_customers.sql
│       │   └── stg_products.sql
│       └── marts/
│           ├── fct_orders.sql
│           ├── dim_customers.sql
│           ├── dim_products.sql
│           └── dim_date.sql
│
├── airflow/
│   └── dags/
│       └── retail_pipeline_dag.py
│
├── main.py                 
├── requirements.txt
└── .env.example
```

---

##  Quick Start

### Prerequisites
- Python 3.10+
- Snowflake account ([free trial](https://signup.snowflake.com/))
- PostgreSQL (for Airflow metadata)
- Power BI Desktop (Windows only)

---

### Step 1 — Clone the Repository
```bash
git clone https://github.com/LEAKONO/retail_pipeline
cd retail_pipeline
```

### Step 2 — Set Up Python Environment
```bash
python3 -m venv venv
source venv/bin/activate          # Linux/Mac
pip install -r requirements.txt
```

### Step 3 — Configure Environment Variables
```bash
cp .env.example .env
nano .env
```

Your `.env` should look like this:
```env
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_USER=PIPELINE_USER
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=RETAIL_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=RETAIL_WH
SNOWFLAKE_ROLE=PIPELINE_ROLE
SOURCE_FILE_PATH=data/online_retail_II.csv
PIPELINE_NAME=online_retail
BATCH_SIZE=1000
LOG_LEVEL=INFO
```

### Step 4 — Set Up Snowflake
Run this in your Snowflake worksheet as `ACCOUNTADMIN`:
```sql
-- Create database and schemas
CREATE DATABASE IF NOT EXISTS RETAIL_DB;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.RAW;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.MARTS;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

-- Create role and user
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;
CREATE USER IF NOT EXISTS PIPELINE_USER
    PASSWORD         = 'your_secure_password'
    DEFAULT_ROLE     = PIPELINE_ROLE
    DEFAULT_WAREHOUSE = RETAIL_WH;

-- Grant permissions
GRANT ROLE PIPELINE_ROLE TO USER PIPELINE_USER;
GRANT ALL PRIVILEGES ON DATABASE RETAIL_DB TO ROLE PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE RETAIL_DB TO ROLE PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE RETAIL_DB TO ROLE PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE RETAIL_DB TO ROLE PIPELINE_ROLE;
```

### Step 5 — Download the Dataset
Download from Kaggle and place in the `data/` folder:
```
data/online_retail_II.csv
```
[📥 Download: Online Retail II — Kaggle](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)

### Step 6 — Run the Pipeline
```bash
python main.py
```

### Step 7 — Run dbt Transformations
```bash
cd dbt_retail
dbt debug          # Test connection
dbt run            # Build all models
dbt test           # Run 34 data quality tests
```

### Step 8 — Set Up Airflow
```bash
# Create Airflow virtual environment
python3 -m venv airflow_venv
source airflow_venv/bin/activate
pip install apache-airflow==2.9.1 psycopg2-binary

# Initialise database
export AIRFLOW_HOME=/path/to/retail_pipeline/airflow
airflow db migrate

# Create admin user
airflow users create \
    --username admin \
    --password admin123 \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &
```

Open `http://localhost:8080` → enable `retail_pipeline` DAG → watch it run.

---

##  Pipeline Results

| Metric | Value |
|--------|-------|
| Total records processed | 1,021,429 |
| Unique customers | 5,942 |
| Unique products | 5,299 |
| Total invoices | 53,623 |
| Total revenue | £18.93M |
| Pipeline execution time | ~13 seconds |
| dbt models | 7 |
| Data quality tests | 34 |
| Countries served | 37 |

---

## Business Insights From The Dashboard

```
💰 Revenue peaked November 2011 — clear Christmas season effect
🇬🇧 United Kingdom = 74% of total revenue
👥 63.63% of customers are first-time buyers (NEW segment)
⭐ VIP customers = 0.88% of base but disproportionate revenue
📦 Top product: WHITE HANGING HEART T-LIGHT HOLDER
```

---

## dbt Data Models

### Staging Layer (Views)
| Model | Description | Rows |
|-------|-------------|------|
| `stg_orders` | Cleaned order line items | 1,021,429 |
| `stg_customers` | One row per customer | 5,942 |
| `stg_products` | One row per product | 5,299 |

### Marts Layer (Tables)
| Model | Description | Key Feature |
|-------|-------------|-------------|
| `fct_orders` | Central fact table | MD5 surrogate key, net cancellation logic |
| `dim_customers` | Customer dimension | VIP/LOYAL/REGULAR/NEW segmentation |
| `dim_products` | Product dimension | BESTSELLER/POPULAR/REGULAR/SLOW_MOVER tiers |
| `dim_date` | Calendar dimension | 1,461 generated dates, is_weekend flag |

---

##  Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingestion | Python 3.10+ | ELT pipeline |
| Storage | Snowflake | Cloud data warehouse |
| Transformation | dbt | Data modeling |
| Orchestration | Apache Airflow | Scheduling & monitoring |
| Visualisation | Power BI | Business dashboards |
| Version Control | Git + GitHub | Source control |
| Environment | Linux (Ubuntu) | Development |

---

##  Design Decisions

**Why watermarking instead of full reload?**
Full reloads become exponentially slower as data grows. Watermarking keeps execution time constant regardless of historical data size.

**Why quarantine instead of deletion?**
Silent data deletion makes pipelines untrustworthy. Every rejected record is preserved with its rejection reason — giving data teams full visibility and the ability to reprocess fixed records.

**Why a star schema?**
Flat tables optimised for OLTP are slow for analytics. A star schema with pre-aggregated dimensions and a central fact table makes Power BI queries fast and intuitive.

**Why dbt?**
dbt brings software engineering practices (version control, testing, documentation) to SQL transformations. 34 automated tests catch data quality issues before they reach dashboards.

---

##  Author

**Emmanuel Leakono**
Data Engineer — Nairobi, Kenya

[![GitHub](https://img.shields.io/badge/GitHub-LEAKONO-181717?style=flat&logo=github)](https://github.com/LEAKONO)
[![Email](https://img.shields.io/badge/Email-leakonoemmanuel3@gmail.com-D14836?style=flat&logo=gmail&logoColor=white)](mailto:leakonoemmanuel3@gmail.com)

---

##  License

MIT License — feel free to use this project as a reference for your own data engineering work.

---

*Built with curiosity, and a genuine love for data engineering.*