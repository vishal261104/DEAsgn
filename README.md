# FreshCart Data & ML Pipelines: From Git to Gold

[![Python CI](https://github.com/vishal261104/DEAsgn/actions/workflows/ci.yml/badge.svg)](https://github.com/vishal261104/DEAsgn/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Cloud-29B5E8?logo=snowflake&logoColor=white)
![Mage AI](https://img.shields.io/badge/Orchestration-Mage%20AI-purple.svg)
![Scikit-Learn](https://img.shields.io/badge/ML-Scikit--Learn-orange.svg?logo=scikit-learn)
![RabbitMQ](https://img.shields.io/badge/Streaming-RabbitMQ%20%7C%20Pika-FF6600?logo=rabbitmq&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

## 📖 Executive Summary

**FreshCart Pipelines** is an enterprise-grade Data Engineering and Machine Learning project structured around the end-to-end lifecycle **"From Git to Gold"**. The repository brings together:

1. **A Snowflake Medallion ELT Pipeline**: Demonstrating raw semi-structured JSON ingestion, schema reconciliation, quarantine error handling, deduplication, Slowly Changing Dimensions Type 2 (SCD2), Foreign Exchange currency normalization with gap-filling, geographic enrichment, and idempotent Gold-layer publishing.
2. **A Production ML Pipeline (Mage AI)**: Implementing a DAG-based machine learning workflow for the Iris classification dataset with data loaders, transformers, unit tests, model training, evaluation, and serialized batch inference export.
3. **Event Streaming Infrastructure**: Prepared for RabbitMQ message dispatching and real-time streaming ingestion.
4. **Automated CI/CD**: Fully tested with GitHub Actions running continuous integration against Python 3.12.

---

## 🏗️ Architecture Overview

### 1. Snowflake Medallion ELT Architecture

```
                                [ Web Orders (Flat JSON) ]
                                [ Mobile Orders (Nested JSON) ]
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │   Extract & Landing (GZIP)      │
                           └─────────────────────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │      BRONZE: Raw Storage        │
                           │       bronze.orders_raw         │
                           └─────────────────────────────────┘
                                            │
                      ┌─────────────────────┴─────────────────────┐
                      ▼                                           ▼
         ┌─────────────────────────┐                 ┌─────────────────────────┐
         │  silver.orders_checked  │                 │    quarantine.orders    │
         │  (Schema validation)    │                 │ (Corrupt/invalid rows)  │
         └─────────────────────────┘                 └─────────────────────────┘
                      │
                      ▼
         ┌─────────────────────────┐
         │   silver.orders_clean   │
         └─────────────────────────┘
                      │
                      ▼
         ┌─────────────────────────┐
         │   silver.orders_dedup   │ ◄── Window Dedup (Latest updated_at)
         └─────────────────────────┘
                      │
                      ├──────────────────────────┬──────────────────────────┐
                      ▼                          ▼                          ▼
         ┌─────────────────────────┐┌─────────────────────────┐┌─────────────────────────┐
         │    dim.customers_scd2   ││      ref.fx_daily       ││     ref.pincode_geo     │
         │ (Customer Tier History) ││ (Forward-filled to INR) ││ (District, State, Region)│
         └─────────────────────────┘└─────────────────────────┘└─────────────────────────┘
                      │                          │                          │
                      └──────────────────────────┼──────────────────────────┘
                                                 │
                                                 ▼
                               ┌───────────────────────────────────┐
                               │       silver.orders_enriched      │
                               │   (Point-in-time joins, INR conv) │
                               └───────────────────────────────────┘
                                                 │
                                                 ▼
                               ┌───────────────────────────────────┐
                               │          GOLD FACT TABLE          │
                               │          gold.fct_orders          │
                               │     (Idempotent MERGE by hash)    │
                               └───────────────────────────────────┘
```

### 2. Mage ML Pipeline DAG (`iris_classifier`)

```
   ┌────────────────────────────────┐
   │    data_loader: load_iris      │ ──► Verified with @test (150 rows)
   └────────────────────────────────┘
                   │
                   ▼
   ┌────────────────────────────────┐
   │  transformer: prepare_data     │ ──► 80/20 Train/Test split
   └────────────────────────────────┘
                   │
                   ▼
   ┌────────────────────────────────┐
   │    transformer: train_model    │ ──► DecisionTreeClassifier (max_depth=3)
   └────────────────────────────────┘     Asserts accuracy >= 80% -> iris_model.pkl
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
┌──────────────────┐  ┌───────────────────────────────┐
│ evaluate_model   │  │  data_exporter: save_results  │
│ (Confusion Mtx,  │  │  (iris_predictions.csv)       │
│  P/R/F1-Score)   │  └───────────────────────────────┘
└──────────────────┘
```

---

## 📁 Repository Structure

```plaintext
freshcart-pipelines-c1/
│
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI workflow (Python 3.12)
│
├── elt/
│   ├── .gitkeep
│   └── freshcart_elt.ipynb            # Snowflake Medallion ELT notebook (Bronze -> Silver -> Gold)
│
├── iris_project/                      # Mage AI Orchestration Project
│   ├── __init__.py
│   ├── .gitkeep
│   ├── Mage_ML_Pipeline.ipynb         # Interactive notebook demonstrating Mage setup & execution
│   ├── data_loaders/
│   │   └── load_iris.py               # Data loader with row-count validation test
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── prepare_data.py            # Train/test split transformer with quality tests
│   │   ├── train_model.py             # Model training transformer & decision tree persistence
│   │   └── evaluate_model.py          # Metric computation (Accuracy, Precision, Recall, Macro-F1)
│   ├── data_exporters/
│   │   └── save_results.py            # Batch inference CSV exporter
│   └── pipelines/
│       └── iris_classifier/
│           └── metadata.yaml          # Mage pipeline DAG dependency configuration
│
├── stream/
│   └── .gitkeep                       # Streaming & RabbitMQ dispatcher assets
│
├── tests/
│   └── test_prepare_data.py           # Automated unit test suite
│
├── .gitignore                         # Git exclusion rules
├── requirements.txt                   # Production & testing dependencies
└── README.md                          # Comprehensive project documentation
```

---

## 🔧 Core Pipeline Details

### 1. Snowflake ELT: Medallion Architecture (`elt/freshcart_elt.ipynb`)

The ELT pipeline implements standard data warehousing patterns:

- **Multi-Source Ingestion**: Ingests flat JSON orders from web channels and nested JSON payloads from mobile applications into a unified Bronze landing table (`bronze.orders_raw`).
- **Data Cleansing & Quarantine**: Validates incoming attributes (RFC email standards, non-negative order amounts, recognized payment statuses). Invalid records are isolated into `quarantine.orders` with explicit reason arrays without halting ingestion.
- **Deduplication Engine**: Uses Snowflake window functions (`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, ingest_seq DESC) = 1`) to guarantee deterministic deduplication.
- **Slowly Changing Dimensions (SCD2)**: Tracks historical customer tier modifications (`dim.customers_scd2`) with non-overlapping `valid_from` and `valid_to` timestamps.
- **Foreign Exchange Normalization**: Flattens nested daily currency exchange rates in `ref.fx_daily`, with backward/forward fills across weekends and bank holidays, converting all order transactions to INR.
- **Geographic Enrichment**: Joins postal pincodes (`ref.pincode_geo`) to districts, states, and sales regions.
- **Idempotent Gold Publishing**: Enriches transactions into `silver.orders_enriched` and merges into `gold.fct_orders` using deterministic SHA-256 row hashing (`row_hash`) to avoid duplicate records on re-runs.
- **Data Reconciliation Checks**: Built-in automated SQL assertions verify zero orphaned records, zero loss between deduplication and enrichment, and zero duplicate entries.

### 2. Mage ML Pipeline (`iris_project/`)

An orchestration pipeline leveraging Mage AI design principles:

- **Decoupled Block Architecture**: Modular blocks for loading, transforming, training, and exporting.
- **Built-in Quality Assertions (`@test`)**: Every step executes validation hooks before passing data to downstream blocks:
  - Dataset size check (150 samples).
  - Feature integrity and split tag presence.
  - Model accuracy threshold assertion ($\ge 80\%$).
- **Artifact Management**:
  - Model persistence via `joblib` (`iris_model.pkl`).
  - Predictions exported to `iris_predictions.csv`.
- **DAG Definition**: Configured through `metadata.yaml` with dynamic parameterization support (e.g., `max_depth`).

### 3. Event Streaming (`stream/`)

- Designed for message queue ingestion with RabbitMQ and the Python `pika` library.
- Decouples real-time order generation from batch/micro-batch processing workloads.

---

## 🚀 Getting Started & Local Setup

### Prerequisites

- **Python 3.12+**
- **Git**
- Optional: Active **Snowflake** account (for running the ELT notebook)
- Optional: **RabbitMQ** service (for streaming consumers/producers)

### 1. Clone & Set Up Virtual Environment

```bash
# Clone the repository
git clone https://github.com/vishal261104/DEAsgn.git
cd DEAsgn

# Create and activate a Python virtual environment
python -m venv .venv

# On Windows (PowerShell):
.venv\Scripts\Activate.ps1
# On Windows (CMD):
.venv\Scripts\activate.bat
# On macOS / Linux:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Run Automated Tests

To execute the test suite:

```bash
pytest -q
```

---

## 📊 Running the Pipelines

### Running Snowflake ELT (`elt/freshcart_elt.ipynb`)

1. Launch Jupyter Notebook or JupyterLab:
   ```bash
   pip install jupyter
   jupyter lab
   ```
2. Open `elt/freshcart_elt.ipynb`.
3. Provide your Snowflake connection details:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - Password (prompted securely at runtime via `getpass`)
4. Run all cells to execute extraction, staging, transformation, and verification.
5. **Expected Gold Output**:
   - Aggregated Revenue: `₹7,785.00`
   - Total Orders Processed: `4`
   - Loyalty Points Accumulated: `126`

### Running the ML Pipeline (`iris_project/`)

You can run the pipeline directly via Mage AI or interactively via the notebook:

```bash
# Optional: Install Mage AI
pip install mage-ai

# Start Mage server inside the project root
mage start iris_project
```

Access the Mage user interface at `http://localhost:6789` to monitor block execution and DAG runs. Alternatively, explore `iris_project/Mage_ML_Pipeline.ipynb`.

---

## 🛡️ Engineering Highlights & Best Practices

| Capability | Implementation | Benefit |
| :--- | :--- | :--- |
| **Idempotency** | Surrogate hashing (`row_hash`) & `MERGE` statements | Re-running pipelines produces deterministic results without duplication |
| **Data Quality Gates** | In-pipeline `@test` decorators & SQL assertions | Prevents pipeline regression and bad data propagation |
| **Quarantine Pattern** | Route rejected rows to `quarantine.orders` with reason arrays | Prevents ingestion job failures while providing an audit trail for data cleanup |
| **Point-in-Time Accuracy** | SCD Type 2 dimension mapping | Accurate historical backfills respecting historical state changes |
| **Modular CI/CD** | GitHub Actions workflow validating Python 3.12 | Automated verification on every push and pull request |

---

## 👤 Author & Contribution

- **Author**: Vishal ([@vishal261104](https://github.com/vishal261104))
- **Repository**: [https://github.com/vishal261104/DEAsgn](https://github.com/vishal261104/DEAsgn)
- **Coursework**: Data Engineering assignment: *From Git to Gold*
