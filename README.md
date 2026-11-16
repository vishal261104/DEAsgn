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

**FreshCart Pipelines** is an enterprise-grade Data Engineering and Machine Learning project structured around the end-to-end lifecycle **"From Git to Gold"**. The repository unifies three production-grade systems under a single, CI-verified codebase:

| Pillar | Technology | What it does |
| :--- | :--- | :--- |
| **Snowflake Medallion ELT** | Python · SQL · Snowflake | Bronze → Silver → Gold data lakehouse with quarantine, SCD2, FX normalisation, and idempotent publishing |
| **Mage AI ML Pipeline** | scikit-learn · Mage AI · joblib | Multi-model Iris classification DAG with input validation, Decision Tree & KNN, evaluation, and batch export |
| **RabbitMQ Event Streaming** | Pika · RabbitMQ | Topic-exchange dispatcher + consumer driver simulating real-time ride/food dispatch routing |

Continuous integration via **GitHub Actions** (Python 3.12) validates every push and pull request automatically.

---

## 🏗️ Architecture Overview

### 1. Snowflake Medallion ELT

```
   ┌──────────────────────────┐   ┌──────────────────────────┐
   │  Web Orders (flat JSON)  │   │ Mobile Orders (nested JSON)│
   └────────────┬─────────────┘   └────────────┬──────────────┘
                │                              │
                └──────────────┬───────────────┘
                               ▼
                 ┌─────────────────────────────┐
                 │   EXTRACT & LAND (GZIP)     │  PUT → Snowflake stage
                 └─────────────────────────────┘
                               ▼
                 ┌─────────────────────────────┐
                 │   BRONZE: bronze.orders_raw  │  Raw JSON, append-only
                 └──────────────┬──────────────┘
                                │
           ┌────────────────────┴────────────────────┐
           ▼                                         ▼
 ┌──────────────────────┐                ┌────────────────────────┐
 │ silver.orders_checked│                │   quarantine.orders    │
 │  (validation logic)  │                │  (bad rows + reasons)  │
 └──────────┬───────────┘                └────────────────────────┘
            ▼
 ┌──────────────────────┐
 │  silver.orders_clean │
 └──────────┬───────────┘
            ▼
 ┌──────────────────────┐
 │  silver.orders_dedup │  ← QUALIFY window dedup (latest updated_at)
 └────┬─────────────────┘
      │
      ├──────────────────────┬──────────────────────┐
      ▼                      ▼                      ▼
 ┌──────────────┐   ┌──────────────────┐   ┌──────────────────┐
 │dim.customers │   │  ref.fx_daily    │   │ ref.pincode_geo  │
 │_scd2 (SCD2)  │   │ (gap-filled INR) │   │(district/region) │
 └──────┬───────┘   └────────┬─────────┘   └────────┬─────────┘
        └───────────────────┬┘                       │
                            └───────────────────────┬┘
                                                    ▼
                              ┌───────────────────────────────────┐
                              │      silver.orders_enriched       │
                              │ (point-in-time joins, INR values) │
                              └──────────────────┬────────────────┘
                                                 ▼
                              ┌───────────────────────────────────┐
                              │         gold.fct_orders           │
                              │  Idempotent MERGE via row_hash    │
                              └───────────────────────────────────┘
```

### 2. Mage AI ML Pipeline DAG (`iris_classifier`)

```
         ┌──────────────────────────────────────┐
         │      data_loader: load_iris          │
         │  Loads 150-row Iris DataFrame        │
         │  @test: assert 150 rows             │
         └───────────────────┬──────────────────┘
                             │
             ┌───────────────┴───────────────┐
             ▼                               ▼
 ┌───────────────────────┐    ┌───────────────────────────┐
 │transformer:prepare_data│    │transformer:validate_iris  │
 │ 80/20 train/test split │    │ Null check + 3-class check│
 │ adds 'split' column    │    │ @test: clean DataFrame    │
 └──────────┬────────────┘    └───────────────────────────┘
            │
    ┌───────┴────────┐
    ▼                ▼
 ┌────────────────┐  ┌──────────────────────────┐
 │transformer:    │  │transformer: train_knn     │
 │train_model     │  │ KNeighboursClassifier k=5 │
 │DecisionTree    │  │ @test: accuracy >= 80%    │
 │max_depth=3     │  └──────────────────────────┘
 │saves .pkl      │
 │@test: >=80%    │
 └───────┬────────┘
         │
   ┌─────┴─────┐
   ▼           ▼
 ┌──────────┐  ┌────────────────────────────────┐
 │evaluate_ │  │data_exporter: save_results     │
 │model     │  │ Writes iris_predictions.csv    │
 │Confusion │  └────────────────────────────────┘
 │P/R/F1    │
 └──────────┘
```

### 3. RabbitMQ Event Streaming

```
                      ┌──────────────────────┐
                      │  stream/dispatcher.py │
                      │  Publishes 60 events  │
                      │  every 2 s            │
                      └──────────┬───────────┘
                                 │  topic exchange: rapido-dispatch
                    ┌────────────┼─────────────────────────┐
                    │            │                         │
         <city>.<zone>.<type>  <city>.<type>        <city>.<zone>.<type>
         (normal order)        (partial / no zone)  (normal order)
                    │            │                         │
                    ▼            ▼                         ▼
         ┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
         │ driver.py    │  │  driver.py        │  │ driver.py    │
         │ ACCEPT ✓     │  │  REJECT ✗ nack    │  │ ACCEPT ✓     │
         │ (has zone)   │  │  (no zone field)  │  │ (has zone)   │
         └──────────────┘  └──────────────────┘  └──────────────┘
```

---

## 📁 Repository Structure

```plaintext
freshcart-pipelines/
│
├── .github/
│   └── workflows/
│       └── ci.yml                        # GitHub Actions CI — Python 3.12, pytest, stream import check
│
├── elt/
│   ├── .gitkeep
│   └── freshcart_elt.ipynb               # Full Snowflake Medallion ELT notebook (Bronze → Silver → Gold)
│
├── iris_project/                         # Mage AI ML orchestration project
│   ├── __init__.py                       # Package init (v0.2.0)
│   ├── .gitkeep
│   ├── Mage_ML_Pipeline.ipynb            # Interactive walkthrough notebook
│   ├── data_loaders/
│   │   └── load_iris.py                  # Loads Iris dataset; @test asserts 150 rows
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── prepare_data.py               # 80/20 train/test split; @test checks 'split' column
│   │   ├── validate_iris.py              # 🆕 Null + class-count guard before split/train
│   │   ├── train_model.py                # DecisionTreeClassifier (max_depth=3); saves iris_model.pkl
│   │   ├── train_knn.py                  # 🆕 KNeighboursClassifier (k=5, configurable); @test >= 80%
│   │   └── evaluate_model.py             # Accuracy, Precision, Recall, Macro-F1; confusion matrix
│   ├── data_exporters/
│   │   └── save_results.py               # Writes iris_predictions.csv
│   └── pipelines/
│       └── iris_classifier/
│           └── metadata.yaml             # Mage DAG: full 7-block dependency graph
│
├── stream/                               # 🆕 RabbitMQ event streaming module
│   ├── __init__.py                       # Package init (v0.1.0)
│   ├── dispatcher.py                     # Topic-exchange publisher — 60 ride/food dispatch events
│   └── driver.py                         # Consumer driver — accepts/nacks based on routing key
│
├── tests/
│   └── test_prepare_data.py              # Pytest: verifies 80/20 split produces 120/30 rows
│
├── .gitignore
├── requirements.txt                      # pandas, scikit-learn, pytest, pika
└── README.md
```

---

## 🔧 Core Component Details

### 1. Snowflake ELT (`elt/freshcart_elt.ipynb`)

| Stage | Table | Key Logic |
| :--- | :--- | :--- |
| Extract | (stage files) | GZIP-compressed JSON; `source`, `run_id`, `extracted_at` metadata injected |
| Load | `bronze.orders_raw` | `PUT` + `COPY INTO`; append-only; retains all historical loads |
| Cleanse | `silver.orders_checked` | Unified schema across web (flat) and mobile (nested) APIs; RFC email, non-negative amounts, valid status |
| Quarantine | `quarantine.orders` | Invalid rows isolated with structured `reasons` array; pipeline continues |
| Dedup | `silver.orders_dedup` | `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, ingest_seq DESC) = 1` |
| Dimension | `dim.customers_scd2` | SCD Type 2 — non-overlapping `valid_from`/`valid_to` date ranges per customer tier change |
| Reference | `ref.fx_daily` | Daily FX rates flattened from nested JSON; weekend/holiday gaps filled by forward-fill |
| Reference | `ref.pincode_geo` | `MERGE` upsert from India Post lookup; district, state, sales region |
| Enrich | `silver.orders_enriched` | Point-in-time joins to SCD2 and FX; LEFT JOIN ensures no order lost |
| Publish | `gold.fct_orders` | Idempotent `MERGE ON order_id` with SHA-256 `row_hash` guard; re-runnable |
| Quality | (inline SQL) | Automated checks: row-count preservation, zero duplicates, zero orphaned orders |

**Expected final output:** `4 orders · ₹7,785 revenue · 126 loyalty points`

---

### 2. Mage AI ML Pipeline (`iris_project/`)

#### Block inventory

| Block | Type | Description |
| :--- | :--- | :--- |
| `load_iris` | data_loader | Loads 150-row Iris DataFrame from scikit-learn; asserts row count |
| `validate_iris` | transformer | Upstream quality gate — no nulls, exactly 3 target classes |
| `prepare_data` | transformer | Stratified 80/20 split with deterministic `random_state=42`; labels rows with `'split'` column |
| `train_model` | transformer | `DecisionTreeClassifier(max_depth=3)`; asserts ≥ 80 % test accuracy; persists to `iris_model.pkl` |
| `train_knn` | transformer | `KNeighborsClassifier(n_neighbors=5)` running in parallel to Decision Tree; same accuracy gate |
| `evaluate_model` | transformer | Confusion matrix + Accuracy, Precision, Recall, Macro-F1 Score |
| `save_results` | data_exporter | Writes Decision Tree predictions to `iris_predictions.csv` |

All blocks use a **`try/except ImportError` decorator shim** so they execute correctly both inside the Mage server and standalone via `python -m pytest`.

---

### 3. RabbitMQ Streaming (`stream/`)

#### `dispatcher.py`

Publishes **60 randomised order events** to the `rapido-dispatch` topic exchange at a configurable cadence (default: 2 s / message).

| Config var | Default | Description |
| :--- | :--- | :--- |
| `AMQP_HOST` | `localhost` | RabbitMQ broker hostname |
| `DISPATCH_COUNT` | `60` | Total messages to publish |
| `DISPATCH_INTERVAL` | `2.0` | Seconds between publishes |

Routing key convention:
- **Normal** (90 % of traffic): `<city>.<zone>.<type>` — e.g. `bangalore.koramangala.ride`
- **Partial** (every 10th message): `<city>.<type>` — e.g. `chennai.food` ← triggers driver rejection

#### `driver.py`

A topic-pattern consumer simulating a delivery driver:

- Subscribes to any AMQP topic pattern supplied as a CLI argument.
- **Accepts** messages that contain a `zone` field (`basic_ack`).
- **Rejects** zone-less or malformed messages (`basic_nack`, no requeue).
- Sets `prefetch_count=1` for fair dispatch across concurrent driver processes.
- Shuts down gracefully on `Ctrl+C`.

```bash
# Start a driver in bangalore accepting all order types
python -m stream.driver "Ravi" "bangalore.#"

# Start a driver for a specific city/zone/type combination
python -m stream.driver "Priya" "chennai.velachery.food"
```

---

## 🚀 Getting Started

### Prerequisites

| Requirement | Notes |
| :--- | :--- |
| Python 3.12+ | Required |
| Git | Required |
| Snowflake account | Optional — for the ELT notebook only |
| RabbitMQ (local or Docker) | Optional — for the stream module only |

### Setup

```bash
# 1. Clone
git clone https://github.com/vishal261104/DEAsgn.git
cd DEAsgn

# 2. Virtual environment
python -m venv .venv
# Windows (PowerShell):  .venv\Scripts\Activate.ps1
# macOS / Linux:         source .venv/bin/activate

# 3. Dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# 4. Run tests
python -m pytest -q
```

### Run the ML Pipeline (Mage AI)

```bash
pip install mage-ai
mage start iris_project
# Open http://localhost:6789 → iris_classifier → Run pipeline
```

### Run the Streaming Demo (RabbitMQ required)

```bash
# Terminal 1 — dispatcher
python -m stream.dispatcher

# Terminal 2 — driver A (all bangalore)
python -m stream.driver "Ravi" "bangalore.#"

# Terminal 3 — driver B (specific city/zone/type)
python -m stream.driver "Priya" "chennai.velachery.food"
```

### Run the ELT Notebook (Snowflake required)

```bash
pip install jupyter
jupyter lab
# Open elt/freshcart_elt.ipynb → provide Snowflake credentials → Run All
```

---

## 🛡️ Engineering Highlights

| Practice | Implementation | Benefit |
| :--- | :--- | :--- |
| **Idempotency** | `row_hash` SHA-256 + SQL `MERGE` in Gold layer | Pipeline re-runs never produce duplicates |
| **Quarantine pattern** | Invalid rows routed to `quarantine.orders` with reason arrays | Zero pipeline failures on bad input; full audit trail |
| **SCD Type 2** | `valid_from` / `valid_to` dimension history | Point-in-time accurate joins for historical analytics |
| **FX gap-fill** | Forward-fill across weekends/holidays in `ref.fx_daily` | Every order date finds exactly one exchange rate |
| **Quality gates** | `@test` decorators on every Mage block + SQL reconciliation checks | Bugs surface at the block level before they propagate |
| **Parallel models** | Decision Tree + KNN run concurrently in the DAG | Side-by-side model comparison without blocking the main path |
| **Upstream validation** | `validate_iris` runs before split/train stages | Prevents training on corrupted or incomplete datasets |
| **Fair message dispatch** | `basic_qos(prefetch_count=1)` in RabbitMQ driver | Even load distribution across multiple concurrent consumers |
| **Graceful shutdown** | `KeyboardInterrupt` caught in driver consumer loop | No dangling connections or unacked messages on exit |
| **CI/CD** | GitHub Actions — pytest + stream import check on every push | Every commit is verified before it reaches `main` |

---

## 👤 Author

- **Vishal** · [@vishal261104](https://github.com/vishal261104)
- **Repository**: [https://github.com/vishal261104/DEAsgn](https://github.com/vishal261104/DEAsgn)
- **Coursework**: Data Engineering Assignment — *From Git to Gold*
