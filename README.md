
# 🚀 Hybrid Data Pipeline (Midterm Project)

## 📖 Overview
This project implements a **Hybrid Data Pipeline** for processing e-commerce orders using an **ELT (Extract, Load, Transform)** pattern.
The system automatically selects the processing engine based on file size:
- **Python Batch (Streaming):** for small files (<= 200 MB).
- **Apache Spark (Parallel):** for large files (> 200 MB).

The pipeline ingests dirty CSV data into a MongoDB `Raw` layer, applies 9 data quality rules, and routes records to either `Validated` (with Audit Trail) or `Quarantine` collections, ensuring strict **Idempotency** via Upserts.

## 🏗️ Architecture

Dirty CSV --> File Router --> [Python Batch | PySpark] --> orders_raw (MongoDB)
                                                              |
                                                              v
                                                    Quality Rules Engine
                                                      /             \
                                                     v               v
                                           orders_validated    orders_quarantine
                                           (Upsert + Audit)    (Error Codes)


## ⚙️ Installation
1. Ensure Python 3.10+ and Java 17+ are installed.
2. Ensure MongoDB is running locally on `mongodb://localhost:27017`.
3. Create and activate a virtual environment:
   python -m venv .venv
   .venv\Scripts\activate   # On Windows
  
4. Install dependencies:

   pip install -r requirements.txt

## 🚀 Usage
The project has a **single entry point** (`src/main.py`) that handles everything automatically.

### 1. Setup MongoDB Collections
python src/mongo_setup.py

### 2. Extract a Small Sample (for testing)
python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv --rows 100000


### 3. Run the Pipeline
python src/main.py
*The File Router automatically detects the file size and chooses between Python Batch or PySpark.*

### 4. Run Unit Tests
pytest tests/ -v

## 🔌 MongoDB Spark Connector (Spark 4.0 Compatibility)
The pipeline writes raw data in parallel using **MongoDB Spark Connector `10.5.0` (Scala 2.13)** — the correct artifact for PySpark 4.0.
- `_2.12:10.2.1` → ❌ `NoSuchMethodError: RowEncoder$.apply` (built for Spark 3.x)
- `_2.12:10.4.0` → ❌ `NoSuchMethodError: resolveAndBind` (Scala mismatch)
- `_2.13:10.5.0` → ✅ Parallel write succeeded (500K records @ 16,557 rec/s)

A defensive fallback (Micro-batching via `toLocalIterator` + `pymongo`) remains built in for restricted environments; the effective `write_mode` is logged per run.

## 🌟 Advanced Path B: Incremental Loading (Group Requirement)
Instead of reprocessing the entire dataset, an **Incremental Loader** processes only new/modified records via Delta Files:
1. Ensure your Delta CSV exists at `data/delta_updates.csv`.
2. Run:
 
   python run_delta.py
3. **Idempotency Proof:** running the same Delta twice yields `Inserted: 0` with no duplicates in `orders_validated`.

## 🧪 Testing Idempotency
1. Run `python src/main.py` (first run: inserts records).
2. Run it again (second run: `Upserted: 0`, records updated in place).

## 📂 Project Structure

midterm-data-pipeline/
├── config/settings.py         # Centralized configurations
├── data/
│   └── .gitkeep
├── src/
│   ├── main.py                # Single entry point (Router)
│   ├── file_router.py         # Engine selection logic
│   ├── create_small_sample.py # Sample extraction script
│   ├── batch_loader.py        # Python streaming loader
│   ├── spark_loader.py        # PySpark parallel loader (Connector + fallback)
│   ├── incremental_loader.py  # Path B: Incremental Loading Engine
│   ├── quality_rules.py       # 9 Data cleaning rules + Audit Trail
│   ├── elt_pipeline.py        # Classification & Idempotent Upsert
│   ├── mongo_setup.py         # Collections + Schema Validation setup
│   └── metrics.py             # JSON reporting
├── tests/
│   ├── test_cleaning_rules.py
│   └── test_classification.py
├── run_delta.py               # Triggers Incremental Loading
├── reports/                   # results.json, results.md & screenshots/
├── docs/architecture.md       # Architecture documentation
└── README.md

## 📸 Screenshots
Visual proofs in `reports/screenshots/`:
- `01_main_run_success.png` — Main run (500K records, consistency check passed)
- `02_pytest_passed.png` — Unit tests (10 passed)
- `03_delta_idempotency1.png` / `03_delta_idempotency2.png` — Delta rerun with `Inserted: 0`
- `04_mongodb_validated.png` — `orders_validated` with Audit Trail (`_corrections`)
- `05_mongodb_quarantine.png` — `orders_quarantine` with `error_codes`
- `06_spark_ui_jobs.png` — Spark UI Jobs
- `07_spark_ui_stages.png` — Spark UI Stages (50/50 tasks = repartition effect)

