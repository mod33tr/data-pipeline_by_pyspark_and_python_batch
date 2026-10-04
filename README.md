# ?? Hybrid Data Pipeline & Analytics API (Final Project)

## ?? Overview
This project implements a complete **Big Data & Analytics Platform** for e-commerce orders. 
- **Phase 1 (Midterm):** A Hybrid ELT Pipeline that ingests dirty CSV data using Python Batch (small files) or Apache Spark (large files), applies 9 quality rules, and routes records to MongoDB (`Raw`, `Validated`, `Quarantine`) with strict Idempotency and Incremental Delta Loading.
- **Phase 2 (Final):** An Analytics & API Layer that adds Compound Indexes with `Explain` proofs, Aggregation Reports, Incremental Materialized Views, Scheduled Jobs, and a unified FastAPI interface to operate and test the entire system.

## ?? Team & Work Division

| Member | Ownership (Phase 1 + Phase 2) | Key Files |
|--------|-------------------------------|-----------|
| **Mohameed Hizam** | PySpark Engine, File Router, Metrics, Path B (Delta), **Indexes & Explain, FastAPI** | `spark_loader.py`, `file_router.py`, `metrics.py`, `incremental_loader.py`, `indexes.py`, `queries.py`, `api.py` |
| **Mohameed Adnan** | Python Batch, Quality Rules, ELT Pipeline, MongoDB Setup, **Aggregations, Materialized Views, Scheduler** | `batch_loader.py`, `quality_rules.py`, `elt_pipeline.py`, `mongo_setup.py`, `aggregations.py`, `materialized_views.py`, `scheduler.py` |

> Both members understand the full pipeline end-to-end and can explain any component.

## ??? Architecture

```text
Dirty CSV --> File Router --> [Python Batch | PySpark] --> orders_raw (MongoDB)
                                                              |
                                                              v
                                                    Quality Rules Engine
                                                      /             \
                                                     v               v
                                           orders_validated    orders_quarantine
                                           (Upsert + Audit)    (Error Codes)
                                                              |
                      +---------------------------------------+
                      | (Phase 2: Analytics & API Layer)
                      v
        [Indexes] -> [Aggregations] -> [Materialized Views] -> [FastAPI]
```

## ?? Installation
1. Ensure Python 3.10+, Java 17+, and MongoDB are installed and running (`mongodb://localhost:27017`).
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # On Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. (Optional) Copy `.env.example` to `.env` to customize settings (MongoDB URI, thresholds, job intervals).

---

## ?? Usage: Phase 1 (Data Ingestion & ELT)

### 1. Setup MongoDB Collections
```bash
python src/mongo_setup.py
```

### 2. Run the Hybrid Pipeline
The File Router automatically chooses the engine based on file size (Threshold: 200 MB).
```bash
python src/main.py                     # Default: small sample
python src/main.py --file huge         # Full 30M records via PySpark
python src/main.py --engine python_batch  # Force Batch engine for testing
```

### 3. Path B: Incremental Delta Loading
Process only new/modified records without reprocessing the whole dataset:
```bash
python run_delta.py
```

---

## ?? Usage: Phase 2 (Analytics, Indexes & Jobs)

### 1. Indexes & Explain Analysis
Generates 3 indexes (including Compound) and creates a Before/After `Explain` report proving the performance jump (COLLSCAN -> IXSCAN).
```bash
python src/indexes.py
# Output saved to: reports/explain_report.md
```

### 2. Aggregation Reports
Runs 5 independent analytical reports (Sales by City, Top Customers, etc.) dynamically without hardcoding values.
```bash
python src/aggregations.py
```

### 3. Materialized Views (Incremental Refresh)
Builds summary views once, then refreshes them incrementally using a `_processed_at` watermark index.
```bash
python src/materialized_views.py build    # Full initial build
python src/materialized_views.py refresh  # Incremental update (only affected keys)
```

### 4. Scheduled Jobs
Runs background tasks (MV refresh & Quality Reports) and logs execution (start/end/status) to MongoDB `job_logs`.
```bash
python src/scheduler.py --run refresh_materialized_views  # Manual trigger
python src/scheduler.py --run daily_quality_report        # Manual trigger
python src/scheduler.py --logs                            # View execution history
python src/scheduler.py                                   # Start background scheduler
```

---

## ?? Unified API (FastAPI)
A unified interface to operate and test all project functions without building a separate backend.

### Start the API Server:
```bash
uvicorn src.api:app --host 127.0.0.1 --port 8000 --reload
```

### Interactive Swagger UI:
Open your browser and visit: **http://127.0.0.1:8000/docs**

**Available Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/health` | Health check |
| `POST` | `/ingest` | Triggers the exact same ELT Pipeline from Phase 1 |
| `POST` | `/indexes` | Creates indexes and generates Explain report |
| `GET`  | `/queries` | List available named queries |
| `GET`  | `/queries/{name}` | Execute a specific query |
| `GET`  | `/aggregations` | List available aggregation reports |
| `GET`  | `/aggregations/{name}` | Execute a specific aggregation |
| `POST` | `/refresh-mv` | Trigger incremental MV refresh |
| `GET`  | `/jobs` | List scheduled jobs and recent logs |
| `POST` | `/jobs/{name}/run` | Manually trigger a scheduled job |

---

## ?? Project Structure

```text
midterm-data-pipeline/
??? config/settings.py         # Centralized configurations
??? .env.example               # Environment variables template
??? data/                      # CSV files and delta updates
??? src/
?   ??? main.py                # Single entry point (Router)
?   ??? file_router.py         # Engine selection logic
?   ??? batch_loader.py        # Python streaming loader
?   ??? spark_loader.py        # PySpark parallel loader
?   ??? quality_rules.py       # 9 Data cleaning rules + Audit Trail
?   ??? elt_pipeline.py        # Classification & Idempotent Upsert
?   ??? incremental_loader.py  # Path B: Delta Loading Engine
?   ??? metrics.py             # JSON reporting
?   ??? mongo_setup.py         # Collections & Schema Validation
?   ??? queries.py             # 5 Named Queries (Phase 2)
?   ??? indexes.py             # 3 Indexes + Explain Report (Phase 2)
?   ??? aggregations.py        # 5 Aggregation Reports (Phase 2)
?   ??? materialized_views.py  # Incremental MVs (Phase 2)
?   ??? scheduler.py           # Scheduled Jobs & Logs (Phase 2)
?   ??? api.py                 # FastAPI Unified Interface (Phase 2)
??? tests/                     # Pytest unit tests
??? reports/                   # results.json, explain_report.md, quality_report.json
??? README.md
```

## ?? Screenshots & Proofs
Visual proofs are available in `reports/screenshots/` (Spark UI Stages, Idempotency, MongoDB Compass collections, API Swagger UI).
```

