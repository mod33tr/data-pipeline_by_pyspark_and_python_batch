import sys
import uuid
from pathlib import Path
from typing import Optional
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config.settings import HUGE_FILE_PATH, SMALL_SAMPLE_PATH
from src.file_router import choose_engine
from src.batch_loader import load_csv_batch
from src.spark_loader import load_csv_spark
from src.elt_pipeline import process_raw_to_final
from src.metrics import save_metrics
from src import queries as Q
from src import aggregations as A
from src import indexes as IX
from src.materialized_views import refresh_mv, MV_DAILY, MV_PRODUCTS, show_mv
from src.scheduler import run_job, show_logs, JOBS

app = FastAPI(
    title="Hybrid Data Pipeline — Unified Execution API",
    description="واجهة تنفيذ واختبار موحدة لنفس وظائف المشروع (ليست Backend مستقلاً)",
    version="2.0",
)

# ============================================================
# 1) GET /health
# ============================================================
@app.get("/health")
def health():
    return {"status": "ok", "service": "hybrid-data-pipeline",
            "engines": ["python_batch", "pyspark"]}

# ============================================================
# 2) POST /ingest — نفس بوابة الإدخال في المشروع النصفي (بدون مسار جديد)
# ============================================================
class IngestRequest(BaseModel):
    file: str = "sample"          # sample | huge
    engine: str = "auto"          # auto | python_batch | pyspark

@app.post("/ingest")
def ingest(req: IngestRequest):
    if req.file == "huge":
        target = HUGE_FILE_PATH
    elif req.file == "sample":
        target = SMALL_SAMPLE_PATH
    else:
        target = Path(req.file)
        if not target.is_absolute() and not target.exists():
            data_candidate = Path("data") / req.file
            if data_candidate.exists():
                target = data_candidate

    if not target.exists():
        raise HTTPException(404, f"File not found: {target}")

    if req.engine == "auto":
        engine, size_mb = choose_engine(target)
    else:
        engine = req.engine
        size_mb = target.stat().st_size / (1024 * 1024)

    run_id = str(uuid.uuid4())
    if engine == "python_batch":
        raw_metrics = load_csv_batch(target, run_id, engine)
    else:
        raw_metrics = load_csv_spark(target, run_id, engine)

    elt_metrics = process_raw_to_final(run_id, duplicate_ids=raw_metrics.get("duplicate_ids"))
    save_metrics(run_id, target.name, size_mb, engine, raw_metrics, elt_metrics)
    return {"run_id": run_id, "engine": engine, "file": target.name,
            "file_size_mb": round(size_mb, 2), "metrics": elt_metrics}

# ============================================================
# 3) POST /indexes
# ============================================================
@app.post("/indexes")
def create_indexes_endpoint():
    return {"created": IX.create_indexes(), "explain_report": "reports/explain_report.md"}

# ============================================================
# 4-5) Queries
# ============================================================
@app.get("/queries")
def list_queries_endpoint():
    return Q.list_queries()

@app.get("/queries/{name}")
def run_query_endpoint(name: str, city: Optional[str] = None, status: Optional[str] = None,
                       customer_id: Optional[str] = None, payment_status: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       limit: Optional[int] = None):
    params = {k: v for k, v in
              dict(city=city, status=status, customer_id=customer_id,
                   payment_status=payment_status, start=start, end=end, limit=limit).items()
              if v is not None}
    try:
        return Q.run_query(name, params)
    except KeyError as e:
        raise HTTPException(404, str(e))

# ============================================================
# 6-7) Aggregations
# ============================================================
@app.get("/aggregations")
def list_aggregations_endpoint():
    return A.list_aggregations()

@app.get("/aggregations/{name}")
def run_aggregation_endpoint(name: str):
    try:
        return A.run_aggregation(name)
    except KeyError as e:
        raise HTTPException(404, str(e))

# ============================================================
# 8) POST /refresh-mv
# ============================================================
@app.post("/refresh-mv")
def refresh_mv_endpoint(mv: Optional[str] = None):
    targets = [mv] if mv else [MV_DAILY, MV_PRODUCTS]
    try:
        return {name: refresh_mv(name) for name in targets}
    except Exception as e:
        raise HTTPException(400, str(e))

# ============================================================
# 9-10) Jobs
# ============================================================
@app.get("/jobs")
def jobs_endpoint():
    return {
        "registered": {n: {"interval_minutes": j["interval_minutes"], "desc": j["desc"]}
                       for n, j in JOBS.items()},
        "last_logs": show_logs(10),
    }

@app.post("/jobs/{name}/run")
def run_job_endpoint(name: str):
    try:
        return run_job(name)
    except KeyError as e:
        raise HTTPException(404, str(e))