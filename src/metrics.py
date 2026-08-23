import json
from pathlib import Path
from datetime import datetime
from config.settings import REPORTS_DIR

def save_metrics(run_id, file_name, file_size_mb, engine_used, raw_metrics, elt_metrics):
    metrics_path = REPORTS_DIR / "results.json"
    
    # حساب throughput
    elapsed = raw_metrics.get("elapsed_seconds", 1)
    rows_read = raw_metrics.get("rows_read", 0)
    throughput = rows_read / elapsed if elapsed > 0 else 0
    
    final_metrics = {
        "run_id": run_id,
        "file_name": str(file_name),
        "file_size_mb": round(file_size_mb, 2),
        "engine_used": engine_used,
        "rows_read": rows_read,
        "raw_loaded": raw_metrics.get("raw_loaded", 0),
        "valid_count": elt_metrics.get("valid_count", 0),
        "corrected_count": elt_metrics.get("corrected_count", 0),
        "quarantine_count": elt_metrics.get("quarantine_count", 0),
        "elapsed_seconds": round(elapsed, 2),
        "throughput": round(throughput, 2),
        "batch_size": 1000 if engine_used == "python_batch" else "N/A",
        "partitions": raw_metrics.get("partitions", "N/A"),
        "error_case_counts": elt_metrics.get("error_case_counts", {}),  # 🔥 إضافة أنواع الأخطاء
        "inserted_count": elt_metrics.get("inserted_count", 0),
        "updated_count": elt_metrics.get("updated_count", 0),
        "unchanged_count": elt_metrics.get("unchanged_count", 0),
        "timestamp": datetime.now().isoformat()
    }
    
    # التأكد من وجود المجلد
    REPORTS_DIR.mkdir(exist_ok=True)
    
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(final_metrics, f, ensure_ascii=False, indent=2)
        
    print(f"\n✅ تم حفظ المقاييس في: {metrics_path}")