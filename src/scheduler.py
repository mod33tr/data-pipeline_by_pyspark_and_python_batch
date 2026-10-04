import sys
import os
import time
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import schedule
from pymongo import MongoClient
from config.settings import (MONGODB_URI, DB_NAME, REPORTS_DIR,
                             VALIDATED_COLLECTION, QUARANTINE_COLLECTION)
from src.materialized_views import refresh_mv, MV_DAILY, MV_PRODUCTS

JOB_LOGS = "job_logs"

def get_db():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME]

# ============================================================
# المهمة 1: تحديث تزايدي للعارضين الماديين (وظيفة حقيقية)
# ============================================================
def job_refresh_mvs():
    results = {}
    for name in (MV_DAILY, MV_PRODUCTS):
        results[name] = refresh_mv(name)
    return results

# ============================================================
# المهمة 2: تقرير دوري لجودة البيانات (وظيفة حقيقية)
# ============================================================
def job_quality_report():
    client, db = get_db()
    try:
        valid = db[VALIDATED_COLLECTION]
        quar = db[QUARANTINE_COLLECTION]

        status_dist = list(valid.aggregate(
            [{"$group": {"_id": "$status", "count": {"$sum": 1}}}], allowDiskUse=True))
        error_dist = list(quar.aggregate(
            [{"$unwind": "$error_codes"},
             {"$group": {"_id": "$error_codes", "count": {"$sum": 1}}},
             {"$sort": {"count": -1}}], allowDiskUse=True))

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": {
                "validated": valid.estimated_document_count(),
                "quarantined": quar.estimated_document_count(),
            },
            "status_distribution": {r["_id"]: r["count"] for r in status_dist},
            "error_distribution": {r["_id"]: r["count"] for r in error_dist},
        }

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORTS_DIR / "quality_report.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        return {"saved_to": str(out), **report["totals"]}
    finally:
        client.close()

# ============================================================
# سجل المهام (الفترات من متغيرات البيئة)
# ============================================================
JOBS = {
    "refresh_materialized_views": {
        "func": job_refresh_mvs,
        "interval_minutes": int(os.getenv("JOB_REFRESH_INTERVAL_MINUTES", 5)),
        "desc": "تحديث تزايدي للعارضين الماديين",
    },
    "daily_quality_report": {
        "func": job_quality_report,
        "interval_minutes": int(os.getenv("JOB_REPORT_INTERVAL_MINUTES", 60)),
        "desc": "تقرير دوري لجودة البيانات",
    },
}

# ============================================================
# تشغيل + تسجيل (بداية/نهاية/حالة) — يدوياً أو مجدولاً
# ============================================================
def run_job(name):
    if name not in JOBS:
        raise KeyError(f"Unknown job: {name}. Available: {list(JOBS)}")

    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    print(f"▶️ بدء المهمة: {name} ...")
    try:
        detail = JOBS[name]["func"]()
        status = "success"
    except Exception as e:
        detail = {"error": str(e)}
        status = "failed"
    duration = round(time.perf_counter() - t0, 2)
    finished = datetime.now(timezone.utc)

    log_entry = {
        "job": name, "started_at": started, "finished_at": finished,
        "duration_s": duration, "status": status, "detail": detail,
    }
    client, db = get_db()
    try:
        db[JOB_LOGS].insert_one(log_entry)
    finally:
        client.close()

    icon = "✅" if status == "success" else "❌"
    print(f"{icon} انتهت المهمة: {name} | status={status} | duration={duration}s")
    return log_entry

def show_logs(limit=10):
    client, db = get_db()
    try:
        return list(db[JOB_LOGS].find({}, {"_id": 0}).sort("started_at", -1).limit(limit))
    finally:
        client.close()

# ============================================================
# حلقة الجدولة
# ============================================================
def start_scheduler():
    for name, job in JOBS.items():
        schedule.every(job["interval_minutes"]).minutes.do(run_job, name=name)
        print(f"⏰ جَدولة [{name}] كل {job['interval_minutes']} دقيقة — {job['desc']}")
    print("🔁 المجدول يعمل الآن... (Ctrl+C للإيقاف)")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", choices=list(JOBS), help="تشغيل مهمة يدوياً (للاختبار والمناقشة)")
    ap.add_argument("--logs", action="store_true", help="عرض آخر سجلات التنفيذ")
    args = ap.parse_args()

    if args.run:
        entry = run_job(args.run)
        print(json.dumps(entry, ensure_ascii=False, indent=2, default=str))
    elif args.logs:
        for row in show_logs():
            print(row)
    else:
        start_scheduler()