import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient, ASCENDING
from config.settings import MONGODB_URI, DB_NAME, VALIDATED_COLLECTION

MV_DAILY = "mv_daily_sales_summary"
MV_PRODUCTS = "mv_top_products_summary"
META = "mv_metadata"

def get_db():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME]
def ensure_mv_indexes():
    """فهرس الـ Watermark: يجعل اكتشاف المتغير فورياً بدل فحص 26 مليون سجل"""
    client, db = get_db()
    try:
        col = db[VALIDATED_COLLECTION]
        col.create_index([("_processed_at", ASCENDING)], name="idx_processed_at")
    finally:
        client.close()
# ============================================================
# خطوط البناء الكامل (تُستخدم مرة واحدة فقط عند أول بناء)
# ============================================================
def _pipeline_daily():
    return [
        {"$match": {"total_amount": {"$type": "number"}, "order_date": {"$type": "string"}}},
        {"$group": {"_id": {"$substr": ["$order_date", 0, 10]},
                    "total_sales": {"$sum": "$total_amount"},
                    "order_count": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
    ]

def _pipeline_products():
    return [
        {"$match": {"items": {"$type": "array"}}},
        {"$unwind": "$items"},
        {"$match": {"items.sku": {"$type": "string"},
                    "items.qty": {"$type": "number"},
                    "items.unit_price": {"$type": "number"}}},
        {"$group": {"_id": "$items.sku",
                    "total_qty": {"$sum": "$items.qty"},
                    "total_revenue": {"$sum": {"$multiply": ["$items.qty", "$items.unit_price"]}}}},
        {"$sort": {"total_revenue": -1}},
    ]

# ============================================================
# التحديث التزايدي: نكتشف "المفاتيح المتأثرة" من الـ Delta فقط
# (نستفيد من _processed_at الذي يكتبه الـ Pipeline عند كل Upsert)
# ============================================================
def _affected_days(col, last):
    rows = col.aggregate([
        {"$match": {"_processed_at": {"$gt": last}, "order_date": {"$type": "string"}}},
        {"$group": {"_id": {"$substr": ["$order_date", 0, 10]}}},
    ], allowDiskUse=True)
    return [r["_id"] for r in rows if r["_id"]]

def _affected_skus(col, last):
    rows = col.aggregate([
        {"$match": {"_processed_at": {"$gt": last}, "items": {"$type": "array"}}},
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.sku"}},
    ], allowDiskUse=True)
    return [r["_id"] for r in rows if r["_id"]]

def _refresh_daily(col, mv, last):
    """إعادة حساب الأيام المتأثرة فقط (باستخدام فهرس التاريخ = سريع)"""
    days = _affected_days(col, last)
    for day in days:
        nxt = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        res = list(col.aggregate([
            {"$match": {"order_date": {"$gte": day, "$lt": nxt}, "total_amount": {"$type": "number"}}},
            {"$group": {"_id": {"$substr": ["$order_date", 0, 10]},
                        "total_sales": {"$sum": "$total_amount"},
                        "order_count": {"$sum": 1}}},
        ]))
        doc = res[0] if res else {"_id": day, "total_sales": 0, "order_count": 0}
        mv.replace_one({"_id": day}, doc, upsert=True)  ;                               print(f"   📅 أيام متأثرة: {len(days)}") 
    return len(days)

def _refresh_products(col, mv, last):
    """إعادة حساب المنتجات المتأثرة فقط (مسحة واحدة مفلترة بدل بناء الكل)"""
    skus = _affected_skus(col, last)
    if not skus:
        return 0
    res = col.aggregate([
        {"$match": {"items": {"$type": "array"}, "items.sku": {"$in": skus}}},
        {"$unwind": "$items"},
        {"$match": {"items.sku": {"$in": skus},
                    "items.qty": {"$type": "number"},
                    "items.unit_price": {"$type": "number"}}},
        {"$group": {"_id": "$items.sku",
                    "total_qty": {"$sum": "$items.qty"},
                    "total_revenue": {"$sum": {"$multiply": ["$items.qty", "$items.unit_price"]}}}},
    ], allowDiskUse=True)
    for doc in res:
        mv.replace_one({"_id": doc["_id"]}, doc, upsert=True)  ;                        print(f"   📦 منتجات متأثرة: {len(skus)}") 
    return len(skus)

# ============================================================
# البناء الكامل / التحديث التزايدي / العرض
# ============================================================
def build_mv(name):
    start = time.perf_counter()
    client, db = get_db()
    try:
        col = db[VALIDATED_COLLECTION]
        mv = db[name]
        mv.delete_many({})
        pipe = _pipeline_daily() if name == MV_DAILY else _pipeline_products()
        rows = list(col.aggregate(pipe, allowDiskUse=True))
        if rows:
            mv.insert_many(rows)
        db[META].update_one({"_id": name}, {"$set": {
            "last_refresh_at": datetime.now(timezone.utc),
            "mode": "full_build", "rows": len(rows),
            "duration_s": round(time.perf_counter() - start, 2),
            "status": "success"}}, upsert=True)
        return len(rows)
    finally:
        client.close()

def refresh_mv(name):
    ensure_mv_indexes()  
    start = time.perf_counter()
    client, db = get_db()
    try:
        col = db[VALIDATED_COLLECTION]
        mv = db[name]
        meta = db[META].find_one({"_id": name})
        if not meta or "last_refresh_at" not in meta:
            n = build_mv(name)
            return {"mode": "full_build", "affected": n}
        last = meta["last_refresh_at"]    ;                                             print(f"   🔎 الـ Watermark: {last} — اكتشاف المفاتيح المتأثرة عبر الفهرس...")                                          
        if name == MV_DAILY:
            n = _refresh_daily(col, mv, last)
        else:
            n = _refresh_products(col, mv, last)
        db[META].update_one({"_id": name}, {"$set": {
            "last_refresh_at": datetime.now(timezone.utc),
            "mode": "incremental", "affected_keys": n,
            "duration_s": round(time.perf_counter() - start, 2),
            "status": "success"}}, upsert=True)
        return {"mode": "incremental", "affected": n}
    finally:
        client.close()

def show_mv(name, limit=10):
    client, db = get_db()
    try:
        sort_key = {"_id": -1} if name == MV_DAILY else {"total_revenue": -1}
        rows = list(db[name].find().sort(sort_key).limit(limit))
        meta = db[META].find_one({"_id": name}, {"_id": 0})
        return rows, meta
    finally:
        client.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["build", "refresh", "show"])
    ap.add_argument("--mv", choices=[MV_DAILY, MV_PRODUCTS], default=None)
    args = ap.parse_args()

    targets = [args.mv] if args.mv else [MV_DAILY, MV_PRODUCTS]
    for name in targets:
        if args.action == "build":
            print(f"🏗️ بناء كامل لـ {name} (مرة واحدة فقط)...")
            n = build_mv(name)
            print(f"   ✅ تم بناء {n:,} صف")
        elif args.action == "refresh":
            print(f"🔄 تحديث تزايدي لـ {name} (المتأثر فقط)...")
            r = refresh_mv(name)
            print(f"   ✅ mode={r['mode']} | affected_keys={r['affected']}")
        else:
            rows, meta = show_mv(name)
            print(f"\n📊 {name}")
            print(f"   metadata: {meta}")
            for r in rows:
                print("   ", r)