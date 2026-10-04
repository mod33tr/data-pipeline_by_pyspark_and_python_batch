import sys
import time
from pathlib import Path
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient
from config.settings import MONGODB_URI, DB_NAME, VALIDATED_COLLECTION

def get_collection():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME][VALIDATED_COLLECTION]

# ============================================================
# فهارس تغطية لتسريع التجميعات (تقرأ الفهرس بدل الوثيقة كاملة)
# ============================================================
AGG_INDEXES = [
    ("agg_city_sales", [("city", 1), ("total_amount", 1)]),
    ("agg_customer_sales", [("customer_id", 1), ("total_amount", 1)]),
    ("agg_status", [("status", 1)]),
    ("agg_payment_sales", [("payment_method", 1), ("total_amount", 1)]),
    ("agg_date_sales", [("order_date", 1), ("total_amount", 1)]),
]

def ensure_agg_indexes():
    client, col = get_collection()
    try:
        for name, keys in AGG_INDEXES:
            col.create_index(keys, name=name)
    finally:
        client.close()

# ============================================================
# 5 تقارير التجميع (ديناميكية 100%)
# ============================================================
AGGREGATION_DEFS = {
    "sales_by_city": {
        "desc": "إجمالي المبيعات وعدد الطلبات حسب المدينة (أعلى 10)",
        "pipeline": [
            {"$match": {"total_amount": {"$type": "number"}}},
            {"$group": {"_id": "$city", "total_sales": {"$sum": "$total_amount"}, "order_count": {"$sum": 1}}},
            {"$sort": {"total_sales": -1}},
            {"$limit": 10}
        ]
    },
    "top_customers": {
        "desc": "أفضل 10 عملاء من حيث إجمالي الإنفاق",
        "pipeline": [
            {"$match": {"total_amount": {"$type": "number"}, "customer_id": {"$type": "string"}}},
            {"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$total_amount"}, "orders_count": {"$sum": 1}}},
            {"$sort": {"total_spent": -1}},
            {"$limit": 10}
        ]
    },
    "orders_by_status": {
        "desc": "توزيع الطلبات حسب الحالة",
        "pipeline": [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
    },
    "sales_by_payment_method": {
        "desc": "إجمالي المبيعات حسب طريقة الدفع",
        "pipeline": [
            {"$match": {"total_amount": {"$type": "number"}}},
            {"$group": {"_id": "$payment_method", "total_sales": {"$sum": "$total_amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total_sales": -1}}
        ]
    },
    "daily_sales_trend": {
        "desc": "اتجاه المبيعات اليومية (آخر 10 أيام مسجلة)",
        "pipeline": [
            {"$match": {"order_date": {"$type": "string"}, "total_amount": {"$type": "number"}}},
            {"$group": {"_id": {"$substr": ["$order_date", 0, 10]}, "daily_sales": {"$sum": "$total_amount"}, "order_count": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
            {"$limit": 10}
        ]
    }
}

def run_aggregation(name):
    if name not in AGGREGATION_DEFS:
        raise KeyError(f"Unknown aggregation: {name}. Available: {list(AGGREGATION_DEFS)}")
    client, col = get_collection()
    try:
        results = list(col.aggregate(AGGREGATION_DEFS[name]["pipeline"], allowDiskUse=True))
        return {"aggregation": name, "description": AGGREGATION_DEFS[name]["desc"], "results": results}
    finally:
        client.close()

def list_aggregations():
    return {n: d["desc"] for n, d in AGGREGATION_DEFS.items()}

if __name__ == "__main__":
    # 🔥 يمكن تشغيل تقرير واحد: python src/aggregations.py top_customers
    target = sys.argv[1] if len(sys.argv) > 1 else None

    print("🔧 تجهيز فهارس التسريع (Covering Indexes)...")
    ensure_agg_indexes()
    print("✅ فهارس التسريع جاهزة")

    names = [target] if target else list(AGGREGATION_DEFS)
    for name in names:
        print(f"\n📊 [{name}] {AGGREGATION_DEFS[name]['desc']}")
        print("   ⏳ جاري الحساب على كامل المجموعة...")
        t0 = time.perf_counter()
        res = run_aggregation(name)
        elapsed = time.perf_counter() - t0

        for i, doc in enumerate(res["results"][:5], 1):
            print(f"   {i}. {doc}")
        if len(res["results"]) > 5:
            print(f"   ... و {len(res['results']) - 5} نتائج أخرى")
        print(f"   ✅ اكتمل خلال {elapsed:.1f} ثانية")