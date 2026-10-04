import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient, DESCENDING
from config.settings import MONGODB_URI, DB_NAME, VALIDATED_COLLECTION

def get_collection():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME][VALIDATED_COLLECTION]

def _sample_value(col, field):
    """قيمة افتراضية ديناميكية (بدون تثبيت بيانات) عبر عينة سريعة"""
    doc = col.find_one({field: {"$type": "string"}}, {field: 1})
    return doc.get(field) if doc else None

def _clean(docs):
    return [{k: v for k, v in d.items() if k == "_id"} and {} or {k: v for k, v in d.items() if k != "_id"} for d in docs]

# ============================================================
# 5 استعلامات مسماة (القيم الافتراضية ديناميكية 100%)
# ============================================================
QUERY_DEFS = {
    "orders_by_city_status": {
        "desc": "Orders filtered by city + status (served by idx_city_status)",
        "filter": lambda p, col: {"city": p.get("city") or _sample_value(col, "city"),
                                  "status": p.get("status", "confirmed")},
        "sort": [("order_date", DESCENDING)], "limit": 50, "count": False,
    },
    "customer_order_history": {
        "desc": "Full order history of one customer (served by idx_customer_id)",
        "filter": lambda p, col: {"customer_id": p.get("customer_id") or _sample_value(col, "customer_id")},
        "sort": [("order_date", DESCENDING)], "limit": 50, "count": False,
    },
    "orders_in_date_range": {
        "desc": "Orders within a date range (served by idx_date_payment prefix)",
        "filter": lambda p, col: {"order_date": {"$gte": p.get("start") or (_sample_value(col, "order_date") or "0000-00-00"),
                                                 "$lte": p.get("end", "9999-12-31T23:59:59")}},
        "sort": [("order_date", DESCENDING)], "limit": 50, "count": False,
    },
    "paid_orders_in_date_range": {
        "desc": "Paid orders within a date range (served by idx_date_payment fully)",
        "filter": lambda p, col: {"payment_status": p.get("payment_status", "paid"),
                                  "order_date": {"$gte": p.get("start") or (_sample_value(col, "order_date") or "0000-00-00"),
                                                 "$lte": p.get("end", "9999-12-31T23:59:59")}},
        "sort": [("order_date", DESCENDING)], "limit": 50, "count": False,
    },
    "city_status_count": {
        "desc": "Count of orders for city+status (covered by idx_city_status)",
        "filter": lambda p, col: {"city": p.get("city") or _sample_value(col, "city"),
                                  "status": p.get("status", "confirmed")},
        "sort": [], "limit": 0, "count": True,
    },
}

def build_query(name, params=None):
    """يبني الفلتر والفرز بدون تنفيذ (يستخدمه الـ Explain أيضاً)"""
    params = params or {}
    if name not in QUERY_DEFS:
        raise KeyError(f"Unknown query: {name}. Available: {list(QUERY_DEFS)}")
    q = QUERY_DEFS[name]
    client, col = get_collection()
    try:
        flt = q["filter"](params, col)
    finally:
        client.close()
    return flt, q.get("sort", []), q.get("limit", 0), q.get("count", False)

def run_query(name, params=None):
    params = params or {}
    flt, sort, limit, is_count = build_query(name, params)
    client, col = get_collection()
    try:
        if is_count:
            return {"query": name, "filter": flt, "count": col.count_documents(flt)}
        cur = col.find(flt, {"_id": 0})
        if sort:
            cur = cur.sort(sort)
        if limit:
            cur = cur.limit(limit)
        return {"query": name, "filter": flt, "results": list(cur)}
    finally:
        client.close()

def list_queries():
    return {n: d["desc"] for n, d in QUERY_DEFS.items()}

if __name__ == "__main__":
    for n in QUERY_DEFS:
        out = run_query(n)
        size = out.get("count", len(out.get("results", [])))
        print(f"✅ {n}: {size} نتيجة")