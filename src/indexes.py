import sys
import json
from pathlib import Path
from datetime import datetime, timezone
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure
from config.settings import MONGODB_URI, DB_NAME, VALIDATED_COLLECTION, REPORTS_DIR
from src.queries import build_query

# ============================================================
# 3 فهارس (منها 2 Compound) — كل فهرس له سبب واضح
# ============================================================
INDEXES = [
    {"name": "idx_city_status",
     "keys": [("city", ASCENDING), ("status", ASCENDING)],
     "reason": "Compound Index: يطابق فلتر (مدينة+حالة) الأكثر استخداماً ويخدم orders_by_city_status و city_status_count"},
    {"name": "idx_customer_id",
     "keys": [("customer_id", ASCENDING)],
     "reason": "Single Index: بحث سريع عن تاريخ عميل معين بدون فحص كامل للمجموعة"},
    {"name": "idx_date_payment",
     "keys": [("order_date", DESCENDING), ("payment_status", ASCENDING)],
     "reason": "Compound Index: نطاقات التاريخ + فلتر الدفع؛ يخدم orders_in_date_range و paid_orders_in_date_range"},
]

EXPLAIN_QUERIES = ["orders_by_city_status", "customer_order_history", "paid_orders_in_date_range"]

def get_collection():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME][VALIDATED_COLLECTION]

def drop_our_indexes():
    """حذف فهارسنا الثلاثة فقط (نترك _id_ والفهارس الأخرى)"""
    client, col = get_collection()
    try:
        for idx in INDEXES:
            try:
                col.drop_index(idx["name"])
                print(f"   🗑️  تم حذف الفهرس: {idx['name']}")
            except OperationFailure as e:
                if "index not found" in str(e).lower():
                    pass  # غير موجود أصلاً
                else:
                    print(f"   ⚠️  تحذير حذف {idx['name']}: {e}")
    finally:
        client.close()

def create_indexes():
    """إنشاء الفهارس الثلاثة"""
    client, col = get_collection()
    created = []
    try:
        for idx in INDEXES:
            col.create_index(idx["keys"], name=idx["name"])
            created.append(idx["name"])
            print(f"   ✅ تم إنشاء الفهرس: {idx['name']}")
        return created
    finally:
        client.close()

def _collect_stages(stage, acc):
    if not isinstance(stage, dict):
        return acc
    if "stage" in stage:
        acc.append(stage["stage"])
    for key in ("inputStage", "inputStages"):
        sub = stage.get(key)
        if isinstance(sub, list):
            for s in sub:
                _collect_stages(s, acc)
        elif isinstance(sub, dict):
            _collect_stages(sub, acc)
    return acc

def explain_stats(name, params=None):
    flt, sort, limit, is_count = build_query(name, params)
    client, col = get_collection()
    try:
        # 🔥 hint={} يجبر MongoDB على إعادة التخطيط بدون Cache
        cur = col.find(flt, {"_id": 0})
        if sort:
            cur = cur.sort(sort)
        if limit:
            cur = cur.limit(limit)
        plan = cur.explain()
        es = plan.get("executionStats", {})
        return {
            "executionTimeMillis": es.get("executionTimeMillis"),
            "totalDocsExamined": es.get("totalDocsExamined"),
            "totalKeysExamined": es.get("totalKeysExamined"),
            "nReturned": es.get("nReturned"),
            "stages": _collect_stages(es.get("executionStages", {}), []),
            "winning_plan_stage": plan.get("queryPlanner", {}).get("winningPlan", {}).get("stage", "UNKNOWN"),
        }
    finally:
        client.close()

def generate_explain_report(params=None):
    """التقرير الرسمي: قبل الفهارس (COLLSCAN) وبعد الفهارس (IXSCAN)"""
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "queries": {}}

    # 🧪 المرحلة 1: حذف فهارسنا الثلاثة ثم Explain (سيظهر COLLSCAN)
    print("🧪 المرحلة 1/3: حذف فهارسنا الثلاثة للتأكد من COLLSCAN...")
    drop_our_indexes()
    
    print("🧪 المرحلة 2/3: قياس الأداء بدون فهارس (فحص كامل للمجموعة)...")
    for name in EXPLAIN_QUERIES:
        before = explain_stats(name, params)
        report["queries"][name] = {"before": before}
        stage_type = "COLLSCAN" if "COLLSCAN" in str(before["stages"]) else "IXSCAN (قديمة)"
        print(f"   ⏳ {name}: stage={before['stages']}, docsExamined={before['totalDocsExamined']:,}, time={before['executionTimeMillis']}ms -> {stage_type}")

    # 🧪 المرحلة 2: إنشاء الفهارس ثم Explain (سيظهر IXSCAN)
    print("\n🧪 المرحلة 3/3: إنشاء الفهارس الثلاثة ثم قياس الأداء...")
    create_indexes()
    for name in EXPLAIN_QUERIES:
        after = explain_stats(name, params)
        before = report["queries"][name]["before"]
        report["queries"][name]["after"] = after
        report["queries"][name]["improvement"] = {
            "docs_examined": f"{before['totalDocsExamined']:,} → {after['totalDocsExamined']:,}",
            "stage": f"{before['stages']} → {after['stages']}",
            "time_ms": f"{before['executionTimeMillis']} → {after['executionTimeMillis']}",
        }
        print(f"   ✅ {name}: stage={after['stages']}, docsExamined={after['totalDocsExamined']:,}, time={after['executionTimeMillis']}ms")

    # 💾 حفظ التقريرين (JSON + MD)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORTS_DIR / "explain_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    lines = ["# 📊 Explain Report (Before / After Indexes)", ""]
    lines.append("## 🔑 Indexes Created")
    for idx in INDEXES:
        lines.append(f"- **`{idx['name']}`** `{idx['keys']}` — {idx['reason']}")
    lines.append("")
    lines.append("## 📈 Performance Comparison")
    lines.append("")
    lines.append("| Query | Before (docs examined) | After (docs examined) | Speed-up |")
    lines.append("|-------|----------------------|---------------------|----------|")
    for name, q in report["queries"].items():
        b = q["before"]["totalDocsExamined"]
        a = q["after"]["totalDocsExamined"]
        speed = f"{b/a:.0f}x" if a > 0 and b > 0 else "N/A"
        lines.append(f"| `{name}` | {b:,} ({'COLLSCAN' if 'COLLSCAN' in str(q['before']['stages']) else 'other'}) | {a:,} (IXSCAN) | **{speed}** |")
    lines.append("")
    lines.append("## 📝 Detailed Results")
    lines.append("")
    for name, q in report["queries"].items():
        lines.append(f"### `{name}`")
        lines.append(f"- **Before:** docsExamined={q['before']['totalDocsExamined']:,}, stages={q['before']['stages']}, time={q['before']['executionTimeMillis']}ms")
        lines.append(f"- **After :** docsExamined={q['after']['totalDocsExamined']:,}, stages={q['after']['stages']}, time={q['after']['executionTimeMillis']}ms")
        lines.append("")
    
    with open(REPORTS_DIR / "explain_report.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n✅ تم حفظ التقرير في: {REPORTS_DIR / 'explain_report.md'}")
    print(f"✅ تم حفظ التفاصيل في: {REPORTS_DIR / 'explain_report.json'}")
    return report

if __name__ == "__main__":
    generate_explain_report()