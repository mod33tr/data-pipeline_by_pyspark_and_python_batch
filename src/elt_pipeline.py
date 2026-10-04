import json
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone
from src.quality_rules import apply_quality_rules
from config.settings import MONGODB_URI, DB_NAME, RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION

WRITE_BATCH = 5000

def process_raw_to_final(run_id, duplicate_ids=None):
    print(f"\n🔄 بدء معالجة ELT لـ run_id: {run_id}")
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    raw_col = db[RAW_COLLECTION]
    valid_col = db[VALIDATED_COLLECTION]
    quarantine_col = db[QUARANTINE_COLLECTION]

    total_raw = raw_col.count_documents({"run_id": run_id})
    print(f"📥 عدد السجلات الخام لهذا التشغيل: {total_raw:,}")

    # 🔥 مجموعة التكرارات صغيرة (تأتي من Spark) فلا تستهلك الذاكرة
    dup_set = set(duplicate_ids) if duplicate_ids else None
    seen = set()

    metrics = {
        "valid_count": 0, "corrected_count": 0, "quarantine_count": 0,
        "inserted_count": 0, "updated_count": 0, "unchanged_count": 0,
        "error_case_counts": {}
    }

    valid_batch = []
    quarantine_batch = []
    processed = 0

    def count_errors(codes):
        for c in codes:
            metrics["error_case_counts"][c] = metrics["error_case_counts"].get(c, 0) + 1

    def flush_valid():
        if not valid_batch:
            return
        try:
            res = valid_col.bulk_write(valid_batch, ordered=False)
            metrics["inserted_count"] += res.upserted_count
            metrics["updated_count"] += res.modified_count
            metrics["unchanged_count"] += len(valid_batch) - res.upserted_count - res.modified_count
            print(f"   ✅ Upsert دفعة: {len(valid_batch):,} سجل")
        except BulkWriteError as e:
            print(f"خطأ في Upsert: {e.details}")
        valid_batch.clear()

    def flush_quarantine():
        if not quarantine_batch:
            return
        try:
            quarantine_col.insert_many(quarantine_batch, ordered=False)
        except BulkWriteError as e:
            print(f"خطأ في إدخال المعزولات: {e.details}")
        quarantine_batch.clear()

    # 🔥 قراءة Streaming بالمؤشر — لا تحميل كامل في الذاكرة أبداً
    cursor = raw_col.find({"run_id": run_id}, {"_id": 0}).batch_size(2000)

    for raw_doc in cursor:
        try:
            raw_record_raw = raw_doc.get("raw_record", "{}")
            raw_record = json.loads(raw_record_raw) if isinstance(raw_record_raw, str) else raw_record_raw

            result = apply_quality_rules(raw_record)
            processed_record = result["record"]
            corrections = result["corrections"]
            error_codes = result["error_codes"]
            status = result["status"]

            processed_record["_quality_status"] = status
            processed_record["quality_status"] = status
            if "_delta_timestamp" in raw_doc:
                processed_record["_delta_timestamp"] = raw_doc["_delta_timestamp"]
                processed_record["_processed_at"] = datetime.now(timezone.utc)

            if status == "quarantined":
                metrics["quarantine_count"] += 1
                if len(error_codes) >= 2 and "MULTIPLE_CONFLICTING_ERRORS" not in error_codes:
                    error_codes.append("MULTIPLE_CONFLICTING_ERRORS")
                count_errors(error_codes)
                quarantine_batch.append({
                    "run_id": run_id,
                    "source_file": raw_doc.get("source_file"),
                    "source_row_number": raw_doc.get("source_row_number"),
                    "raw_record": raw_record,
                    "error_codes": error_codes,
                    "error_details": f"Errors found: {', '.join(error_codes)}",
                    "quarantined_at": datetime.now(timezone.utc)
                })
            else:
                order_id = processed_record.get("order_id")

                # 🔥 فحص التكرار (ذاكرة محدودة: نتتبع فقط المعرفات المكررة أصلاً)
                is_dup = False
                if order_id:
                    if order_id in seen:
                        is_dup = True
                    elif dup_set is None or order_id in dup_set:
                        seen.add(order_id)

                if order_id and is_dup:
                    metrics["quarantine_count"] += 1
                    count_errors(["DUPLICATE_ORDER_ID"])
                    quarantine_batch.append({
                        "run_id": run_id,
                        "source_file": raw_doc.get("source_file"),
                        "raw_record": raw_record,
                        "error_codes": ["DUPLICATE_ORDER_ID"],
                        "error_details": "Duplicate order_id within the same run_id",
                        "quarantined_at": datetime.now(timezone.utc)
                    })
                elif order_id:
                    if status == "corrected":
                        metrics["corrected_count"] += 1
                        processed_record["_corrections"] = corrections
                        processed_record["corrections"] = corrections
                    elif status == "valid":
                        metrics["valid_count"] += 1

                    # فصل بيانات الإنشاء الأولي (تضمن قياس unchanged بدقة عند إعادة التشغيل)
                    set_doc = dict(processed_record)
                    set_on_insert = {
                        "_run_id": run_id,
                        "_processed_at": datetime.now(timezone.utc)
                    }
                    valid_batch.append(
                        UpdateOne(
                            {"order_id": order_id},
                            {"$set": set_doc, "$setOnInsert": set_on_insert},
                            upsert=True
                        )
                    )
                else:
                    metrics["quarantine_count"] += 1
                    count_errors(["MISSING_ORDER_ID"])
                    quarantine_batch.append({
                        "run_id": run_id,
                        "raw_record": raw_record,
                        "error_codes": ["MISSING_ORDER_ID"],
                        "error_details": "Order ID missing after processing",
                        "quarantined_at": datetime.now(timezone.utc)
                    })

            processed += 1
            if processed % 500000 == 0:
                print(f"⏳ تمت معالجة {processed:,} / {total_raw:,} سجل...")

            if len(valid_batch) >= WRITE_BATCH:
                flush_valid()
            if len(quarantine_batch) >= WRITE_BATCH:
                flush_quarantine()

        except Exception as e:
            print(f"❌ خطأ في معالجة السجل: {e}")
            metrics["quarantine_count"] += 1
            count_errors(["PROCESSING_ERROR"])
            quarantine_batch.append({
                "run_id": run_id,
                "raw_record": raw_record_raw,
                "error_codes": ["PROCESSING_ERROR"],
                "error_details": str(e),
                "quarantined_at": datetime.now(timezone.utc)
            })
            processed += 1

    # تفريغ نهائي
    flush_valid()
    flush_quarantine()

    total_processed = metrics["valid_count"] + metrics["corrected_count"] + metrics["quarantine_count"]
    print(f"\n📊 ملخص المعالجة:")
    print(f"   السجلات السليمة (Valid): {metrics['valid_count']:,}")
    print(f"   السجلات المصححة (Corrected): {metrics['corrected_count']:,}")
    print(f"   السجلات المعزولة (Quarantined): {metrics['quarantine_count']:,}")
    print(f"   الإجمالي: {total_processed:,}")

    if total_processed != total_raw:
        print(f"❌ خطأ فادح: المعالجة ({total_processed:,}) لا تطابق الخام ({total_raw:,})!")
    else:
        print("✅ قاعدة الاتساق (Raw = Valid + Corrected + Quarantine) محققة بنجاح.")

    try:
        client.close()
    except Exception as e:
        print(f"⚠️ تحذير أثناء إغلاق MongoDB: {e}")

    return metrics