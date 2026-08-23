import json
import traceback
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone
from src.quality_rules import apply_quality_rules
from config.settings import MONGODB_URI, DB_NAME, RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION

def process_raw_to_final(run_id):
    print(f"\n🔄 بدء معالجة ELT لـ run_id: {run_id}")
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    
    raw_col = db[RAW_COLLECTION]
    valid_col = db[VALIDATED_COLLECTION]
    quarantine_col = db[QUARANTINE_COLLECTION]
    
    raw_docs = list(raw_col.find({"run_id": run_id}, {"_id": 0}))
    print(f"📥 تم قراءة {len(raw_docs):,} سجل من orders_raw")
    
    valid_operations = []
    quarantine_docs = []
    
    metrics = {
        "valid_count": 0,
        "corrected_count": 0,
        "quarantine_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "unchanged_count": 0,
        "error_case_counts": {}  # 🔥 لتتبع عدد كل نوع خطأ (متطلب 6.12)
    }
    
    processed_count = 0
    seen_order_ids = set()  # 🔥 لتتبع الـ order_ids داخل نفس التشغيل

    # ============================================
    # 🔥 دالة مساعدة لتعبئة error_case_counts
    # ============================================
    def count_errors(error_codes_list):
        for code in error_codes_list:
            metrics["error_case_counts"][code] = metrics["error_case_counts"].get(code, 0) + 1

    # ============================================
    # 🔥 الحلقة الرئيسية مع حماية كاملة
    # ============================================
    for raw_doc in raw_docs:
        try:
            raw_record_raw = raw_doc.get("raw_record", "{}")
            
            # فك تشفير JSON إذا كان نصاً
            if isinstance(raw_record_raw, str):
                raw_record = json.loads(raw_record_raw)
            else:
                raw_record = raw_record_raw
                
            # تطبيق قواعد التنظيف
            result = apply_quality_rules(raw_record)
            processed_record = result["record"]
            corrections = result["corrections"]
            error_codes = result["error_codes"]
            status = result["status"]
            
            # إضافة Metadata للتتبع
            processed_record["_quality_status"] = status
            processed_record["_run_id"] = run_id
            
            # 🔥 Version Handling (متطلب المسار B)
            if "_delta_timestamp" in raw_doc:
                processed_record["_delta_timestamp"] = raw_doc["_delta_timestamp"]
            processed_record["_processed_at"] = datetime.now(timezone.utc)
            
            # ============================================
            # التصنيف والتوجيه
            # ============================================
            if status == "quarantined":
                metrics["quarantine_count"] += 1
                
                # 🔥 فحص MULTIPLE_CONFLICTING_ERRORS (متطلب 6.8)
                if len(error_codes) >= 2 and "MULTIPLE_CONFLICTING_ERRORS" not in error_codes:
                    error_codes.append("MULTIPLE_CONFLICTING_ERRORS")
                
                # 🔥 تعبئة error_case_counts (متطلب 6.12)
                count_errors(error_codes)
                
                quarantine_docs.append({
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
                
                # 🔥 فحص DUPLICATE_ORDER_ID داخل نفس التشغيل (متطلب 6.8)
                if order_id and order_id in seen_order_ids:
                    metrics["quarantine_count"] += 1
                    count_errors(["DUPLICATE_ORDER_ID"])
                    quarantine_docs.append({
                        "run_id": run_id,
                        "source_file": raw_doc.get("source_file"),
                        "raw_record": raw_record,
                        "error_codes": ["DUPLICATE_ORDER_ID"],
                        "error_details": "Duplicate order_id within the same run_id",
                        "quarantined_at": datetime.now(timezone.utc)
                    })
                    processed_count += 1
                    continue
                
                if order_id:
                    seen_order_ids.add(order_id)
                    
                    if status == "corrected":
                        metrics["corrected_count"] += 1
                        processed_record["_corrections"] = corrections
                    elif status == "valid":
                        metrics["valid_count"] += 1
                    
                    valid_operations.append(
                        UpdateOne(
                            {"order_id": order_id},
                            {"$set": processed_record},
                            upsert=True
                        )
                    )
                else:
                    # 🔥 عزل عند فقدان order_id
                    metrics["quarantine_count"] += 1
                    count_errors(["MISSING_ORDER_ID"])
                    quarantine_docs.append({
                        "run_id": run_id,
                        "raw_record": raw_record,
                        "error_codes": ["MISSING_ORDER_ID"],
                        "error_details": "Order ID missing after processing",
                        "quarantined_at": datetime.now(timezone.utc)
                    })
                    
            processed_count += 1
            
            # 🔥 عداد التقدم
            if processed_count % 50000 == 0:
                print(f"⏳ تمت معالجة {processed_count:,} سجل...")
                
        except Exception as e:
            # اصطياد أي خطأ غريب وعزل السجل
            print(f"❌ خطأ في معالجة السجل: {e}")
            metrics["quarantine_count"] += 1
            count_errors(["PROCESSING_ERROR"])
            quarantine_docs.append({
                "run_id": run_id,
                "raw_record": raw_record_raw,
                "error_codes": ["PROCESSING_ERROR"],
                "error_details": str(e),
                "quarantined_at": datetime.now(timezone.utc)
            })
            processed_count += 1

    # ============================================
    # إدخال المعزولات
    # ============================================
    if quarantine_docs:
        try:
            quarantine_col.insert_many(quarantine_docs, ordered=False)
            print(f"⚠️ تم عزل {len(quarantine_docs):,} سجل في orders_quarantine")
        except BulkWriteError as e:
            print(f"خطأ في إدخال المعزولات: {e.details}")
            
    # ============================================
    # تنفيذ Upsert للسجلات الصحيحة
    # ============================================
    if valid_operations:
        try:
            result = valid_col.bulk_write(valid_operations, ordered=False)
            metrics["inserted_count"] = result.upserted_count
            metrics["updated_count"] = result.modified_count
            metrics["unchanged_count"] = len(valid_operations) - result.upserted_count - result.modified_count
            
            print(f"✅ تم Upsert {len(valid_operations):,} سجل في orders_validated")
            print(f"   - إدراجات جديدة (Upserted): {result.upserted_count:,}")
            print(f"   - تحديثات (Modified): {result.modified_count:,}")
            print(f"   - بدون تغيير (Unchanged): {metrics['unchanged_count']:,}")
        except BulkWriteError as e:
            print(f"خطأ في Upsert: {e.details}")
    
    # ============================================
    # التحقق من قاعدة الاتساق (متطلب 6.11)
    # ============================================
    total_processed = metrics["valid_count"] + metrics["corrected_count"] + metrics["quarantine_count"]
    print(f"\n📊 ملخص المعالجة:")
    print(f"   السجلات السليمة (Valid): {metrics['valid_count']:,}")
    print(f"   السجلات المصححة (Corrected): {metrics['corrected_count']:,}")
    print(f"   السجلات المعزولة (Quarantined): {metrics['quarantine_count']:,}")
    print(f"   الإجمالي: {total_processed:,}")
    
    if total_processed != len(raw_docs):
        print(f"❌ خطأ فادح: عدد السجلات المعالجة ({total_processed}) لا يطابق عدد السجلات الخام ({len(raw_docs)})!")
    else:
        print("✅ قاعدة الاتساق (Raw = Valid + Corrected + Quarantine) محققة بنجاح.")
    
    # 🔥 try/finally لإغلاق MongoDB (متطلب 9)
    try:
        client.close()
    except Exception as e:
        print(f"⚠️ تحذير أثناء إغلاق MongoDB: {e}")
    
    return metrics