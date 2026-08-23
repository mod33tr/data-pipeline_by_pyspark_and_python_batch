import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from config.settings import MONGODB_URI, DB_NAME, RAW_COLLECTION, DEFAULT_BATCH_SIZE

def insert_batch_safely(collection, batch):
    """دالة مساعدة لإدخال الدفعة ومعالجة الأخطاء"""
    if not batch:
        return 0, 0
    try:
        # ordered=False عشان لو في سجل فشل، يكمل باقي السجلات
        result = collection.insert_many(batch, ordered=False)
        return len(result.inserted_ids), 0
    except BulkWriteError as e:
        details = e.details or {}
        inserted = details.get("nInserted", 0)
        rejected = len(details.get("writeErrors", []))
        print(f"⚠️ BulkWriteError: تم إدخال {inserted}، ورفض {rejected}")
        return inserted, rejected

def load_csv_batch(file_path, run_id, engine_name="python_batch"):
    print(f"\n🚀 بدء التحميل الدفعي (Batch Loading) للملف: {Path(file_path).name}")
    
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[RAW_COLLECTION]
    
    batch = []
    inserted_count = 0
    row_number = 0
    rejected_count = 0
    
    start_time = time.perf_counter()
    
    # استخدام DictReader للقراءة كـ Streaming (لا يحمل الملف كاملاً في الذاكرة)
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            row_number += 1
            
            # إضافة الـ Metadata المطلوبة في المتطلب 6.5
            raw_document = {
                "run_id": run_id,
                "source_file": str(Path(file_path).name),
                "source_row_number": row_number,
                "ingested_at": datetime.now(timezone.utc),
                "engine_used": engine_name,
                "raw_record": dict(row) # حفظ السجل الخام كما هو (كل الاعمدة)
            }
            
            batch.append(raw_document)
            
            # إذا وصلت الدفعة للحد الأقصى، ندخلها
            if len(batch) >= DEFAULT_BATCH_SIZE:
                inserted, rejected = insert_batch_safely(collection, batch)
                inserted_count += inserted
                rejected_count += rejected
                batch_num = row_number // DEFAULT_BATCH_SIZE
                print(f"📦 الدفعة {batch_num} | تم إدخال {inserted_count} سجل")
                batch.clear()
                
    # إدخال ما تبقى في الدفعة الأخيرة
    if batch:
        inserted, rejected = insert_batch_safely(collection, batch)
        inserted_count += inserted
        rejected_count += rejected
        
    elapsed = time.perf_counter() - start_time
    throughput = inserted_count / elapsed if elapsed > 0 else 0
    
    print("\n" + "=" * 50)
    print("✅ اكتمل التحميل الدفعي (Raw Load)")
    print(f"إجمالي السجلات المقروءة : {row_number:,}")
    print(f"إجمالي السجلات المدخلة : {inserted_count:,}")
    print(f"السجلات المرفوضة        : {rejected_count:,}")
    print(f"الزمن المستغرق          : {elapsed:.2f} ثانية")
    print(f"معدل المعالجة           : {throughput:,.0f} سجل/ثانية")
    print("=" * 50)
    
    try:
        client.close()
    except Exception as e:
        print(f"⚠️ تحذير أثناء إغلاق MongoDB: {e}")
    
    # إرجاع المقاييس عشان نستخدمها لاحقاً في ملف results.json
    return {
        "rows_read": row_number,
        "raw_loaded": inserted_count,
        "rejected": rejected_count,
        "elapsed_seconds": elapsed,
        "throughput": throughput
    }