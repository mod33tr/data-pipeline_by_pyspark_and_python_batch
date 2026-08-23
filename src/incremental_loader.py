import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, RAW_COLLECTION
from src.elt_pipeline import process_raw_to_final

def load_delta(delta_file_path):
    """
    يقرأ ملف Delta صغير (CSV) ويحقنه في orders_raw كـ Incremental Load،
    ثم يستدعي محرك ELT لمعالجته وعمل Upsert.
    """
    print(f"\n🔄 بدء التحميل التزايدي (Incremental Load) للملف: {delta_file_path}")
    
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    col = db[RAW_COLLECTION]
    
    run_id = str(uuid.uuid4())
    batch = []
    row_count = 0
    
    with open(delta_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            # إضافة Metadata الخاصة بالـ Delta
            raw_doc = {
                "run_id": run_id,
                "source_file": str(Path(delta_file_path).name),
                "source_row_number": row_count,
                "ingested_at": datetime.now(timezone.utc),
                "engine_used": "incremental_loader",
                "load_type": "delta",
                "_delta_timestamp": datetime.now(timezone.utc).isoformat(), # 🔥 Version Handling Marker
                "raw_record": json.dumps(row, ensure_ascii=False)
            }
            batch.append(raw_doc)
            
    if batch:
        col.insert_many(batch, ordered=False)
        print(f"📥 تم حقن {len(batch):,} سجل Delta في orders_raw")
        
    client.close()
    
    # استدعاء محرك ELT لمعالجة الـ Delta وعمل Upsert
    print("🚀 بدء معالجة Delta وتطبيق Upsert...")
    metrics = process_raw_to_final(run_id)
    
    print("\n" + "=" * 50)
    print("🏆 نتائج التحميل التزايدي (Delta Load):")
    print(f"   إدراجات جديدة (Inserted): {metrics.get('inserted_count', 0):,}")
    print(f"   تحديثات (Updated): {metrics.get('updated_count', 0):,}")
    print(f"   بدون تغيير (Unchanged): {metrics.get('unchanged_count', 0):,}")
    print("=" * 50)
    
    return metrics