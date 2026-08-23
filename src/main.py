import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.file_router import choose_engine
from src.mongo_setup import setup_mongo
from src.batch_loader import load_csv_batch
from src.elt_pipeline import process_raw_to_final
from src.metrics import save_metrics
from src.spark_loader import load_csv_spark
from config.settings import HUGE_FILE_PATH, SMALL_SAMPLE_PATH

def main():
    print("=" * 60)
    print("تشغيل خط البيانات الهجين (Hybrid Data Pipeline)")
    print("=" * 60)
    
    target_file = SMALL_SAMPLE_PATH
    if not target_file.exists():
        print("لم يتم العثور على العينة الصغيرة، سنحاول الملف الكبير...")
        target_file = HUGE_FILE_PATH
        
    if not target_file.exists():
        print(f"❌ لا يوجد ملف للمعالجة: {target_file}")
        sys.exit(1)
        
    engine, size_mb = choose_engine(target_file)
    
    run_id = str(uuid.uuid4())
    run_start = datetime.now(timezone.utc)
    
    print(f"\nمعرف التشغيل (run_id): {run_id}")
    print(f"وقت البدء: {run_start.isoformat()}")
    print(f"جاهز لبدء التحميل باستخدام {engine}...")
    print("-" * 60)
    
    raw_metrics = {}
    
    if engine == "python_batch":
        raw_metrics = load_csv_batch(target_file, run_id, engine)
    else:
        # 🔥 Spark يقوم فقط بالتحميل الخام (Raw Load) لتجنب مشاكل Windows
        raw_metrics = load_csv_spark(target_file, run_id, engine)
        
    # 🔄 مرحلة ELT: المعالجة والتنظيف تتم دائماً عبر محرك البايثون الموثوق
    print("\n🚀 بدء مرحلة ELT (التنظيف والتصنيف والـ Upsert)...")
    elt_metrics = process_raw_to_final(run_id)
    
    # حفظ المقاييس النهائية
    save_metrics(run_id, target_file.name, size_mb, engine, raw_metrics, elt_metrics)
    print("🎉 اكتمل خط البيانات بنجاح!")

if __name__ == "__main__":
    main()