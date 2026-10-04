import sys
import uuid
import argparse
from pathlib import Path
from datetime import datetime, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.file_router import choose_engine
from src.batch_loader import load_csv_batch
from src.elt_pipeline import process_raw_to_final
from src.metrics import save_metrics
from src.spark_loader import load_csv_spark
from config.settings import HUGE_FILE_PATH, SMALL_SAMPLE_PATH

def main():
    print("=" * 60)
    print("تشغيل خط البيانات الهجين (Hybrid Data Pipeline)")
    print("=" * 60)

    # 🔥 اختيار الملف والمحرك
    parser = argparse.ArgumentParser(description="Hybrid Data Pipeline Execution")
    parser.add_argument("--file", default=None,
                        help="sample | huge | أو مسار أي ملف CSV مخصص")
    parser.add_argument("--input", default=None,
                        help="مسار ملف CSV مخصص (بديل لـ --file)")
    parser.add_argument("--engine", choices=["auto", "python_batch", "pyspark"], default="auto",
                        help="تحديد المحرك يدوياً أو auto للاختيار التلقائي")
    args = parser.parse_args()

    chosen_file = args.input or args.file or "sample"
    if chosen_file == "huge":
        target_file = HUGE_FILE_PATH
    elif chosen_file == "sample":
        target_file = SMALL_SAMPLE_PATH
    else:
        target_file = Path(chosen_file)
        if not target_file.is_absolute() and not target_file.exists():
            # البحث داخل مجلد data أيضاً
            data_candidate = Path("data") / chosen_file
            if data_candidate.exists():
                target_file = data_candidate

    if not target_file.exists():
        print(f"❌ الملف غير موجود: {target_file}")
        sys.exit(1)

    auto_engine, size_mb = choose_engine(target_file)
    engine = auto_engine if args.engine == "auto" else args.engine
    if args.engine != "auto":
        print(f"⚡ تم فرض المحرك يدوياً: {engine}")

    run_id = str(uuid.uuid4())
    run_start = datetime.now(timezone.utc)

    print(f"\nمعرف التشغيل (run_id): {run_id}")
    print(f"وقت البدء: {run_start.isoformat()}")
    print(f"جاهز لبدء التحميل باستخدام {engine}...")
    print("-" * 60)

    if engine == "python_batch":
        raw_metrics = load_csv_batch(target_file, run_id, engine)
    else:
        raw_metrics = load_csv_spark(target_file, run_id, engine)

    # 🔥 تمرير معرفات التكرار المكتشفة بواسطة Spark (لتوفير الذاكرة في المرحلة الثانية)
    print("\n🚀 بدء مرحلة ELT (التنظيف والتصنيف والـ Upsert)...")
    elt_metrics = process_raw_to_final(run_id, duplicate_ids=raw_metrics.get("duplicate_ids"))

    save_metrics(run_id, target_file.name, size_mb, engine, raw_metrics, elt_metrics)
    print("🎉 اكتمل خط البيانات بنجاح!")

if __name__ == "__main__":
    main()