from pathlib import Path
from config.settings import SMALL_FILE_THRESHOLD_MB

def get_file_size_mb(file_path):
    p = Path(file_path)
    if not p.exists():
        return 0
    return p.stat().st_size / (1024 * 1024)

def choose_engine(file_path):
    size_mb = get_file_size_mb(file_path)
    print(f"حجم الملف: {size_mb:.2f} MB")
    print(f"الحد الفاصل: {SMALL_FILE_THRESHOLD_MB} MB")
    
    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = "الملف صغير، سنستخدم Python Batch (Streaming)"
    else:
        engine = "pyspark"
        reason = "الملف كبير، سنستخدم Apache Spark للمعالجة المتوازية"
        
    print(f"المحرك المختار: {engine}")
    print(f"السبب: {reason}")
    return engine, size_mb