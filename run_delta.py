import sys
from pathlib import Path

# إضافة جذر المشروع إلى مسار بايثون
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.incremental_loader import load_delta

if __name__ == "__main__":
    # سنتحقق من وجود ملف Delta
    delta_file = Path("data/delta_updates.csv")
    
    if not delta_file.exists():
        print(f"❌ لم يتم العثور على ملف Delta: {delta_file}")
        print("الرجاء إنشاء ملف delta_updates.csv في مجلد data/")
        sys.exit(1)
        
    # تشغيل التحميل التزايدي
    load_delta(str(delta_file))