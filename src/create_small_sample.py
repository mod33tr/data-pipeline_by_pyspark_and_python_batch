import sys
import argparse
from pathlib import Path

# 🔥 الحل السحري: إضافة جذر المشروع إلى مسار Python
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import SMALL_SAMPLE_PATH, SAMPLE_ROWS

def create_sample(input_path, rows):
    in_file = Path(input_path)
    if not in_file.exists():
        print(f"❌ الملف غير موجود: {in_file}")
        return
    
    print(f"جاري استخراج عينة بحجم {rows} سطر من {in_file.name}...")
    out_file = SMALL_SAMPLE_PATH
    
    with in_file.open("r", encoding="utf-8") as f_in, out_file.open("w", encoding="utf-8") as f_out:
        header = f_in.readline()
        f_out.write(header)
        
        count = 0
        for line in f_in:
            if count >= rows:
                break
            f_out.write(line)
            count += 1
            
    print(f"✅ تم حفظ العينة في: {out_file}")
    print(f"عدد السجلات: {count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="مسار الملف الكبير")
    parser.add_argument("--rows", type=int, default=SAMPLE_ROWS, help="عدد السجلات")
    args = parser.parse_args()
    
    create_sample(args.input, args.rows)