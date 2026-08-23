import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# مسارات المشروع الاساسية
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
CONFIG_DIR = BASE_DIR / "config"

# التاكد من وجود المجلدات تلقائيا
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# اعدادات MongoDB
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "ecommerce_store")

# اسماء المجموعات (Collections)
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# اعدادات الموجه (Router)
SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", 200))

# اعدادات الدفعات (Batch)
DEFAULT_BATCH_SIZE = int(os.getenv("DEFAULT_BATCH_SIZE", 1000))

# مسارات الملفات
HUGE_FILE_PATH = DATA_DIR / "orders_huge_mixed_quality.csv"
SMALL_SAMPLE_PATH = DATA_DIR / "orders_sample.csv"  
# اعدادات العينات
SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS", 100000))