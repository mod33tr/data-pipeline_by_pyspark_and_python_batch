import sys
from pathlib import Path

# 🔥 الحل: إضافة جذر المشروع إلى مسار Python قبل أي استيراد
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from config.settings import (
    MONGODB_URI, DB_NAME,
    RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION
)

# Schema Validation لمجموعة orders_validated
# هذا هو حارس البوابة الذي يضمن أن البيانات النظيفة دائماً صحيحة
orders_validated_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["order_id", "customer_id", "_quality_status", "_run_id"],
        "properties": {
            "order_id": {
                "bsonType": "string",
                "description": "معرف الطلب - مفتاح العمل الأساسي"
            },
            "customer_id": {
                "bsonType": "string",
                "description": "معرف العميل - مطلوب"
            },
            "_quality_status": {
                "bsonType": "string",
                "enum": ["valid", "corrected"],
                "description": "حالة الجودة - يجب أن تكون valid أو corrected فقط"
            },
            "_run_id": {
                "bsonType": "string",
                "description": "معرف التشغيل الذي أنشأ هذه الوثيقة"
            },
            "order_date": {
                "bsonType": ["string", "null"],
                "description": "تاريخ الطلب بصيغة ISO"
            },
            "status": {
                "bsonType": ["string", "null"],
                "description": "حالة الطلب"
            },
            "customer_name": {
                "bsonType": ["string", "null"],
                "description": "اسم العميل"
            },
            "customer_phone": {
                "bsonType": ["string", "null"],
                "description": "هاتف العميل"
            },
            "customer_email": {
                "bsonType": ["string", "null"],
                "description": "بريد العميل الإلكتروني"
            },
            "city": {
                "bsonType": ["string", "null"],
                "description": "المدينة"
            },
            "district": {
                "bsonType": ["string", "null"],
                "description": "المديرية"
            },
            "delivery_type": {
                "bsonType": ["string", "null"],
                "description": "نوع التوصيل"
            },
            "delivery_cost": {
                "bsonType": ["number", "null"],
                "description": "تكلفة التوصيل - يجب أن تكون رقماً"
            },
            "payment_method": {
                "bsonType": ["string", "null"],
                "description": "طريقة الدفع"
            },
            "payment_status": {
                "bsonType": ["string", "null"],
                "description": "حالة الدفع"
            },
            "payment_amount": {
                "bsonType": ["number", "null"],
                "description": "مبلغ الدفع - يجب أن يكون رقماً"
            },
            "currency": {
                "bsonType": ["string", "null"],
                "description": "العملة"
            },
            "total_amount": {
                "bsonType": ["number", "null"],
                "description": "المبلغ الإجمالي - يجب أن يكون رقماً"
            },
            "items": {
                "bsonType": ["array", "null"],
                "description": "عناصر الطلب"
            },
            "_processed_at": {
                "bsonType": ["date", "string"],
                "description": "وقت المعالجة"
            },
            "_corrections": {
                "bsonType": ["array", "null"],
                "description": "تتبع التعديلات - موجود فقط في السجلات المصححة"
            }
        }
    }
}

def setup_mongo():
    print("جاري الاتصال بـ MongoDB...")
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        print("✅ تم الاتصال بنجاح")
    except ConnectionFailure as e:
        print("❌ فشل الاتصال. تاكد ان Mongo شغال")
        sys.exit(1)

    db = client[DB_NAME]

    # حذف المجموعات القديمة عشان نبداء من الصفر
    for col_name in [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION]:
        db.drop_collection(col_name)
        print(f"تم حذف المجموعة القديمة: {col_name}")

    # 1. مجموعة Raw (بدون قيود - تستقبل أي شيء كما وصل)
    db.create_collection(RAW_COLLECTION)
    print(f"✅ تم انشاء {RAW_COLLECTION}")

    # 2. مجموعة Validated (مع Schema Validation + Unique Index)
    db.create_collection(
        VALIDATED_COLLECTION,
        validator=orders_validated_schema,
        validationLevel="moderate",
        validationAction="warn"
    )
    db[VALIDATED_COLLECTION].create_index(
        [("order_id", ASCENDING)],
        unique=True,
        name="uniq_order_id"
    )
    print(f"✅ تم انشاء {VALIDATED_COLLECTION} مع فهرس فريد و Schema Validation")

    # 3. مجموعة العزل
    db.create_collection(QUARANTINE_COLLECTION)
    db[QUARANTINE_COLLECTION].create_index([("run_id", ASCENDING)])
    print(f"✅ تم انشاء {QUARANTINE_COLLECTION}")

    client.close()
    print("✅ تم تجهيز بيئة قاعدة البيانات بنجاح.")

if __name__ == "__main__":
    setup_mongo()