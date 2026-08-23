import sys
from pathlib import Path
import pytest

# إضافة جذر المشروع للمسار
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.quality_rules import apply_quality_rules

# ============================================================
# اختبار التصنيف: سجل سليم → valid
# ============================================================
def test_classification_valid_record():
    """سجل لا يحتاج أي تعديل يجب أن يُصنف كـ valid"""
    record = {
        "order_id": "ORD-VALID-1",
        "order_date": "2025-01-01T10:00:00",
        "status": "confirmed",          # قيمة موحدة
        "customer_id": "C-001",
        "customer_name": "أحمد محمد",
        "customer_phone": "777123456",
        "customer_email": "ahmed@example.com",
        "city": "صنعاء",
        "district": "شعوب",
        "delivery_type": "سريع",
        "delivery_cost": "5000",
        "payment_method": "wallet",     # قيمة موحدة (بدل محفظة إلكترونية)
        "payment_status": "paid",       # قيمة موحدة (بدل تم الدفع)
        "payment_amount": "50000",
        "currency": "YER",
        "total_amount": "55000",
        "items_json": '[{"sku":"P1","name":"منتج","qty":1,"unit_price":50000,"total":50000}]'
    }
    result = apply_quality_rules(record)
    
    # التصنيف يجب أن يكون "valid"
    assert result["status"] == "valid", f"Expected valid, got {result['status']}"
    
    # لا يجب أن تكون هناك أي تعديلات
    assert len(result["corrections"]) == 0, f"Valid record should have no corrections, but got {result['corrections']}"
    
    # لا يجب أن تكون هناك أي أخطاء
    assert len(result["error_codes"]) == 0, "Valid record should have no errors"
# ============================================================
# اختبار التصنيف: سجل مصحح → corrected
# ============================================================
def test_classification_corrected_record():
    """سجل يحتوي على أخطاء قابلة للتصحيح يجب أن يُصنف كـ corrected"""
    record = {
        "order_id": "ORD-CORRECT-1",
        "order_date": "2025-01-01T10:00:00",
        "status": "مؤكد",  # مسافة زيادة + بالعربي
        "customer_id": "C-002",
        "customer_name": "فاطمة أحمد",
        "customer_phone": "777 123 456",  # مسافات
        "customer_email": "fatima@example.com",
        "city": "عدن",
        "district": "المنصورة",
        "delivery_type": "عادي",
        "delivery_cost": "٢٠٠٠",  # أرقام عربية
        "payment_method": "نقداً عند التسليم",
        "payment_status": "بانتظار الدفع",
        "payment_amount": "20,000",  # فواصل آلاف
        "currency": "YER",
        "total_amount": "22000",
        "items_json": '[{"sku":"P2","name":"منتج","qty":1,"unit_price":20000,"total":20000}]'
    }
    result = apply_quality_rules(record)

    # التصنيف يجب أن يكون "corrected"
    assert result["status"] == "corrected", f"Expected corrected, got {result['status']}"

    # يجب أن تكون هناك تعديلات (Audit Trail)
    assert len(result["corrections"]) > 0, "Corrected record must have corrections"

    # لا يجب أن تكون هناك أخطاء عزل
    assert len(result["error_codes"]) == 0, "Corrected record should not be quarantined"

    # التحقق من أن الأرقام العربية تم تصحيحها
    assert result["record"]["delivery_cost"] == 2000.0, "Arabic numbers should be converted"

    # التحقق من أن فواصل الآلاف تم تصحيحها
    assert result["record"]["payment_amount"] == 20000.0, "Thousands separators should be removed"

# ============================================================
# اختبار التصنيف: سجل معزول → quarantined
# ============================================================
def test_classification_quarantined_record_missing_order_id():
    """سجل بدون معرف طلب يجب أن يُصنف كـ quarantined"""
    record = {
        "order_date": "2025-01-01T10:00:00",
        "status": "confirmed",
        "customer_id": "C-003",
        "customer_name": "علي حسن",
        "items_json": '[{"sku":"P3","name":"منتج","qty":1,"unit_price":10000,"total":10000}]'
    }
    result = apply_quality_rules(record)

    # التصنيف يجب أن يكون "quarantined"
    assert result["status"] == "quarantined", f"Expected quarantined, got {result['status']}"

    # يجب أن يحتوي على رمز خطأ فقدان معرف الطلب
    assert "MISSING_ORDER_ID" in result["error_codes"], "Should have MISSING_ORDER_ID error"

def test_classification_quarantined_record_negative_qty():
    """سجل يحتوي على كمية سالبة يجب أن يُصنف كـ quarantined"""
    record = {
        "order_id": "ORD-NEG-1",
        "order_date": "2025-01-01T10:00:00",
        "status": "confirmed",
        "customer_id": "C-004",
        "customer_name": "خالد عمر",
        "delivery_cost": "5000",
        "payment_amount": "10000",
        "total_amount": "15000",
        "items_json": '[{"sku":"P4","name":"منتج","qty":-3,"unit_price":10000,"total":10000}]'
    }
    result = apply_quality_rules(record)

    # التصنيف يجب أن يكون "quarantined"
    assert result["status"] == "quarantined", f"Expected quarantined, got {result['status']}"

    # يجب أن يحتوي على رمز خطأ القيمة السالبة
    assert "AMBIGUOUS_NEGATIVE_VALUE" in result["error_codes"], "Should have AMBIGUOUS_NEGATIVE_VALUE error"

def test_classification_quarantined_record_corrupted_json():
    """سجل يحتوي على JSON تالف يجب أن يُصنف كـ quarantined"""
    record = {
        "order_id": "ORD-BAD-JSON",
        "order_date": "2025-01-01T10:00:00",
        "status": "confirmed",
        "customer_id": "C-005",
        "customer_name": "منى صالح",
        "delivery_cost": "5000",
        "payment_amount": "10000",
        "total_amount": "15000",
        "items_json": '{"sku":"P5","name":"منتج","qty":1'  # JSON ناقص
    }
    result = apply_quality_rules(record)

    # التصنيف يجب أن يكون "quarantined"
    assert result["status"] == "quarantined", f"Expected quarantined, got {result['status']}"

    # يجب أن يحتوي على رمز خطأ JSON
    assert "CORRUPTED_ITEMS_JSON" in result["error_codes"], "Should have CORRUPTED_ITEMS_JSON error"

# ============================================================
# اختبار قاعدة الاتساق: كل سجل يجب أن ينتهي إلى حالة واحدة فقط
# ============================================================
def test_classification_consistency():
    """كل سجل يجب أن يُصنف إلى حالة واحدة فقط: valid أو corrected أو quarantined"""
    test_records = [
        {"order_id": "ORD-1", "customer_id": "C-1", "status": "confirmed",
         "delivery_cost": "5000", "payment_amount": "10000", "total_amount": "15000",
         "items_json": '[{"sku":"P1","qty":1,"unit_price":10000}]'},
        {"order_id": "ORD-2", "customer_id": "C-2", "status": "مؤكد",
         "delivery_cost": "٥٠٠٠", "payment_amount": "10000", "total_amount": "15000",
         "items_json": '[{"sku":"P2","qty":1,"unit_price":10000}]'},
        {"order_date": "2025-01-01", "customer_id": "C-3",
         "items_json": '[{"sku":"P3","qty":1,"unit_price":10000}]'},
    ]

    for record in test_records:
        result = apply_quality_rules(record)

        # يجب أن يكون هناك حالة واحدة فقط
        assert result["status"] in ["valid", "corrected", "quarantined"], \
            f"Invalid status: {result['status']}"

        # إذا كان معزولاً، يجب أن يحتوي على أخطاء
        if result["status"] == "quarantined":
            assert len(result["error_codes"]) > 0, "Quarantined record must have error codes"

        # إذا كان سليماً، يجب ألا يحتوي على تعديلات أو أخطاء
        if result["status"] == "valid":
            assert len(result["corrections"]) == 0, "Valid record should have no corrections"
            assert len(result["error_codes"]) == 0, "Valid record should have no errors"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])