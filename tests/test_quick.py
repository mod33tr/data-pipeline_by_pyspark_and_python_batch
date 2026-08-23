import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.quality_rules import apply_quality_rules

# سجل تجريبي يحتوي على أخطاء قابلة للتصحيح
test_record = {
    "order_id": "طلب-100000",
    "order_date": "24/02/2025",  # صيغة غير قياسية
    "status": "مؤكد ",  # مسافة زيادة
    "customer_id": "عميل-0",
    "customer_phone": "777 123 456",  # مسافات
    "customer_email": "user@@mail..com",  # تكرار
    "delivery_cost": "٥٠٠٠",  # أرقام عربية
    "payment_amount": "769,000.00",  # فواصل آلاف
    "total_amount": "769000",
    "items_json": '[{"sku":"SKU-1010","name":"هاتف","qty":1,"unit_price":183000.0}]'
}

result = apply_quality_rules(test_record)
print("Status:", result["status"])
print("Corrections:", len(result["corrections"]))
for c in result["corrections"]:
    print(f"  - {c['field']}: {c['original_value']} → {c['corrected_value']} ({c['rule_code']})")
print("Errors:", result["error_codes"])