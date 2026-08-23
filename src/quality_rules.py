import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

# القاموس القياسي للحالات (للـ Trim والتوحيد)
STATUS_DICT = {
    "مؤكد": "confirmed",
    "قيد الانتظار": "pending",
    "مدفوع": "paid",
    "pending": "pending",
    "paid": "paid",
    "confirmed": "confirmed",
    "شحن": "shipped",
    "shipped": "shipped"
}

PAYMENT_STATUS_DICT = {
    "تم الدفع": "paid",
    "بانتظار الدفع": "pending",
    "paid": "paid",
    "pending": "pending"
}

# خريطة الأرقام العربية إلى اللاتينية
ARABIC_DIGITS = {
    '٠': '0', '١': '1', '٢': '2', '٣': '3', '٤': '4',
    '٥': '5', '٦': '6', '٧': '7', '٨': '8', '٩': '9'
}

def _arabic_to_latin(value: str) -> str:
    """القاعدة 1: تحويل الأرقام العربية إلى لاتينية"""
    if not isinstance(value, str):
        return value
    for ar, la in ARABIC_DIGITS.items():
        value = value.replace(ar, la)
    return value

def _parse_price_to_number(value):
    """تحويل أي قيمة سعر إلى رقم عائم، مع دعم الأرقام العربية والكلمات"""
    if value is None or value == "":
        return None, False
    
    if isinstance(value, (int, float)):
        return float(value), False
    
    value = str(value).strip()
    changed = False # 🔥 متغير لتتبع التغيير الفعلي فقط
    
    # القاعدة 1: الأرقام العربية
    if any(c in value for c in ARABIC_DIGITS):
        value = _arabic_to_latin(value)
        changed = True
    
    # القاعدة 4: السعر بالكلمات (قيم معروفة فقط)
    words_map = {
        "ألف": 1000, "الف": 1000,
        "ألفان": 2000, "الفان": 2000,
        "خمسة آلاف": 5000, "خمسة آلالف": 5000,
        "عشرة آلاف": 10000, "عشرة آلالف": 10000
    }
    lower_val = value.strip()
    if lower_val in words_map:
        return float(words_map[lower_val]), True
    
    # القاعدة 3: إزالة فواصل الآلاف
    if "," in value:
        value = value.replace(",", "")
        changed = True
    
    # القاعدة 2: إزالة رموز العملة
    keywords = ["ريال", "ريالا", "لاير", "لايرا", "يمني", "YER", "USD", "ريال يمني"]
    for keyword in keywords:
        if keyword in value:
            value = value.replace(keyword, "")
            changed = True
    
    value = value.strip()
    
    try:
        # 🔥 نرجع متغير changed بدلاً من المقارنة الخاطئة
        return float(value), changed
    except (ValueError, InvalidOperation):
        return None, False
    

def _normalize_phone(phone):
    """القاعدة 5: توحيد رقم الهاتف"""
    if not phone:
        return phone, False
    phone = str(phone).strip()
    # إزالة المسافات والشرطات
    cleaned = re.sub(r"[\s\-\(\)]", "", phone)
    return cleaned, (cleaned != phone)

def _normalize_email(email):
    """القاعدة 6: إصلاح تكرار الرموز في البريد"""
    if not email:
        return email, False, False
    email = str(email).strip()
    original = email
    # إصلاح التكرار الواضح: user@@mail..com → user@mail.com
    email = re.sub(r"@{2,}", "@", email)
    email = re.sub(r"\.{2,}", ".", email)
    
    is_valid = bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))
    changed = (email != original)
    return email, changed, is_valid

def _normalize_date(date_val):
    """القاعدة 7: تحويل التاريخ إلى صيغة ISO قياسية"""
    if not date_val:
        return None, False, True
    
    date_str = str(date_val).strip()
    
    # إذا كانت ISO بالفعل
    try:
        datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return date_str, False, True
    except ValueError:
        pass
    
    # محاولة صيغ أخرى شائعة
    formats = ["%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # التحقق من المنطقية (مثلاً لا يوجد يوم 32)
            return dt.isoformat(), True, True
        except ValueError:
            continue
    
    return None, False, False

def _normalize_status(value, dictionary):
    """القاعدة 8: Trim وتوحيد القيم إلى القاموس"""
    if not value:
        return value, False
    value = str(value).strip()
    normalized = dictionary.get(value, value)
    return normalized, (normalized != value)

def _parse_items_json(items_str):
    """تحليل عمود items_json والتحقق منه"""
    if not items_str:
        return None, "EMPTY_ITEMS"
    
    try:
        items = json.loads(items_str)
        if not isinstance(items, list) or len(items) == 0:
            return None, "EMPTY_ITEMS"
        
        # التحقق من الكميات السالبة
        for item in items:
            qty = item.get("qty")
            if isinstance(qty, (int, float)) and qty < 0:
                return items, "AMBIGUOUS_NEGATIVE_VALUE"
        
        return items, None
    except json.JSONDecodeError:
        return None, "CORRUPTED_ITEMS_JSON"

def apply_quality_rules(raw_record: dict) -> dict:
    """
    تطبق جميع القواعد على سجل خام واحد.
    ترجع قاموساً يحتوي:
      - record: السجل بعد التعديل
      - corrections: قائمة التعديلات (Audit Trail)
      - error_codes: قائمة الأخطاء غير القابلة للتصحيح
      - status: "valid" | "corrected" | "quarantined"
    """
    corrections = []
    error_codes = []
    record = dict(raw_record)  # نسخة للتعديل
    
    # === التحقق من الحقول الجوهرية أولاً ===
    if not record.get("order_id"):
        error_codes.append("MISSING_ORDER_ID")
    
    if not record.get("customer_id"):
        error_codes.append("MISSING_CUSTOMER_ID")
    
    # === القاعدة 1 & 3 & 4: تحويل الأسعار والمبالغ ===
    price_fields = ["delivery_cost", "payment_amount", "total_amount"]
    for field in price_fields:
        if field in record:
            parsed, changed = _parse_price_to_number(record[field])
            if parsed is None and record[field]:
                error_codes.append("UNKNOWN_PRICE")
            else:
                if changed:
                    corrections.append({
                        "field": field,
                        "original_value": str(record[field]),
                        "corrected_value": parsed,
                        "rule_code": "PRICE_NORMALIZATION"
                    })
                record[field] = parsed
    
    # === القاعدة 5: رقم الهاتف ===
    if record.get("customer_phone"):
        new_phone, changed = _normalize_phone(record["customer_phone"])
        if changed:
            corrections.append({
                "field": "customer_phone",
                "original_value": raw_record["customer_phone"],
                "corrected_value": new_phone,
                "rule_code": "PHONE_FORMAT"
            })
        record["customer_phone"] = new_phone
    
    # === القاعدة 6: البريد الإلكتروني ===
    if record.get("customer_email"):
        new_email, changed, is_valid = _normalize_email(record["customer_email"])
        if changed:
            corrections.append({
                "field": "customer_email",
                "original_value": raw_record["customer_email"],
                "corrected_value": new_email,
                "rule_code": "EMAIL_REPEATED_SYMBOLS"
            })
        if not is_valid and record.get("customer_email"):
            error_codes.append("INVALID_EMAIL")
        record["customer_email"] = new_email
    
    # === القاعدة 7: التاريخ ===
    if record.get("order_date"):
        new_date, changed, is_valid = _normalize_date(record["order_date"])
        if changed:
            corrections.append({
                "field": "order_date",
                "original_value": raw_record["order_date"],
                "corrected_value": new_date,
                "rule_code": "DATE_FORMAT"
            })
        if not is_valid:
            error_codes.append("INVALID_IMPOSSIBLE_DATE")
        record["order_date"] = new_date
    
    # === القاعدة 8: توحيد الحالات ===
    if record.get("status"):
        new_status, changed = _normalize_status(record["status"], STATUS_DICT)
        if changed:
            corrections.append({
                "field": "status",
                "original_value": raw_record["status"],
                "corrected_value": new_status,
                "rule_code": "STATUS_NORMALIZATION"
            })
        record["status"] = new_status
    
    if record.get("payment_status"):
        new_pstatus, changed = _normalize_status(record["payment_status"], PAYMENT_STATUS_DICT)
        if changed:
            corrections.append({
                "field": "payment_status",
                "original_value": raw_record["payment_status"],
                "corrected_value": new_pstatus,
                "rule_code": "PAYMENT_STATUS_NORMALIZATION"
            })
        record["payment_status"] = new_pstatus
    
    # === تحليل JSON للـ items ===
    items_str = record.get("items_json")
    parsed_items, item_error = _parse_items_json(items_str)
    if item_error:
        error_codes.append(item_error)
    record["items"] = parsed_items
    if parsed_items is not None:
        del record["items_json"]  # استبدلنا النص بـ parsed array
    
    # === تحديد الحالة النهائية ===
    if error_codes:
        status = "quarantined"
    elif corrections:
        status = "corrected"
    else:
        status = "valid"
        
        
        
        # === القاعدة 9: إعادة حساب إجمالي الطلب ===
    if record.get("items") and record.get("delivery_cost") is not None:
        try:
            items_total = 0.0
            items_valid = True
            for item in record["items"]:
                qty = item.get("qty")
                price = item.get("unit_price")
                if qty is None or price is None or not isinstance(qty, (int, float)) or not isinstance(price, (int, float)):
                    items_valid = False
                    break
                if qty < 0:  # كمية سالبة = غير صالحة للحساب
                    items_valid = False
                    break
                items_total += qty * price
            
            delivery_cost = record.get("delivery_cost")
            if items_valid and isinstance(delivery_cost, (int, float)):
                expected_total = items_total + delivery_cost
                current_total = record.get("total_amount")
                
                # مقارنة مع هامش بسيط لتجنب أخطاء الفاصلة العائمة
                if current_total is not None and abs(expected_total - float(current_total)) > 0.01:
                    corrections.append({
                        "field": "total_amount",
                        "original_value": str(current_total),
                        "corrected_value": expected_total,
                        "rule_code": "TOTAL_RECALCULATION"
                    })
                    record["total_amount"] = expected_total
        except Exception:
            pass  # إذا فشل الحساب، نتجاهل ولا نعزل السجل
    
    # === التحقق من التكرار في نفس التشغيل (سيتم في pipeline) ===
    
        # === القاعدة الصارمة: الأخطاء المتضاربة (القسم 6.8) ===
    # إذا تجمع خطآن جوهريان أو أكثر، يصبح السجل غير قابل للتصحيح الآمن
    if len(error_codes) >= 2:
        if "MULTIPLE_CONFLICTING_ERRORS" not in error_codes:
            error_codes.append("MULTIPLE_CONFLICTING_ERRORS")
            # إجبار الحالة على العزل إذا لم تكن معزولة بالفعل
            status = "quarantined" 
    return {
        "record": record,
        "corrections": corrections,
        "error_codes": error_codes,
        "status": status
    }