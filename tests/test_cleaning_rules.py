import sys
from pathlib import Path
import pytest
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.quality_rules import apply_quality_rules

def test_valid_record():
    record = {
        "order_id": "ORD-1",
        "order_date": "2025-01-01T10:00:00",
        "status": "confirmed",
        "customer_id": "C-1",
        "customer_phone": "777123456",
        "customer_email": "test@example.com",
        "delivery_cost": "5000",
        "payment_amount": "10000",
        "total_amount": "15000",
        "items_json": '[{"sku":"S1","qty":1,"unit_price":10000}]'
    }
    result = apply_quality_rules(record)
    assert result["status"] == "valid"
    assert len(result["corrections"]) == 0
    assert len(result["error_codes"]) == 0

def test_arabic_numbers_and_email_correction():
    record = {
        "order_id": "ORD-2",
        "order_date": "2025-01-01T10:00:00",
        "status": "مؤكد",
        "customer_id": "C-2",
        "customer_phone": "777 123",
        "customer_email": "user@@mail..com",
        "delivery_cost": "٥٠٠٠",
        "payment_amount": "10,000",
        "total_amount": "15000",
        "items_json": '[{"sku":"S1","qty":1,"unit_price":10000}]'
    }
    result = apply_quality_rules(record)
    
    assert result["status"] == "corrected"
    assert result["record"]["delivery_cost"] == 5000.0
    assert result["record"]["payment_amount"] == 10000.0
    assert result["record"]["customer_email"] == "user@mail.com"
    assert result["record"]["status"] == "confirmed"
    assert len(result["corrections"]) > 0

def test_quarantine_negative_qty():
    record = {
        "order_id": "ORD-3",
        "order_date": "2025-01-01T10:00:00",
        "status": "confirmed",
        "customer_id": "C-3",
        "delivery_cost": "5000",
        "payment_amount": "10000",
        "total_amount": "15000",
        "items_json": '[{"sku":"S1","qty":-2,"unit_price":10000}]'
    }
    result = apply_quality_rules(record)
    
    assert result["status"] == "quarantined"
    assert "AMBIGUOUS_NEGATIVE_VALUE" in result["error_codes"]

def test_quarantine_missing_order_id():
    record = {
        "order_date": "2025-01-01T10:00:00",
        "customer_id": "C-4",
        "items_json": '[{"sku":"S1","qty":1,"unit_price":10000}]'
    }
    result = apply_quality_rules(record)
    
    assert result["status"] == "quarantined"
    assert "MISSING_ORDER_ID" in result["error_codes"]

if __name__ == "__main__":
    pytest.main()