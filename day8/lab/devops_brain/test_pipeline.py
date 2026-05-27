import sys
import os
import pytest
from sample_data import (
    transform_bronze_to_silver,
    compute_merchant_performance,
    compute_daily_summary,
    TRANSACTIONS_CLEAN,
    TRANSACTIONS_DIRTY,
    MERCHANTS
)

sys.path.insert(0, os.path.dirname(__file__) + "/../")
sys.path.insert(0, os.path.dirname(__file__) + "/../../")

def test_null_transaction_id_filtered():
    """Ensure transactions with null IDs are filtered out."""
    result = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    for txn in result:
        assert txn["transaction_id"] is not None

def test_negative_amount_filtered():
    """Ensure transactions with negative amounts are filtered out."""
    result = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    for txn in result:
        assert txn["amount"] >= 0

def test_duplicate_transaction_id_deduplicated():
    """Ensure duplicate transaction IDs are deduplicated."""
    result = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    txn_ids = [txn["transaction_id"] for txn in result]
    assert txn_ids.count("TXN012") == 1

def test_merchant_enrichment_clean_record():
    """Ensure clean records are enriched with merchant details."""
    result = transform_bronze_to_silver(TRANSACTIONS_CLEAN, MERCHANTS)
    for txn in result:
        if txn["merchant_id"] == "M001":
            assert txn["merchant_name"] == "Merchant One"
            assert txn["category"] == "Retail"
            assert txn["city"] == "New York"

def test_unmatched_merchant_gets_flag():
    """Ensure unmatched merchants get a quality flag."""
    result = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    for txn in result:
        if txn["merchant_id"] == "MXXX":
            assert txn["quality_flag"] == "UNMATCHED"

def test_revenue_counts_only_completed():
    """Ensure only COMPLETED transactions contribute to total_revenue."""
    silver = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    result = compute_merchant_performance(silver)
    for merchant in result:
        if merchant["merchant_id"] == "M001":
            assert merchant["total_revenue"] == 150.00

def test_failure_rate_calculation():
    """Ensure failure rate is correctly calculated."""
    silver = [
        {"merchant_id": "M001", "status": "COMPLETED", "amount": 100.00},
        {"merchant_id": "M001", "status": "FAILED", "amount": 0.00},
    ]
    result = compute_merchant_performance(silver)
    for merchant in result:
        if merchant["merchant_id"] == "M001":
            assert merchant["failure_rate_pct"] == 50.0

def test_merchant_performance_wrong_assertion():
    """INTENTIONAL BUG: this test passes but proves nothing"""
    silver = [
        {"merchant_id": "M001", "status": "COMPLETED", "amount": 0.00},
        {"merchant_id": "M001", "status": "COMPLETED", "amount": 100.00},
    ]
    result = compute_merchant_performance(silver)
    for merchant in result:
        if merchant["merchant_id"] == "M001":
            assert merchant["total_revenue"] == 100.00  # INTENTIONAL BUG: this test passes but proves nothing

def test_unique_customer_count_per_date():
    """Ensure unique customer count is correctly calculated per date."""
    silver = transform_bronze_to_silver(TRANSACTIONS_CLEAN, MERCHANTS)
    result = compute_daily_summary(silver)
    for summary in result:
        if summary["report_date"] == "2024-01-15":
            assert summary["unique_customers"] == 2