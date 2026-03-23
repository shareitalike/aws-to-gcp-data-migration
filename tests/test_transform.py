import sys
import os
from pathlib import Path
import pytest
from pyspark.sql import Row

# Add parent directory to sys.path to allow importing from numbered folders
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Dynamic import because of the '04_' prefix
import importlib
process_daily_orders = importlib.import_module("04_spark_processing.process_daily_orders")
process = process_daily_orders.process

def test_process_deduplication(spark):
    """Verify that duplicate order_ids are removed."""
    # Create test data (2 duplicate order_ids)
    orders_data = [
        Row(order_id="ORD1", user_id="U1", amount=100.0, currency="USD", status="COMPLETED"),
        Row(order_id="ORD1", user_id="U1", amount=100.0, currency="USD", status="COMPLETED")
    ]
    events_data = [Row(user_id="U1", event_id="E1", event_type="VIEW", dt="2026-03-19")]
    segments_data = [Row(user_id="U1", segment="VIP", lifetime_value=500.0, country="USA")]
    
    orders = spark.createDataFrame(orders_data)
    events = spark.createDataFrame(events_data)
    segments = spark.createDataFrame(segments_data)
    
    # Run transformation
    enriched = process(orders, events, segments, "2026-03-19")
    
    # Assert
    assert enriched.count() == 1
    assert enriched.collect()[0]["order_id"] == "ORD1"

def test_process_amount_filter(spark):
    """Verify that negative amounts are filtered out."""
    orders_data = [
        Row(order_id="ORD1", user_id="U1", amount=100.0, currency="USD", status="COMPLETED"),
        Row(order_id="ORD2", user_id="U2", amount=-50.0, currency="USD", status="COMPLETED")
    ]
    events_data = [Row(user_id="U1", event_id="E1", event_type="VIEW", dt="2026-03-19")]
    segments_data = [Row(user_id="U1", segment="VIP", lifetime_value=500.0, country="USA")]
    
    # Note: process() expects users for both orders or it might fail on left join if no user records
    # But usually we'd have them. For this test, we just check count.
    
    orders = spark.createDataFrame(orders_data)
    events = spark.createDataFrame(events_data)
    segments = spark.createDataFrame(segments_data)
    
    # Run transformation
    enriched = process(orders, events, segments, "2026-03-19")
    
    # Assert
    assert enriched.count() == 1
    assert enriched.collect()[0]["amount"] == 100.0
