"""
Stage 7a: Data Quality Checks — standalone validation script.

Runs against BigQuery to validate production data quality.
Can be triggered as a standalone check or from Airflow.

Usage:
  python data_quality_checks.py --date 2026-03-19
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")


class QualityCheck:
    def __init__(self, name, query, assertion):
        self.name = name
        self.query = query
        self.assertion = assertion  # function(result_value) → True/False


def build_checks(run_date):
    """Define all quality checks for a given date."""
    return [
        QualityCheck(
            name="Row Count > 0",
            query=f"""
                SELECT COUNT(*) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}'
            """,
            assertion=lambda v: v > 0,
        ),
        QualityCheck(
            name="No Null Primary Keys",
            query=f"""
                SELECT COUNT(*) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}' AND order_id IS NULL
            """,
            assertion=lambda v: v == 0,
        ),
        QualityCheck(
            name="No Negative Amounts",
            query=f"""
                SELECT COUNT(*) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}' AND amount < 0
            """,
            assertion=lambda v: v == 0,
        ),
        QualityCheck(
            name="Duplicate Rate < 1%",
            query=f"""
                SELECT
                  1.0 - (COUNT(DISTINCT order_id) / COUNT(*)) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}'
            """,
            assertion=lambda v: v < 0.01,
        ),
        QualityCheck(
            name="User Segment Coverage > 5%",
            query=f"""
                SELECT
                  COUNTIF(user_segment IS NOT NULL) / COUNT(*) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}'
            """,
            assertion=lambda v: v > 0.05,
        ),
        QualityCheck(
            name="Amount Range Sanity (avg $1-$1000)",
            query=f"""
                SELECT AVG(amount) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}'
            """,
            assertion=lambda v: 1.0 <= v <= 1000.0,
        ),
        QualityCheck(
            name="Freshness < 24 Hours",
            query=f"""
                SELECT TIMESTAMP_DIFF(
                  CURRENT_TIMESTAMP(),
                  MAX(updated_at),
                  HOUR
                ) as value
                FROM `analytics.enriched_orders`
                WHERE process_date = '{run_date}'
            """,
            assertion=lambda v: v < 24,
        ),
    ]


def main():
    parser = argparse.ArgumentParser(description="Data quality checks")
    parser.add_argument("--date", required=True, help="Date to check (YYYY-MM-DD)")
    args = parser.parse_args()

    print("=" * 60)
    print("  Stage 7a: Data Quality Checks")
    print(f"  Date: {args.date}")
    print("=" * 60)
    print()

    if not PROJECT_ID:
        print("❌ Set GCP_PROJECT_ID in .env!")
        sys.exit(1)

    client = bigquery.Client(project=PROJECT_ID)
    checks = build_checks(args.date)

    passed = 0
    failed = 0

    for check in checks:
        try:
            result = list(client.query(check.query).result())
            value = result[0].value

            if check.assertion(value):
                print(f"  ✅ PASS: {check.name} (value={value})")
                passed += 1
            else:
                print(f"  ❌ FAIL: {check.name} (value={value})")
                failed += 1
        except Exception as e:
            print(f"  ⚠️ ERROR: {check.name} — {e}")
            failed += 1

    print()
    print("─" * 60)
    print(f"  Results: {passed} passed, {failed} failed, "
          f"{len(checks)} total")
    print("─" * 60)

    if failed > 0:
        print("\n  ❌ QUALITY CHECKS FAILED!\n")
        sys.exit(1)
    else:
        print("\n  ✅ ALL QUALITY CHECKS PASSED!\n")


if __name__ == "__main__":
    main()
