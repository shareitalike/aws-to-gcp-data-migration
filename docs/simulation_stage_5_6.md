# STAGE 5 — Orchestration (Airflow / Composer)

## 🔍 PROBE

**[MENTOR]** You've got the ingestion, processing, and loading working. Now wire it together with Airflow on Cloud Composer. Walk me through DAG design, retry strategy, and how you prevent double-processing when Airflow backfills.

**[CANDIDATE]** DAG design:

```
sensor_gcs_files → submit_dataproc_job → load_bq_staging → merge_bq_prod → quality_check
```

- Each task has `retries=3`, `retry_delay=timedelta(minutes=5)`
- Use `catchup=False` to prevent backfill on deploy
- Execution date as partition key ensures idempotency

*Gap: No mention of how to handle partial failures (e.g., Spark succeeds but BQ load fails), no SLA alerting, no task-level callbacks for monitoring.*

## 🔄 REFINE

**[MENTOR]** What happens when `submit_dataproc_job` succeeds, but `load_bq_staging` fails? On retry, Spark runs again — wasting $30 and 90 minutes. How do you make this smarter?

**[CANDIDATE]** Use idempotency markers:

1. **Spark job** writes a `_SUCCESS` marker file to GCS after completion
2. **GCS sensor before Spark** checks for the marker — if it exists, skip Spark
3. Alternatively, use Airflow's `ShortCircuitOperator` to check if processed files already exist
4. For BQ: since MERGE is idempotent, retrying it is safe — just costs a few dollars in slot time

Also add:
- `sla=timedelta(hours=4)` on the final task — if the pipeline hasn't finished by 4 hours, page on-call
- `on_failure_callback` to send Slack alerts per task failure
- `execution_timeout` on Spark task to kill hung jobs

## 📝 ASSIGN — Airflow DAG

**[MENTOR]** Write the full DAG. Production quality — no toy examples.

**[CANDIDATE]**

```python
# dags/daily_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

PROJECT = "company-analytics-prod"
REGION = "us-central1"
CLUSTER_NAME = "ephemeral-daily-{{ ds_nodash }}"
BUCKET = "company-data-lake-validated"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,  # BUG: should be True for sequential
                                # date dependencies — if yesterday failed,
                                # today's join with yesterday's events
                                # produces wrong results
    "email_on_failure": True,
    "email": ["data-eng-oncall@company.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 4,
        "machine_type_uri": "n1-standard-8",
        "disk_config": {"boot_disk_size_gb": 200},
    },
    "secondary_worker_config": {
        "num_instances": 8,
        "is_preemptible": True,
        "machine_type_uri": "n1-standard-8",
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.shuffle.partitions": "2000",
            "spark:spark.dynamicAllocation.enabled": "true",
        },
    },
    "autoscaling_config": {
        "policy_uri": f"projects/{PROJECT}/regions/{REGION}"
                      f"/autoscalingPolicies/daily-pipeline-policy",
    },
}

SPARK_JOB = {
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": (
            "gs://company-data-lake-code/jobs/process_daily_orders.py"
        ),
        "args": ["{{ ds }}"],
        "jar_file_uris": [],
        "python_file_uris": [],
    },
}

def check_processed_exists(**ctx):
    """Skip Spark if already processed (idempotency)."""
    from google.cloud import storage
    client = storage.Client()
    prefix = (f"enriched_orders/process_date={ctx['ds']}/"
              f"_SUCCESS")
    bucket = client.bucket("company-data-lake-processed")
    blob = bucket.blob(prefix)
    if blob.exists():
        logging.info(f"Already processed {ctx['ds']}, skipping Spark")
        return False
    return True

def run_quality_checks(**ctx):
    """Post-load data quality validation."""
    from google.cloud import bigquery
    client = bigquery.Client()
    ds = ctx["ds"]

    checks = {
        "row_count": f"""
            SELECT COUNT(*) as cnt
            FROM `{PROJECT}.analytics.enriched_orders`
            WHERE process_date = '{ds}'
        """,
        "null_pks": f"""
            SELECT COUNT(*) as cnt
            FROM `{PROJECT}.analytics.enriched_orders`
            WHERE process_date = '{ds}' AND order_id IS NULL
        """,
        "amount_sanity": f"""
            SELECT COUNT(*) as cnt
            FROM `{PROJECT}.analytics.enriched_orders`
            WHERE process_date = '{ds}' AND amount < 0
        """,
    }

    for name, query in checks.items():
        result = list(client.query(query).result())
        count = result[0].cnt

        if name == "row_count" and count == 0:
            raise ValueError(f"Quality check '{name}' failed: 0 rows for {ds}")
        if name in ("null_pks", "amount_sanity") and count > 0:
            raise ValueError(
                f"Quality check '{name}' failed: {count} violations on {ds}"
            )

    logging.info(f"All quality checks passed for {ds}")

with DAG(
    dag_id="daily_enriched_orders_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",  # 6 AM UTC daily
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["production", "enriched-orders"],
) as dag:

    check_input = GCSObjectExistenceSensor(
        task_id="check_input_files_exist",
        bucket=BUCKET,
        object=f"source=orders/dt={{{{ ds }}}}/",
        timeout=3600,
        poke_interval=120,
        mode="reschedule",
    )

    check_already_processed = ShortCircuitOperator(
        task_id="check_already_processed",
        python_callable=check_processed_exists,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_spark = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT,
        region=REGION,
        job=SPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,  # delete even if Spark fails
    )

    load_staging = BigQueryInsertJobOperator(
        task_id="load_bq_staging",
        configuration={
            "load": {
                "sourceUris": [
                    "gs://company-data-lake-processed/"
                    "enriched_orders/process_date={{ ds }}/*"
                ],
                "destinationTable": {
                    "projectId": PROJECT,
                    "datasetId": "staging",
                    "tableId": "enriched_orders_staging",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    merge_prod = BigQueryInsertJobOperator(
        task_id="merge_bq_production",
        configuration={
            "query": {
                "query": open(
                    "/home/airflow/gcs/dags/sql/merge_enriched_orders.sql"
                ).read().replace("${RUN_DATE}", "{{ ds }}"),
                "useLegacySql": False,
            }
        },
    )

    quality_check = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    # DAG flow
    (check_input
     >> check_already_processed
     >> create_cluster
     >> submit_spark
     >> delete_cluster
     >> load_staging
     >> merge_prod
     >> quality_check)
```

## 💣 FAILURE INJECTION — Airflow Backfill Duplication

**[MENTOR]** An engineer ran `airflow dags backfill daily_enriched_orders_pipeline -s 2026-03-01 -e 2026-03-15` to reprocess 2 weeks of data. Now BigQuery shows double the expected row counts for those dates.

```
SELECT process_date, COUNT(*) as row_count
FROM `project.analytics.enriched_orders`
WHERE process_date BETWEEN '2026-03-01' AND '2026-03-15'
GROUP BY 1 ORDER BY 1;

process_date | row_count
2026-03-01   | 4,221,880   -- expected ~2.1M
2026-03-02   | 4,156,422   -- expected ~2.1M
...
```

**[CANDIDATE]** Root cause: The MERGE key is `order_id + process_date`, but during reprocessing, the Spark job produced new `row_hash` values (maybe a code change, or timestamp in hash). So the MERGE `ON` clause didn't match existing rows → all rows were `NOT MATCHED` → inserted as duplicates.

**Immediate fix:**
```sql
-- Delete duplicates, keeping the latest
DELETE FROM `project.analytics.enriched_orders` t
WHERE t.process_date BETWEEN '2026-03-01' AND '2026-03-15'
AND t.updated_at < (
    SELECT MAX(t2.updated_at)
    FROM `project.analytics.enriched_orders` t2
    WHERE t2.order_id = t.order_id
    AND t2.process_date = t.process_date
);
```

**Prevention:**
1. Before MERGE, truncate the target partition for the date being loaded: `DELETE FROM target WHERE process_date = '${RUN_DATE}'` — then MERGE acts as a clean insert
2. Or switch to `WRITE_TRUNCATE` per partition for backfills (add an Airflow variable `IS_BACKFILL` that toggles behavior)

**Detection:** Alert if `COUNT(*) / COUNT(DISTINCT order_id)` > 1.05 for any partition — means >5% duplicate rate.

**[MENTOR]** The root cause was actually your `depends_on_past=False` bug too. The backfill ran all 15 dates in parallel, and since there's no date dependency, they stomped on each other. For sequential date processing, `depends_on_past=True` ensures each date finishes before the next starts. Also, `max_active_runs=1` helps but doesn't prevent task-level parallelism within a backfill.

---

## ── DEBRIEF ─────────────────────────────

**[MENTOR]**
- **Score: Passable**
- **Gaps:** `depends_on_past` bug is subtle but critical for backfills. The SQL file `open()` call in the DAG is fragile — should use Jinja template or `BigQueryInsertJobOperator` with a SQL file path. Also, no Slack/PagerDuty callback.

**Tradeoff Questions:**

**1. Why Cloud Composer over self-managed Airflow?**

**[CANDIDATE]** Operational overhead. Self-managed Airflow means managing the metadata DB (Postgres), scheduler HA, worker scaling, and upgrades. Composer handles all of that. The cost premium (~$400/month for a small environment) is justified by saving ~20 hours/month of SRE time.

**2. What breaks at 10× scale?**

**[CANDIDATE]** The metadata DB becomes a bottleneck — 50 DAGs with 20 tasks each and 2-minute poke intervals creates thousands of DB writes/minute. We'd need Composer 2 with Cloud SQL HA, and we'd shard DAGs across multiple Composer environments by domain (e.g., one for ingestion, one for analytics).

**3. How do you explain this to a PM?**

**[CANDIDATE]** "Airflow is the conductor of our data orchestra. It schedules every step — ingesting files, processing them, loading BigQuery — in the right order, at the right time. If anything fails, it retries automatically and alerts us. The backfill feature lets us reprocess historical data when we fix a bug."

── STAGE 5 COMPLETE ─────────────────────

---

# STAGE 6 — Observability + Cost + Hardening

## 🔍 PROBE

**[MENTOR]** The pipeline is running. Now tell me how you know it's healthy. What are you monitoring, what are your SLOs, and how do you control BigQuery costs?

**[CANDIDATE]** Monitoring layers:

1. **Pipeline health:** Airflow task success/failure rates, duration trends
2. **Data quality:** Row count deltas, null rates, freshness (time since last successful load)
3. **Infrastructure:** Dataproc cluster utilization, GCS operations, BQ slot usage
4. **Cost:** BQ on-demand bytes scanned, Dataproc cluster-hours, GCS storage by tier

SLOs:
- Data freshness: data available in BQ within 4 hours of source event
- Pipeline uptime: 99.5% (allows ~36 hours downtime/year)
- Data quality: <0.1% null PKs, <1% row count variance day-over-day

*Gap: No concrete implementation — dashboards, alert thresholds, runbooks.*

## 🔄 REFINE

**[MENTOR]** "Monitor things" is not an answer. Give me three specific alerts with exact thresholds, and tell me what happens when each fires.

**[CANDIDATE]**

| Alert | Condition | Severity | Action |
|---|---|---|---|
| Pipeline SLA breach | DAG `quality_checks` not succeeded by 10AM UTC | P1 (page) | On-call investigates, manual re-trigger |
| BQ cost spike | Daily bytes billed > 150% of 7-day rolling avg | P2 (ticket) | Review query logs, identify rogue queries |
| Data freshness | `MAX(updated_at)` in BQ > 6 hours stale | P1 (page) | Check Airflow logs, GCS file arrival |
| Dataproc failure | Cluster creation failed 2× consecutively | P2 (Slack) | Check quota, zone capacity, fallback zone |
| Quarantine spike | >10 files quarantined in 1 hour | P2 (Slack) | Contact source team, check schema |

## 📝 ASSIGN — Monitoring + Cost Alert Config

**[MENTOR]** Write the Terraform for a Cloud Monitoring alert policy for the pipeline SLA and a BQ budget alert.

**[CANDIDATE]**

```hcl
# terraform/monitoring.tf

# ──── Pipeline SLA Alert ────

resource "google_monitoring_alert_policy" "pipeline_sla" {
  display_name = "Daily Pipeline SLA Breach"
  combiner     = "OR"

  conditions {
    display_name = "Pipeline not completed by SLA"

    condition_threshold {
      filter = <<-EOT
        resource.type = "cloud_composer_environment"
        AND metric.type = "composer.googleapis.com/environment/dagbag_size"
      EOT
      # BUG: Wrong metric — dagbag_size measures number of DAGs,
      # not task completion. Should use a custom metric or
      # log-based metric for task success.
      comparison      = "COMPARISON_LT"
      threshold_value = 1
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.pagerduty.id,
    google_monitoring_notification_channel.slack.id,
  ]

  alert_strategy {
    auto_close = "604800s"  # 7 days
  }
}

resource "google_monitoring_notification_channel" "pagerduty" {
  display_name = "Data Eng PagerDuty"
  type         = "pagerduty"
  labels = {
    service_key = var.pagerduty_service_key
  }
}

resource "google_monitoring_notification_channel" "slack" {
  display_name = "Data Eng Slack"
  type         = "slack"
  labels = {
    channel_name = "#data-eng-alerts"
    auth_token   = var.slack_auth_token
  }
}

# ──── Log-Based Metric for Pipeline Completion ────

resource "google_logging_metric" "pipeline_success" {
  name   = "pipeline/daily_orders_success"
  filter = <<-EOT
    resource.type = "cloud_composer_environment"
    AND jsonPayload.dag_id = "daily_enriched_orders_pipeline"
    AND jsonPayload.task_id = "quality_checks"
    AND jsonPayload.state = "success"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

# ──── BigQuery Cost Alert ────

resource "google_billing_budget" "bq_budget" {
  billing_account = var.billing_account_id
  display_name    = "BigQuery Monthly Budget"

  budget_filter {
    projects = ["projects/${var.project_id}"]
    services = ["services/24E6-581D-38E5"]  # BigQuery service ID
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = "5000"  # $5,000/month cap
    }
  }

  threshold_rules {
    threshold_percent = 0.5   # Alert at 50%
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.8   # Alert at 80%
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0   # Alert at 100%
    spend_basis       = "CURRENT_SPEND"
  }

  all_updates_rule {
    pubsub_topic                     = google_pubsub_topic.budget_alerts.id
    schema_version                   = "1.0"
    monitoring_notification_channels = [
      google_monitoring_notification_channel.slack.id
    ]
  }
}

resource "google_pubsub_topic" "budget_alerts" {
  name = "bq-budget-alerts"
}

# ──── BigQuery Slot Reservation (Cost Governance) ────

resource "google_bigquery_reservation" "analytics" {
  name              = "analytics-pipeline"
  location          = "US"
  slot_capacity     = 500
  edition           = "ENTERPRISE"
  ignore_idle_slots = false
}

resource "google_bigquery_reservation_assignment" "pipeline" {
  assignee    = "projects/${var.project_id}"
  reservation = google_bigquery_reservation.analytics.id
  job_type    = "PIPELINE"
}
```

## 💣 FAILURE INJECTION — Partition Explosion (BQ Cost Spike)

**[MENTOR]** Finance is screaming. Yesterday's BigQuery bill was $12,000 instead of the usual $200. Here's the audit:

```
SELECT
  user_email,
  SUM(total_bytes_billed) / POW(1024,4) AS tb_billed,
  SUM(total_slot_ms) / 1000 / 3600 AS slot_hours
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY 1 ORDER BY 2 DESC;

user_email                          | tb_billed | slot_hours
analytics-dashboard@company.iam     | 18.2      | 342
data-eng-pipeline@company.iam       | 0.8       | 12
```

The dashboard service account scanned 18 TB in a day.

**[CANDIDATE]** Root cause: The dashboard queries are hitting the `enriched_orders` table without a partition filter. A new dashboard was deployed that queries `SELECT * FROM enriched_orders WHERE user_segment = 'premium'` — no `process_date` filter, so it scans the entire table (18 TB).

**Immediate fix:**
1. Add `require_partition_filter = true` to the BigQuery table definition:
```hcl
resource "google_bigquery_table" "enriched_orders" {
  # ...
  time_partitioning {
    type  = "DAY"
    field = "process_date"
    require_partition_filter = true
  }
}
```
2. Contact the dashboard team — their query needs a date range

**Detection:** The budget alert at 50% should have fired at $2,500 — if it didn't, the Pub/Sub subscription wasn't active. Fix: add a dead-letter check on the budget topic.

**Prevention:**
1. `require_partition_filter = true` on all large partitioned tables (enforced via Terraform module)
2. BigQuery custom quotas per service account: limit `analytics-dashboard` to 5 TB/day
3. Query cost estimator in CI: dashboard deployments must include `--dry_run` BQ call to check bytes scanned

**[MENTOR]** Right. I'll add: you should also have **authorized views** for the dashboard team instead of raw table access. The view bakes in a partition filter:

```sql
CREATE OR REPLACE VIEW `project.analytics.enriched_orders_last_90d` AS
SELECT * FROM `project.analytics.enriched_orders`
WHERE process_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
```

Dashboard team can only query this view — partition filter is enforced by design, not policy.

---

## ── DEBRIEF ─────────────────────────────

**[MENTOR]**
- **Score: Passable**
- **Gaps:** The monitoring alert used the wrong metric (dagbag_size instead of a log-based metric for task success). The cost alert didn't catch the spike because Pub/Sub delivery wasn't verified. Should have a "meta-alert" that checks if alerts themselves are firing.

**Tradeoff Questions:**

**1. Why slot reservations over on-demand?**

**[CANDIDATE]** Predictable costs. On-demand charges $6.25/TB scanned — at 18 TB/day, that's $112/day just from one rogue query. With 500 reserved slots at ~$10/slot/month, we pay $5,000/month flat regardless of query volume. It also guarantees slot availability during peak hours — on-demand can get throttled.

**2. What breaks at 10× scale?**

**[CANDIDATE]** 500 slots isn't enough. We'd need flex slots that auto-scale. Also, `INFORMATION_SCHEMA.JOBS_BY_PROJECT` only retains 180 days — long-term cost analysis needs export to a separate audit table. The budget API has a 12-hour lag — real-time cost control needs a custom Cloud Function that tails the BQ audit log.

**3. How do you explain this to a PM?**

**[CANDIDATE]** "We added guardrails to prevent surprise bills. Every query must specify a date range — without it, the system rejects the query. We set a monthly budget with alerts at 50%, 80%, and 100%. And we switched to flat-rate pricing so a single bad query can't blow up costs."

── STAGE 6 COMPLETE ─────────────────────

---

## 🎤 INTERVIEW CHECK (After Stages 5–6)

**[MENTOR]** *Explain the full orchestration and observability stack end-to-end. No bullet points.*

**[CANDIDATE]** "The entire pipeline is orchestrated by Airflow running on Cloud Composer. A single DAG runs daily at 6 AM UTC. It first checks that source files have landed in GCS — using a sensor in reschedule mode so it doesn't block a worker. Then it checks if today's data has already been processed for idempotency. If processing is needed, it spins up an ephemeral Dataproc cluster, submits the Spark job, and tears down the cluster regardless of success or failure — that trigger rule is critical, otherwise a failed Spark job leaves a cluster running and burning money.

After Spark, the DAG loads processed Parquet into BigQuery staging with truncate-and-load, then runs a MERGE into production. The final task runs quality checks — row counts, null PKs, negative amounts.

For observability, we use three layers. First, Airflow sends task failure alerts to Slack and pages on-call for SLA breaches. Second, Cloud Monitoring tracks infrastructure: cluster health, GCS ingestion rates, BQ slot utilization. Third, we have data-level monitoring: freshness checks and quality score trends in a Looker dashboard.

Cost governance uses three mechanisms: BigQuery slot reservations for predictable spend, required partition filters to prevent full-table scans, and billing budget alerts at 50%, 80%, and 100% thresholds. The key lesson from our cost spike incident was defense in depth — a single control like a budget alert can fail, so we layer policy enforcement in the table definition itself."

**[MENTOR]** **Score: Strong.** Good call on defense-in-depth for cost. Follow-up: *If Cloud Composer itself goes down for 6 hours, what happens to your pipeline?*

**[CANDIDATE]** "The data doesn't disappear — it accumulates in GCS. When Composer comes back, we have two options: if `catchup=True` is safe for that date range, we let Airflow catch up automatically. If not, we manually trigger for the missed dates in sequence with `depends_on_past=True` ensuring order. The critical thing is our SLA alert won't fire because it depends on Composer — so we also need an external health check. I'd add an uptime check in Cloud Monitoring that pings the Airflow webserver every 5 minutes, independent of Composer."

── INTERVIEW CHECK COMPLETE ─────────────
