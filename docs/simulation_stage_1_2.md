# 🚀 Staff Data Engineer Simulation — AWS → GCP Migration

---

# STAGE 1 — Architecture & Migration Design

## 🔍 PROBE

**[MENTOR]** You're leading the migration of a 12 TB/day e-commerce analytics platform from AWS to GCP. The current stack is S3 → EMR (Spark) → Redshift. Walk me through your target architecture on GCP — and tell me *why* you'd choose each component, not just *what* it is.

**[CANDIDATE]** Sure. Here's my mapping:

| AWS Component | GCP Component | Why |
|---|---|---|
| S3 | GCS | Native GCP object store, Nearline/Coldline tiers for cost |
| EMR (Spark) | Dataproc | Managed Spark with autoscaling, ephemeral clusters |
| Redshift | BigQuery | Serverless, separation of storage/compute, columnar |
| Airflow (MWAA) | Cloud Composer | Managed Airflow on GKE |
| CloudWatch | Cloud Monitoring + Logging | Integrated GCP observability |

Data flows: Sources → GCS (raw landing) → Dataproc (cleanse/transform) → GCS (processed) → BigQuery (serving). I'd use a medallion architecture: Bronze (raw), Silver (cleansed), Gold (aggregated).

*Gap: No mention of dual-write period, cutover strategy, or data validation during migration.*

## 🔄 REFINE

**[MENTOR]** You jumped straight to the target state. What about the *migration itself*? You can't just flip a switch on 12 TB/day. How do you migrate without data loss, without downtime, and how do you *prove* the new system matches the old one?

**[CANDIDATE]** Right — I'd run a parallel pipeline:

1. **Dual-write phase** — S3 triggers replicate data to GCS via Storage Transfer Service. Both pipelines process simultaneously.
2. **Validation framework** — Row counts, checksums (MD5 on sorted partitions), and golden-query comparison: run the same 50 business-critical queries on Redshift and BigQuery, diff results within tolerance (0.01% for floats).
3. **Cutover** — Once validation passes for 7 consecutive days, switch consumers to BigQuery. Keep Redshift read-only for 30 days as rollback.
4. **Rollback plan** — If BigQuery diverges, re-enable Redshift writes within 15 minutes via DNS-level switching on the BI tool connection strings.

**[MENTOR]** Better. But "MD5 on sorted partitions" is fragile — what if column ordering changes, or nulls are handled differently between Redshift and BigQuery? Use a query-level reconciliation: `SELECT COUNT(*), SUM(amount), COUNT(DISTINCT user_id)` on both sides. Deterministic, engine-agnostic.

---

## ── DEBRIEF ─────────────────────────────

**[MENTOR]**
- **Score: Passable**
- **Gaps:** No mention of network topology (VPN/Interconnect between AWS and GCP), no data classification (PII handling differs across clouds), no cost model for running dual systems.

**Tradeoff Questions:**

**1. Why Dataproc over Dataflow (Beam)?**

**[CANDIDATE]** Dataproc gives us Spark compatibility — our existing EMR jobs are PySpark, so migration is mostly config changes. Dataflow would require rewriting everything in Beam. The team knows Spark. At this scale, the rewrite risk outweighs Dataflow's autoscaling benefits. We can revisit for *new* streaming pipelines later.

**2. What breaks at 10× scale (120 TB/day)?**

**[CANDIDATE]** GCS ingestion is fine — it's virtually unlimited. Dataproc clusters would need careful autoscaling policies and maybe preemptible VMs with fallback. BigQuery's main risk is slot contention — we'd need flex slots or reserved capacity. The real bottleneck is orchestration: Airflow's scheduler becomes a chokepoint. We'd need to shard DAGs or move to a multi-scheduler setup.

**3. How do you explain this to a PM?**

**[CANDIDATE]** "We're moving our data warehouse to Google Cloud. We'll run both systems in parallel for 2 weeks to make sure nothing breaks. Dashboards won't change — they'll just point to a faster, cheaper backend. We expect a 30% cost reduction because BigQuery charges per-query instead of keeping servers running 24/7."

── STAGE 1 COMPLETE ─────────────────────

---

# STAGE 2 — GCS Ingestion Layer

## 🔍 PROBE

**[MENTOR]** Your source systems push ~500 files/hour to S3 in mixed formats (CSV, JSON, Parquet). Design the GCS ingestion layer. I want bucket structure, lifecycle rules, and how you guarantee exactly-once semantics when S3 events can be duplicated.

**[CANDIDATE]** Here's my design:

**Bucket Structure:**
```
gs://company-data-lake-raw/
  ├── source={source_name}/
  │   ├── dt={YYYY-MM-DD}/
  │   │   ├── hr={HH}/
  │   │   │   ├── {source}_{timestamp}_{uuid}.parquet
```

**Lifecycle:** Move to Nearline after 30 days, Coldline after 90, delete after 365.

**Exactly-once:** Use Cloud Functions triggered on GCS `finalize` events. Each function checks a Firestore document keyed by filename — if it exists, skip (dedup). If not, write the key and proceed with validation.

*Gap: No mention of how to handle the S3→GCS transfer itself, no file validation on landing, no poison-queue for bad files.*

## 🔄 REFINE

**[MENTOR]** You skipped the transfer mechanism entirely. Storage Transfer Service? `gsutil rsync`? Custom pipeline? And what happens when a Parquet file lands but it's corrupt — 0 bytes, truncated, wrong schema? Your downstream Spark job will silently produce garbage or crash.

**[CANDIDATE]** Corrected approach:

1. **Transfer:** Storage Transfer Service (STS) for bulk migration, with event-driven sync via S3 event → SQS → Cloud Function → GCS for near-real-time. STS handles retries and checksums natively.
2. **Landing validation (Cloud Function on `finalize`):**
   - Check file size > 0
   - For Parquet: read footer with `pyarrow`, validate schema against a registered schema in Firestore
   - For CSV: validate header row against expected columns
   - **Pass** → move to `gs://...-validated/` prefix
   - **Fail** → move to `gs://...-quarantine/` prefix, alert via PagerDuty
3. **Poison queue:** Quarantined files get a Cloud Monitoring alert. If >5 files from same source quarantine in 1 hour → trigger incident.

## 📝 ASSIGN — Terraform + Validation Code

**[MENTOR]** Write me the Terraform for the GCS bucket with lifecycle rules, and the Cloud Function for landing validation. Production quality.

**[CANDIDATE]** Here's the Terraform:

```hcl
# terraform/gcs_ingestion.tf

resource "google_storage_bucket" "data_lake_raw" {
  name          = "company-data-lake-raw"
  location      = "US"
  storage_class = "STANDARD"
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "production"
    team        = "data-engineering"
  }
}

resource "google_storage_bucket" "data_lake_quarantine" {
  name          = "company-data-lake-quarantine"
  location      = "US"
  storage_class = "STANDARD"
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BUG: Missing IAM binding — Cloud Function service account
# has no permission to read/write these buckets
resource "google_storage_notification" "raw_notification" {
  bucket         = google_storage_bucket.data_lake_raw.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.gcs_notifications.id
  event_types    = ["OBJECT_FINALIZE"]

  depends_on = [google_pubsub_topic_iam_binding.gcs_publisher]
}

resource "google_pubsub_topic" "gcs_notifications" {
  name = "gcs-raw-file-notifications"
}

resource "google_pubsub_topic_iam_binding" "gcs_publisher" {
  topic   = google_pubsub_topic.gcs_notifications.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}

data "google_storage_project_service_account" "gcs_account" {}
```

And the validation Cloud Function:

```python
# cloud_functions/validate_landing/main.py
import functions_framework
from google.cloud import storage, firestore
import pyarrow.parquet as pq
import io, json, logging

logger = logging.getLogger(__name__)
db = firestore.Client()
gcs = storage.Client()

EXPECTED_SCHEMAS = {
    "orders": ["order_id", "user_id", "amount", "currency", "created_at"],
    "events": ["event_id", "user_id", "event_type", "payload", "timestamp"],
}

@functions_framework.cloud_event
def validate_file(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Dedup check
    doc_ref = db.collection("ingestion_dedup").document(file_name)
    if doc_ref.get().exists:
        logger.info(f"Duplicate event for {file_name}, skipping")
        return

    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        # Size check
        blob.reload()
        if blob.size == 0:
            _quarantine(bucket_name, file_name, "Empty file")
            return

        # Schema validation for Parquet
        if file_name.endswith(".parquet"):
            content = blob.download_as_bytes()
            table = pq.read_table(io.BytesIO(content))
            source = file_name.split("/")[0].replace("source=", "")
            expected = EXPECTED_SCHEMAS.get(source)

            if expected:
                actual_cols = table.column_names
                missing = set(expected) - set(actual_cols)
                if missing:
                    _quarantine(bucket_name, file_name,
                                f"Missing columns: {missing}")
                    return

        # Mark as processed (dedup)
        # BUG: Race condition — check-then-set is not atomic.
        # Two concurrent invocations can both pass the check.
        doc_ref.set({"processed_at": firestore.SERVER_TIMESTAMP,
                     "file": file_name})

        # Move to validated prefix
        validated_name = file_name.replace("/raw/", "/validated/")
        bucket.rename_blob(blob, validated_name)
        logger.info(f"Validated: {file_name}")

    except Exception as e:
        _quarantine(bucket_name, file_name, str(e))

def _quarantine(bucket_name, file_name, reason):
    logger.error(f"Quarantine {file_name}: {reason}")
    src_bucket = gcs.bucket(bucket_name)
    dst_bucket = gcs.bucket("company-data-lake-quarantine")
    src_blob = src_bucket.blob(file_name)
    src_bucket.copy_blob(src_blob, dst_bucket, file_name)
    src_blob.delete()
```

## 💣 FAILURE INJECTION — IAM Permission Denied

**[MENTOR]** Your Cloud Function just deployed. Here's what production is showing:

```
2026-03-19T10:15:32Z ERROR validate_file
google.api_core.exceptions.Forbidden: 403 GET
https://storage.googleapis.com/storage/v1/b/company-data-lake-raw/o/source%3Dorders%2Fdt%3D2026-03-19%2Fhr%3D10%2Forders_1710842132_abc123.parquet?alt=media
Request had insufficient authentication scopes.
Caller: cloud-function-sa@project.iam.gserviceaccount.com
Required: storage.objects.get on bucket "company-data-lake-raw"
```

Files are piling up unvalidated. 500 files in the last 30 minutes have not been processed.

**[CANDIDATE]** Root cause: The Cloud Function's service account `cloud-function-sa` doesn't have `roles/storage.objectViewer` on the raw bucket. I noticed the Terraform was missing the IAM binding.

**Fix — add to Terraform:**

```hcl
resource "google_storage_bucket_iam_member" "cf_raw_reader" {
  bucket = google_storage_bucket.data_lake_raw.name
  role   = "roles/storage.objectAdmin"  # needs read + delete + copy
  member = "serviceAccount:cloud-function-sa@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "cf_quarantine_writer" {
  bucket = google_storage_bucket.data_lake_quarantine.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:cloud-function-sa@${var.project_id}.iam.gserviceaccount.com"
}
```

**How to detect automatically?** Cloud Monitoring alert on Cloud Function error rate > 0 for `validate_file`. Also, a "stale file" alert: if `gs://...-raw/` has files older than 15 minutes not moved to validated, fire an alert.

**How to prevent entirely?** Terraform `plan` in CI with a policy check (e.g., Open Policy Agent) that validates every Cloud Function has IAM bindings to its target buckets. Also, integration tests in staging that invoke the function with a test file.

**[MENTOR]** Good diagnosis. One addition: you should also fix the **race condition** I see in your dedup logic. The `get().exists` check followed by `set()` is not atomic. Use a Firestore transaction:

```python
@firestore.transactional
def _dedup_check(transaction, doc_ref, file_name):
    snapshot = doc_ref.get(transaction=transaction)
    if snapshot.exists:
        return True  # duplicate
    transaction.set(doc_ref, {
        "processed_at": firestore.SERVER_TIMESTAMP,
        "file": file_name
    })
    return False  # first time
```

---

## ── DEBRIEF ─────────────────────────────

**[MENTOR]**
- **Score: Strong**
- **Gaps:** Race condition in dedup (caught), no monitoring on quarantine volume, Terraform missing IAM (deliberate bug — good that you caught it). Should have mentioned idempotency of the rename operation (what if rename succeeds but dedup write fails?).

**Tradeoff Questions:**

**1. Why Firestore for dedup over Cloud Memorystore (Redis)?**

**[CANDIDATE]** Firestore is persistent — if the function crashes, the dedup state survives. Redis is faster but volatile. At 500 files/hour, Firestore latency (~20ms) is fine. If we hit 50,000 files/hour, I'd consider Redis with periodic snapshots to Firestore.

**2. What breaks at 10× scale?**

**[CANDIDATE]** The Cloud Function itself has a concurrency limit per instance. At 5,000 files/hour, we'd hit cold-start latency issues. I'd switch to a Cloud Run service with min-instances configured to stay warm, reading from the Pub/Sub subscription.

**3. How do you explain this to a PM?**

**[CANDIDATE]** "Every file that lands gets a health check — is it the right format, right columns, not empty? Bad files get quarantined with an alert. Good files move forward. We never process garbage data, and we never miss a file."

── STAGE 2 COMPLETE ─────────────────────

---

## 🎤 INTERVIEW CHECK (After Stages 1–2)

**[MENTOR]** *System Design Check: Explain what we've built so far end-to-end. No bullet points. Speak like you're in an interview.*

**[CANDIDATE]** "We're migrating a high-volume e-commerce analytics platform from AWS to GCP. The source systems generate roughly 500 files per hour in mixed formats — CSVs, JSON, Parquet — totaling about 12 terabytes daily.

The ingestion layer uses Storage Transfer Service for the initial bulk migration from S3 to GCS, and an event-driven pipeline for ongoing sync. When a file lands in our raw GCS bucket, a Pub/Sub notification triggers a Cloud Function. That function performs three checks: file isn't empty, schema matches our registry, and it hasn't been processed before — we use Firestore for atomic deduplication. Files that pass move to a validated zone; failures go to a quarantine bucket with an alert.

The bucket itself has lifecycle policies — data transitions from Standard to Nearline at 30 days, Coldline at 90, and gets deleted at 365. This saves roughly 60% on storage costs for historical data.

We run both the old AWS pipeline and the new GCP pipeline simultaneously during migration. We validate consistency using deterministic query-level reconciliation — row counts, aggregate sums, distinct counts — across Redshift and BigQuery for the same time windows. Once we see 7 consecutive days of match, we cut over.

The tradeoff I'd highlight: we chose Firestore over Redis for dedup because durability matters more than speed at our current throughput. And we chose Cloud Functions over Cloud Run because sub-second latency isn't critical for batch file processing — but that's a decision we'd revisit if volume grows 10×."

**[MENTOR]** Solid explanation. Follow-up: *What's the blast radius if your Firestore dedup table gets corrupted or deleted?*

**[CANDIDATE]** "Blast radius is re-processing already-validated files. Since our downstream processing is idempotent — it overwrites partitions, not appends — the worst case is wasted compute, not data corruption. To recover, I'd rebuild the dedup table from GCS metadata: list all objects in the validated prefix, extract filenames, and backfill the Firestore collection. Takes maybe 20 minutes for a million files."

**[MENTOR]** **Score: Strong.** Clean narrative, mentioned tradeoffs unprompted, recovery plan was concrete.

── INTERVIEW CHECK COMPLETE ─────────────────────
