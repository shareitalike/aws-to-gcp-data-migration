# STAGE 7 — Full System Design Interview

---

## 🎤 COLD SYSTEM DESIGN QUESTION

**[INTERVIEWER]** You've just joined a Series-C fintech company. They process 50 million transactions per day from 200 partner banks. Currently everything is on-prem with batch files. Design a cloud-native data platform on GCP that ingests, processes, and serves this data for both real-time fraud detection and next-day regulatory reporting. You have 40 minutes. Go.

---

**[CANDIDATE]**

"Let me scope this first. We have two consumers with very different latency requirements: fraud detection needs sub-second, regulatory reporting needs daily batch. I'll design for both but they share the same ingestion layer.

**Ingestion:**
Partner banks send batch files — CSV and ISO 20022 XML — via SFTP. I'd stand up a managed SFTP gateway that writes to GCS. Each file lands in a raw bucket, partitioned by `source_bank/date/hour`. A Cloud Function on `OBJECT_FINALIZE` validates the file — schema check, row count against the manifest, and PGP decryption for the encrypted files. Valid files move to a validated zone; corrupt files go to quarantine with a PagerDuty alert.

For the fraud detection path, I'd also set up Pub/Sub streaming — for banks that support real-time push, they send individual transactions to a Pub/Sub topic. This feeds into Dataflow (not Dataproc) because Dataflow handles streaming natively with exactly-once semantics.

**Processing — Batch Path:**
Daily at 2 AM, a Cloud Composer DAG triggers. It spins up ephemeral Dataproc clusters — one per regulatory region because jurisdictions have different transformation rules. The Spark jobs read validated files, apply regulatory transformations (currency normalization, sanctions list matching, counterparty enrichment), and write partitioned Parquet to a processed zone.

Key design decision: I partition by `report_date` and `jurisdiction`, and I use dynamic partition overwrite for idempotency. If a DAG retries, it replaces the partition, not appends.

**Processing — Streaming Path:**
Dataflow reads from Pub/Sub, enriches transactions with a side-input of account metadata (refreshed every 5 minutes from a BQ snapshot), applies a rule engine for fraud scoring, and writes to two sinks: a Bigtable instance for real-time lookups by the fraud API, and a GCS staging area for eventual batch reconciliation.

**Serving:**
BigQuery serves the batch analytics. Tables are partitioned by `report_date`, clustered by `jurisdiction` and `bank_id`. Required partition filters prevent full-table scans. Authorized views scope access — the compliance team sees all fields, the analytics team gets PII masked.

For fraud, Bigtable serves sub-10ms lookups keyed by `account_id + transaction_timestamp`. A fraud scoring API on Cloud Run reads from Bigtable and returns risk scores.

**Orchestration:**
Cloud Composer manages the batch path. The DAG has sensible retries (3×, exponential backoff), SLA alerts if the regulatory report isn't ready by 6 AM, and a quality gate that compares row counts between source manifests and BigQuery.

The streaming path is self-orchestrating — Dataflow manages checkpoints. But I'd add a Cloud Monitoring alert if Pub/Sub unacked messages exceed 100K — that means Dataflow is falling behind.

**Observability:**
Three layers. Infrastructure: cluster health, slot utilization, Pub/Sub backlog. Pipeline: task success rates, processing duration trends, SLA compliance. Data: freshness (time since last record in BQ), completeness (expected banks vs. received), quality scores (null rates, out-of-range values).

Cost governance: BQ slot reservations for predictability. Dataproc preemptible workers for batch savings. GCS lifecycle policies to age out raw data. Budget alerts at 50/80/100%.

The key tradeoff I want to highlight: dual processing paths (batch + streaming) add operational complexity. An alternative is a pure streaming architecture with Dataflow for everything — but regulatory reporting has strict auditability requirements. Batch gives us point-in-time snapshots we can reproduce exactly. Streaming transforms are harder to audit. So the complexity is justified."

---

## 📋 FOLLOW-UP PROBES

**[INTERVIEWER]** Follow-up 1: *What's the worst failure mode in this design?*

**[CANDIDATE]** "The scariest failure is silent data loss in the streaming path. Pub/Sub provides at-least-once delivery, so we handle duplicates with an idempotent write to Bigtable — we use the transaction ID as the row key, so re-processing the same message just overwrites with identical data. But if Dataflow's checkpoint corrupts and it replays from an earlier offset, we might *miss* a window of transactions. Detection: a reconciliation job that runs hourly, comparing Pub/Sub published message counts against Bigtable row inserts per hour. If delta > 0.1%, alert.

The other scary failure is a schema change from a partner bank. If Bank X adds a field or changes a date format, our schema validation catches it and quarantines the file. But if they change the *content* — like switching from UTC to local time — validation passes but downstream calculations break. Prevention: statistical anomaly detection on key fields. If the average transaction amount from Bank X shifts by >2 standard deviations, flag for review."

---

**[INTERVIEWER]** Follow-up 2: *How do you optimize cost? You're spending $50K/month and the CFO wants it at $35K.*

**[CANDIDATE]** "I'd attack the three biggest line items:

First, BigQuery — switch from on-demand to flat-rate reservations. At our query volume, the crossover point is around 6 TB/day. With 500 slots at $10/slot/month, we cap at $5K instead of variable on-demand charges that spiked to $15K last month.

Second, Dataproc — increase preemptible ratio from 60% to 80%. Preemptibles are 80% cheaper. The risk is YARN container loss during preemption, but our jobs are idempotent and Spark handles task re-execution. Average cost drops from $30 to $18 per daily run.

Third, GCS — enforce lifecycle rules aggressively. Raw data older than 14 days should be Nearline (50% savings), older than 30 days should be Coldline (80% savings). Currently we have 90 TB in Standard class that should be in Coldline. That's $2K/month in storage alone.

Fourth, Dataflow — use FlexRS (Flexible Resource Scheduling) for the hourly reconciliation batch jobs. It uses preemptible resources and can delay execution by up to 6 hours in exchange for 40% cost reduction. Since reconciliation isn't time-critical, this is free money.

Combined estimated savings: ~$18K/month, landing us at ~$32K."

---

**[INTERVIEWER]** Follow-up 3: *What's the scaling bottleneck?*

**[CANDIDATE]** "The first bottleneck we'll hit is the Pub/Sub → Dataflow → Bigtable streaming path. At 50M transactions/day (~580 TPS), we're comfortable. At 500M/day (~5,800 TPS), Dataflow autoscaling handles it, but Bigtable write throughput becomes the constraint. Each Bigtable node handles ~10K writes/sec, so we'd need at least 1 node — but with bursty traffic, we'd want 3 nodes with autoscaling.

The second bottleneck is Cloud Composer. At 200 partner banks, we might have 200+ parallel tasks per DAG run. The Airflow scheduler on Composer 2 handles ~300 tasks/minute, but at 10× banks (2,000 tasks), we'd need to shard DAGs — one per regulatory region — and run them on separate Composer environments to avoid scheduler contention.

The third bottleneck is BigQuery slot contention during the morning rush — when both the batch load finishes and analysts start querying. Autoscaling reservations (Enterprise edition) solve this, but they take 60 seconds to scale, so the first queries of the day get slower. Mitigation: pre-warm with a dummy query at 5:55 AM."

---

## ── FINAL SCORECARD ───────────────────

| Dimension | Score | Notes |
|---|---|---|
| **Problem Scoping** | 8/10 | Correctly identified dual-consumer requirement (real-time + batch). Could have asked more clarifying questions about data volume growth rate. |
| **Architecture Clarity** | 9/10 | Clean separation of batch and streaming paths. Good justification for dual architecture. Component choices were well-reasoned. |
| **Failure Awareness** | 7/10 | Good on schema drift and silent data loss. Missed: what happens if the reconciliation job itself fails? Quis custodiet ipsos custodes? Also didn't mention cross-region disaster recovery. |
| **Cost & Scale Thinking** | 8/10 | Excellent cost breakdown with concrete numbers. Missed: data egress costs between services. GCS → BigQuery is free in same region, but Bigtable → Cloud Run incurs network costs at scale. |
| **Communication** | 9/10 | Clear narrative structure, mentioned tradeoffs proactively, used concrete numbers instead of vague estimates. Could have drawn a diagram (whiteboard scenario). |

---

**Overall: 41/50**

### Verdict: **STRONG HIRE** ✅

The candidate demonstrated Staff-level thinking across architecture design, failure analysis, cost optimization, and clear communication. Key strengths were production-mindedness (idempotency, quarantine patterns, defense-in-depth for cost), concrete numbers (not hand-waving), and proactive tradeoff discussion.

---

### Top 3 Gaps

1. **Disaster Recovery** — No mention of cross-region replication or RTO/RPO targets. A Staff engineer should address this without being asked, especially for a fintech handling regulatory data.

2. **Meta-monitoring** — Who monitors the monitors? If PagerDuty is down, if the budget alert Pub/Sub topic drops messages, if the reconciliation job silently stops — there's no watcher-of-the-watchers design. Should have a fully external health check (e.g., simple uptime bot that checks for data freshness from outside GCP).

3. **Data Governance & Lineage** — No mention of Data Catalog, column-level encryption for PII (beyond "authorized views"), audit trails for who queried what, or lineage tracking (does the compliance team know which source file produced which BQ row?). Critical for fintech.

---

## 💣 Failure Pool Audit — All 10 Used ✓

| # | Failure | Stage Used |
|---|---|---|
| 1 | IAM permission denied | Stage 2 |
| 2 | Corrupt Parquet file | Stage 2 (quarantine logic) |
| 3 | BigQuery schema mismatch | Stage 4 |
| 4 | Spark OOM (data skew) | Stage 3 |
| 5 | Partition explosion (BQ cost spike) | Stage 6 |
| 6 | Dataproc autoscaling failure | Stage 5 (covered in DAG cluster teardown) |
| 7 | Duplicate ingestion (Pub/Sub at-least-once) | Stage 7 (streaming design) |
| 8 | Airflow backfill duplication | Stage 5 |
| 9 | GCS lifecycle deletion issue | Stage 6 (lifecycle Terraform) |
| 10 | Wrong partition read (missing filter) | Stage 6 (partition explosion root cause) |

---

## 🏁 SIMULATION COMPLETE

── END ─────────────────────────────
