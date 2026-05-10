# Data Governance & Quality Framework — Portfolio Summary

## Project Overview

Built a **production-grade data governance and quality framework** on Azure. Automated data quality validation using Great Expectations, implemented a quarantine pattern for bad record isolation, built custom data lineage tracking, and configured SLA monitoring with Azure Monitor alerts.

## Architecture

```
Raw Data → Quality Engine (Databricks + GE) → Validated / Quarantine Split
                    │
                    ▼
           Metadata Store (Azure SQL) → Monitoring (Log Analytics) → Alerts
```

## Key Technical Achievements

### Data Quality Engine

- Integrated **Great Expectations** on Databricks for declarative data validation
- Built expectation suites covering 6 quality dimensions: completeness, accuracy, consistency, uniqueness, timeliness, validity
- Implemented quality scoring system (passed/warning/failed thresholds)
- Persisted all check results and scores to Azure SQL for historical trend analysis

### Data Profiling & Anomaly Detection

- Built custom PySpark profiler computing column-level statistics (null rates, distinct counts, distributions, min/max/mean/median)
- Implemented anomaly detection using Z-score comparison against historical baselines
- Detects drift in null rates, cardinality, and distribution statistics

### Quarantine Pattern

- **Never drop bad data** — route failed records to quarantine container with failure reasons attached
- Quality Router class applies PySpark checks and splits DataFrames by outcome
- Quarantine review workflow enables investigation and reprocessing
- Metrics tracked: quarantine rate per dataset per run

### Data Lineage Tracking

- Custom lineage tracker instrumenting all transformations with context manager pattern
- Captures: source → target, row counts, duration, column-level mappings, status
- Recursive CTE queries for impact analysis (forward lineage) and root cause (backward lineage)
- Column-level lineage with transformation descriptions

### SLA Monitoring & Alerting

- Pipeline SLA tracking: expected completion time vs. actual, delay measurement
- Azure Monitor alert rules for critical quality failures and score drops
- KQL queries in Log Analytics for quality dashboards and trend analysis
- Action groups for email/Teams/PagerDuty notification routing

### Data Classification & Access Governance

- Pattern-based PII detection (SSN, email, phone, credit card, IP)
- Column name heuristics for sensitive data identification
- Role-based access control (RBAC) framework with container-level granularity
- Comprehensive audit trail view across quality, lineage, and SLA events

## Azure SQL Metadata Schema

| Table                  | Purpose                                         |
| ---------------------- | ----------------------------------------------- |
| `dq_check_results`     | Individual check pass/fail with observed values |
| `dq_scores`            | Aggregate quality score per dataset per run     |
| `dq_profiling`         | Column-level statistics per dataset             |
| `dq_lineage`           | Pipeline and transformation lineage records     |
| `dq_column_lineage`    | Column-level source→target mappings             |
| `dq_sla_tracking`      | Pipeline SLA compliance records                 |
| `v_audit_trail`        | Unified audit trail view                        |
| `v_quarantine_metrics` | Quarantine rate trends                          |

## Interview Talking Points

1. **"How do you ensure data quality?"** → Six-dimension framework with Great Expectations for automated validation, quality scoring for trending, and quarantine pattern for isolating bad records
2. **"What happens when bad data arrives?"** → Quarantine, don't drop. Failed records go to quarantine container with failure reasons. Good data flows through. Reprocessing workflow handles fixes.
3. **"How do you track lineage?"** → Custom metadata store with context-manager instrumented transformations. Recursive CTE queries enable impact analysis and root cause tracing. Column-level mappings show exact transformations.
4. **"How do you detect data drift?"** → Statistical profiling with Z-score anomaly detection against rolling historical baselines
5. **"What's your monitoring strategy?"** → Three tiers: metadata store for history, Log Analytics for dashboards, Azure Monitor for real-time alerts with severity-based escalation

## Cost Profile

| Resource                                  | Estimated Cost  |
| ----------------------------------------- | --------------- |
| Databricks (single node, 20min auto-term) | ~$2/hr active   |
| Azure SQL (Basic)                         | ~$5/month       |
| ADLS Gen2                                 | ~$0.02/GB/month |
| Log Analytics (Free tier, 5GB)            | Free            |
| Key Vault                                 | ~$0.03/10K ops  |
| **Daily (active dev)**                    | **~$3–6/day**   |
| **Idle**                                  | **~$0.20/day**  |

## Skills Demonstrated

- Great Expectations (open-source data quality framework)
- Data profiling with statistical anomaly detection (Z-score)
- Quarantine pattern for production data pipelines
- Custom data lineage tracking (pipeline + column level)
- SLA monitoring and alerting
- Azure Monitor, Log Analytics, KQL queries
- PII classification and access governance
- SQL metadata store design with audit trail
- PySpark data engineering
