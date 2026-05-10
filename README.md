# Project 6: Data Governance & Quality Framework on Azure

## Overview

A **production-grade data governance and quality framework** built on Azure that demonstrates data quality validation, data lineage tracking, access governance, and monitoring — the operational backbone that separates hobby projects from enterprise-ready data platforms.

## Architecture

```
                    ┌──────────────────────────────┐
                    │      Azure Monitor /          │
                    │      Log Analytics            │
                    │   (Centralized Monitoring)    │
                    └──────────┬───────────────────┘
                               │
    ┌──────────────────────────┼──────────────────────────┐
    │                          │                          │
    ▼                          ▼                          ▼
┌────────────┐     ┌───────────────────┐     ┌───────────────────┐
│  Microsoft │     │    Databricks     │     │    Azure SQL      │
│  Purview   │     │  (Quality Engine) │     │  (Metadata Store) │
│            │     │                   │     │                   │
│ - Catalog  │     │ - Great Expect.   │     │ - Quality Results │
│ - Lineage  │     │ - Custom Checks   │     │ - Lineage Records │
│ - Glossary │     │ - Profiling       │     │ - Audit Trail     │
│ - Classify │     │ - Anomaly Detect  │     │ - SLA Tracking    │
└────────────┘     └───────────────────┘     └───────────────────┘
    │                      │                          │
    └──────────────┬───────┘                          │
                   ▼                                  │
          ┌─────────────────┐                         │
          │   ADLS Gen2     │◄────────────────────────┘
          │  (Data Lake)    │
          │                 │
          │  raw/           │
          │  validated/     │
          │  quarantine/    │
          └─────────────────┘
                   │
                   ▼
          ┌─────────────────┐
          │   Azure         │
          │   Key Vault     │
          │  (Secrets/Certs)│
          └─────────────────┘
```

## What You'll Build

| Component          | Technology                    | Purpose                                            |
| ------------------ | ----------------------------- | -------------------------------------------------- |
| Quality Engine     | Great Expectations + PySpark  | Automated data validation with expectations suites |
| Quality Dashboard  | Azure SQL + views             | Track quality scores, trends, SLA compliance       |
| Data Profiling     | PySpark custom framework      | Column-level statistics, anomaly detection         |
| Lineage Tracking   | Custom metadata store         | Track data flow from source to consumption         |
| Data Catalog       | Microsoft Purview (optional)  | Asset discovery, classification, glossary          |
| Access Governance  | RBAC + policies               | Role-based data access with audit trail            |
| Monitoring         | Azure Monitor + Log Analytics | Alerts on quality failures, pipeline SLAs          |
| Quarantine Pattern | ADLS Gen2 routing             | Bad records isolated for review, not lost          |

## Why This Project?

Every interview for a senior data engineering role asks:

- _"How do you ensure data quality in production?"_
- _"How do you track data lineage?"_
- _"What happens when bad data enters your pipeline?"_
- _"How do you monitor pipeline health and SLAs?"_

This project gives you concrete, code-backed answers to all of these.

## Skills Covered

| Skill                    | Interview Relevance                                    |
| ------------------------ | ------------------------------------------------------ |
| Great Expectations       | Most popular open-source quality framework             |
| Data Profiling           | Column statistics, null rates, distribution analysis   |
| Quarantine Pattern       | Production pattern for handling bad records            |
| Data Lineage             | End-to-end tracking from source to report              |
| SLA Monitoring           | Pipeline health metrics and alerting                   |
| Microsoft Purview        | Enterprise data catalog and classification             |
| Custom Quality Framework | Shows system design thinking                           |
| Anomaly Detection        | Statistical methods (Z-score, IQR) for drift detection |

## Estimated Azure Costs

| Resource                                 | Cost                  |
| ---------------------------------------- | --------------------- |
| Databricks (single node, auto-terminate) | ~$2/hr active         |
| Azure SQL (Basic tier)                   | ~$5/month             |
| ADLS Gen2                                | ~$0.02/GB/month       |
| Log Analytics                            | Free tier (5GB/month) |
| Key Vault                                | ~$0.03/10K operations |
| Purview (optional)                       | ~$0.25/hr (can skip)  |
| **Daily (active dev)**                   | **~$3–6/day**         |
| **Idle**                                 | **~$0.20/day**        |

## Getting Started

```bash
# 1. Configure variables
cp setup/variables.sh.template setup/variables.sh
nano setup/variables.sh

# 2. Provision resources
chmod +x setup/provision_resources.sh
./setup/provision_resources.sh

# 3. Follow implementation guides in order
ls implementation_guide/
```

## Implementation Order

| #   | Guide                                                                 | Time   |
| --- | --------------------------------------------------------------------- | ------ |
| 0   | [Creation Order](implementation_guide/00_creation_order.md)           | 5 min  |
| 1   | [Environment Setup](implementation_guide/01_environment_setup.md)     | 30 min |
| 2   | [Quality Framework](implementation_guide/02_quality_framework.md)     | 60 min |
| 3   | [Great Expectations](implementation_guide/03_great_expectations.md)   | 60 min |
| 4   | [Data Profiling](implementation_guide/04_data_profiling.md)           | 45 min |
| 5   | [Quarantine Pattern](implementation_guide/05_quarantine_pattern.md)   | 45 min |
| 6   | [Lineage Tracking](implementation_guide/06_lineage_tracking.md)       | 45 min |
| 7   | [Monitoring & Alerts](implementation_guide/07_monitoring_alerts.md)   | 45 min |
| 8   | [Catalog & Governance](implementation_guide/08_catalog_governance.md) | 45 min |
