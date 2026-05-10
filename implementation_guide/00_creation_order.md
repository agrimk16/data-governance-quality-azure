# Creation Order — Data Governance & Quality Framework

## Overview

This project builds a production-grade data governance and quality framework. Follow the guides sequentially.

## Phase 1: Foundation (Guides 01–02)

- [ ] Provision Azure resources (Databricks, ADLS Gen2, SQL, Key Vault)
- [ ] Upload sample data to ADLS Gen2
- [ ] Design quality framework architecture
- [ ] Create metadata tables in Azure SQL

**Cost management:** Databricks auto-terminates after 20 min idle. Azure SQL Basic is ~$5/month.

## Phase 2: Quality Engine (Guides 03–05)

- [ ] Install and configure Great Expectations on Databricks
- [ ] Build expectation suites for each data source
- [ ] Implement data profiling notebooks
- [ ] Build quarantine routing for bad records

**Cost management:** Run Databricks only when developing. Upload results to SQL for persistence.

## Phase 3: Governance & Monitoring (Guides 06–08)

- [ ] Build lineage tracking metadata store
- [ ] Configure Azure Monitor alerts for quality failures
- [ ] Set up data catalog with Microsoft Purview (optional)
- [ ] Create governance dashboards

**Cost management:** Purview is optional (expensive). Log Analytics free tier covers 5GB/month.

## Pause/Resume Pattern

```bash
# Pause (stop billing)
# - Databricks cluster auto-terminates (20 min idle)
# - Azure SQL Basic always runs (~$5/month) — can pause via portal

# Resume
# - Start Databricks cluster from workspace
# - Everything else stays running
```

## Full Teardown

```bash
./setup/teardown_resources.sh    # Deletes entire resource group
```
