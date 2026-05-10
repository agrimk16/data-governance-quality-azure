# Guide 02: Quality Framework Design

## The Quality Problem

In production data pipelines, bad data silently corrupts downstream analytics. This guide designs a framework that **catches issues before they reach consumers**.

## Framework Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Raw Data   │────▶│  Quality Engine   │────▶│   Validated      │
│  (ADLS)     │     │  (Databricks)     │     │   (ADLS)         │
└─────────────┘     │                   │     └──────────────────┘
                    │  ┌─────────────┐  │
                    │  │ Expectations│  │     ┌──────────────────┐
                    │  │ Profiling   │  │────▶│   Quarantine     │
                    │  │ Rules       │  │     │   (ADLS)         │
                    │  └─────────────┘  │     └──────────────────┘
                    │         │         │
                    └─────────┼─────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Metadata Store  │
                    │  (Azure SQL)     │
                    │  - Results       │
                    │  - Scores        │
                    │  - Lineage       │
                    └──────────────────┘
```

## Quality Dimensions

Every data quality check maps to one of six dimensions:

| Dimension        | What It Checks                | Example                                    |
| ---------------- | ----------------------------- | ------------------------------------------ |
| **Completeness** | Missing values, null rates    | order_id is never null                     |
| **Accuracy**     | Values within valid ranges    | price > 0, age between 0–120               |
| **Consistency**  | Cross-field/cross-table rules | order_date ≤ ship_date                     |
| **Uniqueness**   | No duplicates                 | order_id is unique                         |
| **Timeliness**   | Data freshness                | File arrived within 2 hours of schedule    |
| **Validity**     | Format and pattern compliance | email matches pattern, state in valid list |

## Metadata Schema

Create the metadata store in Azure SQL:

```sql
-- File: sql/create_metadata_tables.sql

-- Quality check results
CREATE TABLE dq_check_results (
    result_id         INT IDENTITY(1,1) PRIMARY KEY,
    run_id            NVARCHAR(50) NOT NULL,
    run_timestamp     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    dataset_name      NVARCHAR(200) NOT NULL,
    check_name        NVARCHAR(200) NOT NULL,
    check_type        NVARCHAR(50) NOT NULL,    -- completeness, accuracy, etc.
    column_name       NVARCHAR(100),
    expectation       NVARCHAR(500),
    passed            BIT NOT NULL,
    observed_value    NVARCHAR(200),
    expected_value    NVARCHAR(200),
    failed_count      INT DEFAULT 0,
    total_count       INT DEFAULT 0,
    failure_percent   DECIMAL(5,2),
    severity          NVARCHAR(20) DEFAULT 'warning'  -- critical, warning, info
);

-- Quality scores per dataset per run
CREATE TABLE dq_scores (
    score_id          INT IDENTITY(1,1) PRIMARY KEY,
    run_id            NVARCHAR(50) NOT NULL,
    run_timestamp     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    dataset_name      NVARCHAR(200) NOT NULL,
    total_checks      INT NOT NULL,
    passed_checks     INT NOT NULL,
    failed_checks     INT NOT NULL,
    quality_score     DECIMAL(5,2) NOT NULL,   -- percentage
    status            NVARCHAR(20) NOT NULL     -- passed, warning, failed
);

-- Data profiling results
CREATE TABLE dq_profiling (
    profile_id        INT IDENTITY(1,1) PRIMARY KEY,
    run_id            NVARCHAR(50) NOT NULL,
    run_timestamp     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    dataset_name      NVARCHAR(200) NOT NULL,
    column_name       NVARCHAR(100) NOT NULL,
    data_type         NVARCHAR(50),
    row_count         BIGINT,
    null_count        BIGINT,
    null_percent      DECIMAL(5,2),
    distinct_count    BIGINT,
    min_value         NVARCHAR(200),
    max_value         NVARCHAR(200),
    mean_value        DECIMAL(18,4),
    stddev_value      DECIMAL(18,4),
    median_value      DECIMAL(18,4),
    top_values        NVARCHAR(MAX)  -- JSON array of top 10 values
);

-- SLA tracking
CREATE TABLE dq_sla_tracking (
    sla_id            INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name     NVARCHAR(200) NOT NULL,
    expected_by       DATETIME2 NOT NULL,
    actual_arrival    DATETIME2,
    sla_met           BIT,
    delay_minutes     INT,
    recorded_at       DATETIME2 DEFAULT GETUTCDATE()
);

-- Indexes for common queries
CREATE INDEX IX_dq_check_results_dataset ON dq_check_results(dataset_name, run_timestamp);
CREATE INDEX IX_dq_scores_dataset ON dq_scores(dataset_name, run_timestamp);
CREATE INDEX IX_dq_profiling_dataset ON dq_profiling(dataset_name, run_timestamp);
```

## Quality Score Calculation

Quality score per dataset:

$$\text{Quality Score} = \frac{\text{Passed Checks}}{\text{Total Checks}} \times 100$$

Status thresholds:

- **Passed:** score ≥ 95%
- **Warning:** 80% ≤ score < 95%
- **Failed:** score < 80%

## Record Routing Decision

```
For each record:
    Run all quality checks

    If ALL checks pass:
        → Route to validated/ container

    If ANY critical check fails:
        → Route to quarantine/ container
        → Log failure details to dq_check_results
        → Increment quarantine counter

    If only warning checks fail:
        → Route to validated/ container (with flags)
        → Log warning to dq_check_results
```

## Interview Talking Points

1. **"What's your approach to data quality?"** → Six dimensions framework (completeness, accuracy, consistency, uniqueness, timeliness, validity) with automated checks at each pipeline stage

2. **"How do you handle bad data?"** → Quarantine pattern: isolate bad records for review, never drop data silently. Quality metadata store tracks every failure with its severity.

3. **"How do you measure quality over time?"** → Quality scores per dataset per run, tracked in SQL with historical trends. SLA tracking for timeliness.

## Next Step

→ [Guide 03: Great Expectations](03_great_expectations.md)
