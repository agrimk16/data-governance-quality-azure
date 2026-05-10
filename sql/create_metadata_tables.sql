-- ============================================================
-- Metadata Tables — Data Governance & Quality Framework
-- ============================================================
-- Run this against your Azure SQL Database after provisioning

-- Quality check results
CREATE TABLE dq_check_results (
    result_id         INT IDENTITY(1,1) PRIMARY KEY,
    run_id            NVARCHAR(50) NOT NULL,
    run_timestamp     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    dataset_name      NVARCHAR(200) NOT NULL,
    check_name        NVARCHAR(200) NOT NULL,
    check_type        NVARCHAR(50) NOT NULL,
    column_name       NVARCHAR(100),
    expectation       NVARCHAR(500),
    passed            BIT NOT NULL,
    observed_value    NVARCHAR(200),
    expected_value    NVARCHAR(200),
    failed_count      INT DEFAULT 0,
    total_count       INT DEFAULT 0,
    failure_percent   DECIMAL(5,2),
    severity          NVARCHAR(20) DEFAULT 'warning'
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
    quality_score     DECIMAL(5,2) NOT NULL,
    status            NVARCHAR(20) NOT NULL
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
    top_values        NVARCHAR(MAX)
);

-- Data lineage records
CREATE TABLE dq_lineage (
    lineage_id        INT IDENTITY(1,1) PRIMARY KEY,
    run_id            NVARCHAR(50) NOT NULL,
    run_timestamp     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    source_system     NVARCHAR(200) NOT NULL,
    source_layer      NVARCHAR(50) NOT NULL,
    target_system     NVARCHAR(200) NOT NULL,
    target_layer      NVARCHAR(50) NOT NULL,
    transformation    NVARCHAR(500),
    record_count_in   BIGINT,
    record_count_out  BIGINT,
    records_filtered  BIGINT DEFAULT 0,
    pipeline_name     NVARCHAR(200),
    notebook_name     NVARCHAR(200),
    status            NVARCHAR(20) DEFAULT 'success',
    duration_seconds  INT,
    error_message     NVARCHAR(MAX)
);

-- Column-level lineage
CREATE TABLE dq_column_lineage (
    col_lineage_id    INT IDENTITY(1,1) PRIMARY KEY,
    lineage_id        INT FOREIGN KEY REFERENCES dq_lineage(lineage_id),
    source_column     NVARCHAR(100) NOT NULL,
    target_column     NVARCHAR(100) NOT NULL,
    transformation    NVARCHAR(500)
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

-- Indexes
CREATE INDEX IX_dq_check_results_dataset ON dq_check_results(dataset_name, run_timestamp);
CREATE INDEX IX_dq_scores_dataset ON dq_scores(dataset_name, run_timestamp);
CREATE INDEX IX_dq_profiling_dataset ON dq_profiling(dataset_name, run_timestamp);
CREATE INDEX IX_dq_lineage_source ON dq_lineage(source_system, run_timestamp);
CREATE INDEX IX_dq_lineage_target ON dq_lineage(target_system, run_timestamp);

-- Audit trail view
CREATE VIEW v_audit_trail AS
SELECT 
    'quality_check' AS event_type,
    run_id,
    run_timestamp AS event_timestamp,
    dataset_name AS target_asset,
    CONCAT(check_name, ' on ', COALESCE(column_name, '*')) AS event_detail,
    CASE WHEN passed = 1 THEN 'PASSED' ELSE 'FAILED' END AS result
FROM dq_check_results
UNION ALL
SELECT 
    'lineage' AS event_type,
    run_id,
    run_timestamp,
    CONCAT(source_system, N' → ', target_system),
    transformation,
    status
FROM dq_lineage
UNION ALL
SELECT 
    'sla_check' AS event_type,
    CAST(sla_id AS NVARCHAR),
    recorded_at,
    pipeline_name,
    CONCAT('Expected by ', FORMAT(expected_by, 'HH:mm'), ', delay: ', CAST(delay_minutes AS NVARCHAR), ' min'),
    CASE WHEN sla_met = 1 THEN 'MET' ELSE 'MISSED' END
FROM dq_sla_tracking;

-- Quarantine metrics view
CREATE VIEW v_quarantine_metrics AS
SELECT 
    dataset_name,
    CAST(run_timestamp AS DATE) AS run_date,
    SUM(CASE WHEN passed = 0 THEN failed_count ELSE 0 END) AS quarantined_records,
    SUM(total_count) AS total_records,
    CAST(SUM(CASE WHEN passed = 0 THEN failed_count ELSE 0 END) AS FLOAT) 
        / NULLIF(SUM(total_count), 0) * 100 AS quarantine_rate
FROM dq_check_results
GROUP BY dataset_name, CAST(run_timestamp AS DATE);

PRINT 'All metadata tables created successfully.';
