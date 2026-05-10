# Guide 06: Lineage Tracking

## Why Data Lineage?

Data lineage tracks where data came from, how it was transformed, and where it went. It answers:

- **Impact analysis:** "If this source changes, what downstream reports break?"
- **Root cause:** "This dashboard number is wrong — where did the bad data enter?"
- **Compliance:** "Prove that PII data didn't leak to unauthorized systems"

## Lineage Metadata Schema

```sql
-- Add to sql/create_metadata_tables.sql

-- Data lineage records
CREATE TABLE dq_lineage (
    lineage_id        INT IDENTITY(1,1) PRIMARY KEY,
    run_id            NVARCHAR(50) NOT NULL,
    run_timestamp     DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    source_system     NVARCHAR(200) NOT NULL,     -- e.g., "raw/orders.csv"
    source_layer      NVARCHAR(50) NOT NULL,       -- raw, bronze, silver, gold
    target_system     NVARCHAR(200) NOT NULL,      -- e.g., "silver/orders_cleaned"
    target_layer      NVARCHAR(50) NOT NULL,
    transformation    NVARCHAR(500),               -- description of transform
    record_count_in   BIGINT,
    record_count_out  BIGINT,
    records_filtered  BIGINT DEFAULT 0,
    pipeline_name     NVARCHAR(200),
    notebook_name     NVARCHAR(200),
    status            NVARCHAR(20) DEFAULT 'success',  -- success, failed, partial
    duration_seconds  INT,
    error_message     NVARCHAR(MAX)
);

-- Column-level lineage
CREATE TABLE dq_column_lineage (
    col_lineage_id    INT IDENTITY(1,1) PRIMARY KEY,
    lineage_id        INT FOREIGN KEY REFERENCES dq_lineage(lineage_id),
    source_column     NVARCHAR(100) NOT NULL,
    target_column     NVARCHAR(100) NOT NULL,
    transformation    NVARCHAR(500)    -- e.g., "UPPER(city)", "COALESCE(a, b)", "SUM(amount)"
);

CREATE INDEX IX_dq_lineage_source ON dq_lineage(source_system, run_timestamp);
CREATE INDEX IX_dq_lineage_target ON dq_lineage(target_system, run_timestamp);
```

## Step 1: Lineage Tracker Class

```python
# Notebook: notebooks/12_lineage_tracker.py

import uuid
import time
import pyodbc
from datetime import datetime
from contextlib import contextmanager

class LineageTracker:
    """Track data lineage from source to target with transformation metadata."""

    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self.run_id = str(uuid.uuid4())[:8]

    @contextmanager
    def track(self, source: str, source_layer: str, target: str, target_layer: str,
              transformation: str, pipeline_name: str = None, notebook_name: str = None):
        """
        Context manager that tracks a transformation step.

        Usage:
            with tracker.track("raw/orders.csv", "raw", "silver/orders", "silver",
                             "Clean and standardize") as t:
                df_out = transform(df_in)
                t.set_counts(df_in.count(), df_out.count())
        """
        entry = LineageEntry(
            run_id=self.run_id,
            source=source, source_layer=source_layer,
            target=target, target_layer=target_layer,
            transformation=transformation,
            pipeline_name=pipeline_name,
            notebook_name=notebook_name
        )

        start_time = time.time()
        try:
            yield entry
            entry.status = "success"
        except Exception as e:
            entry.status = "failed"
            entry.error_message = str(e)[:500]
            raise
        finally:
            entry.duration_seconds = int(time.time() - start_time)
            self._persist(entry)

    def _persist(self, entry):
        """Write lineage record to Azure SQL."""
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO dq_lineage
            (run_id, source_system, source_layer, target_system, target_layer,
             transformation, record_count_in, record_count_out, records_filtered,
             pipeline_name, notebook_name, status, duration_seconds, error_message)
            OUTPUT INSERTED.lineage_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.run_id, entry.source, entry.source_layer,
            entry.target, entry.target_layer, entry.transformation,
            entry.record_count_in, entry.record_count_out, entry.records_filtered,
            entry.pipeline_name, entry.notebook_name,
            entry.status, entry.duration_seconds, entry.error_message
        ))

        lineage_id = cursor.fetchone()[0]

        # Persist column-level lineage
        for col_map in entry.column_mappings:
            cursor.execute("""
                INSERT INTO dq_column_lineage
                (lineage_id, source_column, target_column, transformation)
                VALUES (?, ?, ?, ?)
            """, (lineage_id, col_map["source"], col_map["target"], col_map.get("transform", "")))

        conn.commit()
        cursor.close()
        conn.close()


class LineageEntry:
    """Single lineage record being tracked."""

    def __init__(self, run_id, source, source_layer, target, target_layer,
                 transformation, pipeline_name=None, notebook_name=None):
        self.run_id = run_id
        self.source = source
        self.source_layer = source_layer
        self.target = target
        self.target_layer = target_layer
        self.transformation = transformation
        self.pipeline_name = pipeline_name
        self.notebook_name = notebook_name
        self.record_count_in = 0
        self.record_count_out = 0
        self.records_filtered = 0
        self.column_mappings = []
        self.status = "running"
        self.duration_seconds = 0
        self.error_message = None

    def set_counts(self, count_in: int, count_out: int):
        self.record_count_in = count_in
        self.record_count_out = count_out
        self.records_filtered = count_in - count_out

    def add_column_mapping(self, source_col: str, target_col: str, transform: str = ""):
        self.column_mappings.append({
            "source": source_col,
            "target": target_col,
            "transform": transform
        })
```

## Step 2: Instrument Transformations

```python
# Notebook: notebooks/13_instrumented_pipeline.py

tracker = LineageTracker(conn_str)

# ── Step 1: Raw → Bronze ──
with tracker.track(
    source="raw/orders.csv", source_layer="raw",
    target="bronze/orders", target_layer="bronze",
    transformation="Schema enforcement, add ingestion timestamp",
    notebook_name="instrumented_pipeline"
) as t:
    df_raw = spark.read.option("header", True).option("inferSchema", True) \
        .csv("/mnt/raw/orders.csv")

    df_bronze = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
                      .withColumn("_source_file", F.input_file_name())

    df_bronze.write.mode("overwrite").format("delta").save("/mnt/bronze/orders")

    t.set_counts(df_raw.count(), df_bronze.count())
    t.add_column_mapping("*", "*", "passthrough")
    t.add_column_mapping("N/A", "_ingested_at", "current_timestamp()")
    t.add_column_mapping("N/A", "_source_file", "input_file_name()")

# ── Step 2: Bronze → Silver ──
with tracker.track(
    source="bronze/orders", source_layer="bronze",
    target="silver/orders_cleaned", target_layer="silver",
    transformation="Clean nulls, standardize status, deduplicate",
    notebook_name="instrumented_pipeline"
) as t:
    df_bronze = spark.read.format("delta").load("/mnt/bronze/orders")
    count_in = df_bronze.count()

    df_silver = (df_bronze
        .dropDuplicates(["order_id"])
        .where(F.col("order_id").isNotNull())
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))
    )

    df_silver.write.mode("overwrite").format("delta").save("/mnt/silver/orders_cleaned")

    t.set_counts(count_in, df_silver.count())
    t.add_column_mapping("status", "status", "LOWER(TRIM(status))")
    t.add_column_mapping("total_amount", "total_amount", "CAST(total_amount AS DECIMAL(10,2))")

print(f"✅ Pipeline run {tracker.run_id} — lineage tracked for 2 steps")
```

## Step 3: Lineage Queries

```sql
-- Full lineage for a dataset (forward)
SELECT
    source_system, source_layer,
    '→' AS direction,
    target_system, target_layer,
    transformation,
    record_count_in, record_count_out, records_filtered,
    status, duration_seconds
FROM dq_lineage
WHERE source_system LIKE '%orders%'
ORDER BY run_timestamp;

-- Impact analysis: what depends on raw/orders.csv?
WITH lineage_chain AS (
    SELECT target_system, target_layer, 1 AS depth
    FROM dq_lineage
    WHERE source_system = 'raw/orders.csv'

    UNION ALL

    SELECT l.target_system, l.target_layer, lc.depth + 1
    FROM dq_lineage l
    INNER JOIN lineage_chain lc ON l.source_system = lc.target_system
    WHERE lc.depth < 5
)
SELECT DISTINCT target_system, target_layer, depth
FROM lineage_chain
ORDER BY depth;

-- Column-level lineage
SELECT
    l.source_system, cl.source_column,
    '→' AS direction,
    l.target_system, cl.target_column,
    cl.transformation
FROM dq_column_lineage cl
JOIN dq_lineage l ON cl.lineage_id = l.lineage_id
WHERE l.target_system LIKE '%orders%'
ORDER BY l.run_timestamp;
```

## Interview Talking Points

1. **"How do you track data lineage?"** → Custom metadata store in Azure SQL. Every transformation step is instrumented with a context manager that captures source, target, row counts, column-level mappings, and duration. Business users can trace any data element back to its source.

2. **"How do you do impact analysis?"** → Recursive CTE across lineage records. Starting from any source, walk forward to find all downstream consumers. Starting from any target, walk backward to trace root cause.

3. **"Column-level lineage?"** → We explicitly map source→target columns with transformation descriptions. This lets us answer "where did this aggregated metric come from?" at the field level.

4. **"Why not use Apache Atlas or Purview for lineage?"** → In production, I'd use Purview (covered in Guide 08). But building a custom tracker teaches the fundamentals and gives full control. The concepts map directly to how enterprise tools work internally.

## Next Step

→ [Guide 07: Monitoring & Alerts](07_monitoring_alerts.md)
