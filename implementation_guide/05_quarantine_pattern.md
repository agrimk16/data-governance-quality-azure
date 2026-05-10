# Guide 05: Quarantine Pattern

## The Problem

When bad records enter a pipeline, you have three options:

1. **Drop them** — data loss, silent failures
2. **Halt the pipeline** — blocks all data for a few bad records
3. **Quarantine them** — isolate bad records for review while good data flows through

Option 3 is the production answer.

## Architecture

```
Raw Data ─── Quality Engine ──┬── ✅ Validated Records → validated/ container
                              │
                              └── ❌ Failed Records  → quarantine/ container
                                                        ├── dataset_name/
                                                        │   ├── 2024-01-15/
                                                        │   │   ├── data.parquet
                                                        │   │   └── failures.json
                                                        │   └── 2024-01-16/
                                                        └── ...
```

## Step 1: Quality Router

```python
# Notebook: notebooks/09_quarantine_router.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

class QualityRouter:
    """Routes records to validated or quarantine based on quality checks."""

    def __init__(self, validated_path: str, quarantine_path: str):
        self.validated_path = validated_path
        self.quarantine_path = quarantine_path

    def apply_checks(self, df: DataFrame, dataset_name: str, checks: list) -> dict:
        """
        Apply quality checks and split data into validated and quarantine DataFrames.

        Args:
            df: Input DataFrame
            dataset_name: Name of the dataset
            checks: List of check dicts with keys: column, check_type, condition
                    condition is a PySpark Column expression that returns True for VALID rows

        Returns:
            dict with 'validated', 'quarantined', and 'summary'
        """
        # Build a combined quality flag
        # Each check adds a boolean column
        check_cols = []
        for i, check in enumerate(checks):
            col_name = f"_qc_{i}"
            df = df.withColumn(col_name, check["condition"])
            check_cols.append(col_name)

        # Row passes if ALL quality checks pass
        df = df.withColumn(
            "_all_passed",
            F.lit(True)
        )
        for col_name in check_cols:
            df = df.withColumn("_all_passed", F.col("_all_passed") & F.col(col_name))

        # Add failure reason for quarantined records
        failure_reasons = []
        for i, check in enumerate(checks):
            failure_reasons.append(
                F.when(~F.col(f"_qc_{i}"), F.lit(check.get("description", f"check_{i}")))
            )

        df = df.withColumn(
            "_failure_reasons",
            F.array_compact(F.array(*failure_reasons))
        )

        # Split
        df_validated = df.where(F.col("_all_passed"))
        df_quarantined = df.where(~F.col("_all_passed"))

        # Clean up internal columns from validated output
        internal_cols = check_cols + ["_all_passed", "_failure_reasons"]
        df_validated_clean = df_validated.drop(*internal_cols)

        # Keep failure reasons in quarantine output
        df_quarantined_out = df_quarantined.drop(*check_cols + ["_all_passed"])

        # Summary
        total = df.count()
        valid_count = df_validated.count()
        quarantine_count = df_quarantined.count()

        summary = {
            "dataset_name": dataset_name,
            "total_records": total,
            "validated_records": valid_count,
            "quarantined_records": quarantine_count,
            "quarantine_rate": round(quarantine_count / total * 100, 2) if total > 0 else 0,
            "timestamp": datetime.utcnow().isoformat()
        }

        return {
            "validated": df_validated_clean,
            "quarantined": df_quarantined_out,
            "summary": summary
        }

    def write_results(self, result: dict, dataset_name: str, partition_date: str):
        """Write validated and quarantined DataFrames to ADLS."""

        # Write validated
        validated_path = f"{self.validated_path}/{dataset_name}"
        result["validated"].write.mode("append") \
            .partitionBy("_partition_date") \
            .parquet(validated_path)

        # Write quarantined (if any)
        if result["summary"]["quarantined_records"] > 0:
            quarantine_path = f"{self.quarantine_path}/{dataset_name}/{partition_date}"
            result["quarantined"].write.mode("overwrite") \
                .parquet(f"{quarantine_path}/data")

            # Write failure summary as JSON
            spark.createDataFrame([result["summary"]]).write.mode("overwrite") \
                .json(f"{quarantine_path}/summary")

        return result["summary"]
```

## Step 2: Define Quality Checks per Dataset

```python
# Notebook: notebooks/10_run_quarantine.py

# Read raw data
df_orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/mnt/raw/orders.csv") \
    .withColumn("_partition_date", F.current_date())

# Define checks as PySpark conditions (True = valid)
order_checks = [
    {
        "column": "order_id",
        "check_type": "completeness",
        "description": "order_id is null",
        "condition": F.col("order_id").isNotNull()
    },
    {
        "column": "customer_id",
        "check_type": "completeness",
        "description": "customer_id is null",
        "condition": F.col("customer_id").isNotNull()
    },
    {
        "column": "total_amount",
        "check_type": "accuracy",
        "description": "total_amount <= 0",
        "condition": F.col("total_amount") > 0
    },
    {
        "column": "status",
        "check_type": "validity",
        "description": "invalid status value",
        "condition": F.col("status").isin(
            "pending", "confirmed", "shipped", "delivered", "cancelled", "returned"
        )
    },
    {
        "column": "order_date",
        "check_type": "consistency",
        "description": "order_date in future",
        "condition": F.col("order_date") <= F.current_date()
    },
]

# Route
router = QualityRouter(
    validated_path="/mnt/validated",
    quarantine_path="/mnt/quarantine"
)

result = router.apply_checks(df_orders, "orders", order_checks)
summary = router.write_results(result, "orders", datetime.utcnow().strftime("%Y-%m-%d"))

print(f"📊 Orders Quality Report:")
print(f"   Total:       {summary['total_records']}")
print(f"   ✅ Validated:  {summary['validated_records']}")
print(f"   ❌ Quarantined: {summary['quarantined_records']} ({summary['quarantine_rate']}%)")
```

## Step 3: Quarantine Review Workflow

```python
# Notebook: notebooks/11_review_quarantine.py

# List quarantined datasets
quarantine_base = "/mnt/quarantine"
print("Quarantined Data:")
for item in dbutils.fs.ls(quarantine_base):
    print(f"  📁 {item.name}")
    for date_folder in dbutils.fs.ls(item.path):
        summary = spark.read.json(f"{date_folder.path}/summary").first()
        print(f"     📅 {date_folder.name} — {summary['quarantined_records']} records")

# Review specific quarantined records
df_quarantined = spark.read.parquet(f"{quarantine_base}/orders/2024-01-15/data")
display(df_quarantined.select("order_id", "total_amount", "status", "_failure_reasons"))

# Reprocess after fixing: manually correct and re-route
# df_fixed = df_quarantined.withColumn("status", F.lit("pending"))
# router.apply_checks(df_fixed, "orders", order_checks)
```

## Step 4: Quarantine Metrics

```sql
-- Track quarantine rates over time (Azure SQL view)
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
```

## Interview Talking Points

1. **"What happens to bad data in your pipeline?"** → Quarantine pattern: bad records are routed to a separate container with failure reasons attached. They're never dropped — they're available for investigation and reprocessing.

2. **"How do you decide what to quarantine vs. warn?"** → Severity levels. Critical checks (nulls in key columns, referential integrity) quarantine the record. Warning checks (minor format issues, soft business rules) flag but let the record through.

3. **"How do you reprocess quarantined data?"** → We fix the root cause, then re-ingest the quarantined partition through the same quality pipeline. If it passes, it joins the validated path.

## Next Step

→ [Guide 06: Lineage Tracking](06_lineage_tracking.md)
