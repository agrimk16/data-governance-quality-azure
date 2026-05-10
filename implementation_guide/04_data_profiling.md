# Guide 04: Data Profiling

## What is Data Profiling?

Data profiling computes column-level statistics to understand data shape, distribution, and anomalies. It answers: _"What does my data actually look like?"_

**Interview relevance:** Profiling catches issues that rule-based checks miss — distribution shifts, cardinality changes, subtle data drift.

## Step 1: Build the Profiler

```python
# Notebook: notebooks/06_data_profiler.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
import json
import uuid
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def profile_dataset(df: DataFrame, dataset_name: str, run_id: str) -> list:
    """Profile all columns in a DataFrame and return structured results."""

    row_count = df.count()
    profiles = []

    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)

        # Base stats for all columns
        null_count = df.where(F.col(col_name).isNull()).count()
        null_percent = round((null_count / row_count * 100), 2) if row_count > 0 else 0
        distinct_count = df.select(col_name).distinct().count()

        profile = {
            "run_id": run_id,
            "run_timestamp": datetime.utcnow().isoformat(),
            "dataset_name": dataset_name,
            "column_name": col_name,
            "data_type": col_type,
            "row_count": row_count,
            "null_count": null_count,
            "null_percent": null_percent,
            "distinct_count": distinct_count,
            "min_value": None,
            "max_value": None,
            "mean_value": None,
            "stddev_value": None,
            "median_value": None,
            "top_values": None,
        }

        # Numeric-specific stats
        if isinstance(field.dataType, NumericType):
            stats = df.select(
                F.min(col_name).alias("min_val"),
                F.max(col_name).alias("max_val"),
                F.mean(col_name).alias("mean_val"),
                F.stddev(col_name).alias("stddev_val"),
                F.percentile_approx(col_name, 0.5).alias("median_val"),
            ).first()

            profile["min_value"] = str(stats["min_val"])
            profile["max_value"] = str(stats["max_val"])
            profile["mean_value"] = float(stats["mean_val"]) if stats["mean_val"] else None
            profile["stddev_value"] = float(stats["stddev_val"]) if stats["stddev_val"] else None
            profile["median_value"] = float(stats["median_val"]) if stats["median_val"] else None

        # String-specific stats
        elif isinstance(field.dataType, StringType):
            str_stats = df.select(
                F.min(F.length(col_name)).alias("min_len"),
                F.max(F.length(col_name)).alias("max_len"),
                F.mean(F.length(col_name)).alias("avg_len"),
            ).first()

            profile["min_value"] = str(str_stats["min_len"])
            profile["max_value"] = str(str_stats["max_len"])

        # Date-specific stats
        elif isinstance(field.dataType, (DateType, TimestampType)):
            date_stats = df.select(
                F.min(col_name).alias("min_date"),
                F.max(col_name).alias("max_date"),
            ).first()

            profile["min_value"] = str(date_stats["min_date"])
            profile["max_value"] = str(date_stats["max_date"])

        # Top values (all types)
        top_vals = (df.groupBy(col_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(10)
                    .collect())
        profile["top_values"] = json.dumps([
            {"value": str(row[col_name]), "count": row["count"]}
            for row in top_vals
        ])

        profiles.append(profile)

    return profiles
```

## Step 2: Run Profiling

```python
run_id = str(uuid.uuid4())[:8]

# Profile each dataset
df_orders = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/raw/orders.csv")
df_customers = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/raw/customers.csv")
df_products = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/raw/products.csv")

all_profiles = []
all_profiles.extend(profile_dataset(df_orders, "orders", run_id))
all_profiles.extend(profile_dataset(df_customers, "customers", run_id))
all_profiles.extend(profile_dataset(df_products, "products", run_id))

print(f"✅ Profiled {len(all_profiles)} columns across 3 datasets")

# Display summary
for p in all_profiles:
    flag = "⚠️" if p["null_percent"] > 5 else "✅"
    print(f"  {flag} {p['dataset_name']}.{p['column_name']}: "
          f"nulls={p['null_percent']}%, distinct={p['distinct_count']}, "
          f"type={p['data_type']}")
```

## Step 3: Anomaly Detection

Detect statistical anomalies by comparing current profile against historical baselines:

```python
# Notebook: notebooks/07_anomaly_detection.py

def detect_anomalies(current_profiles: list, historical_profiles: list, z_threshold: float = 3.0) -> list:
    """
    Compare current profiling results against historical baselines.
    Flag columns where current stats deviate beyond z_threshold standard deviations.
    """
    anomalies = []

    # Build historical baselines (mean + stddev of each metric per column)
    from collections import defaultdict
    baselines = defaultdict(lambda: {"null_pcts": [], "distinct_counts": [], "means": []})

    for hp in historical_profiles:
        key = f"{hp['dataset_name']}.{hp['column_name']}"
        baselines[key]["null_pcts"].append(hp["null_percent"])
        baselines[key]["distinct_counts"].append(hp["distinct_count"])
        if hp["mean_value"] is not None:
            baselines[key]["means"].append(hp["mean_value"])

    for cp in current_profiles:
        key = f"{cp['dataset_name']}.{cp['column_name']}"
        baseline = baselines.get(key)

        if not baseline or len(baseline["null_pcts"]) < 3:
            continue  # Need at least 3 historical runs for meaningful comparison

        # Check null rate anomaly
        import statistics
        hist_null_mean = statistics.mean(baseline["null_pcts"])
        hist_null_std = statistics.stdev(baseline["null_pcts"]) if len(baseline["null_pcts"]) > 1 else 1

        if hist_null_std > 0:
            z_score = abs(cp["null_percent"] - hist_null_mean) / hist_null_std
            if z_score > z_threshold:
                anomalies.append({
                    "dataset": cp["dataset_name"],
                    "column": cp["column_name"],
                    "metric": "null_percent",
                    "current": cp["null_percent"],
                    "baseline_mean": round(hist_null_mean, 2),
                    "z_score": round(z_score, 2),
                    "severity": "critical" if z_score > 5 else "warning"
                })

        # Check distinct count anomaly (cardinality drift)
        hist_dist_mean = statistics.mean(baseline["distinct_counts"])
        hist_dist_std = statistics.stdev(baseline["distinct_counts"]) if len(baseline["distinct_counts"]) > 1 else 1

        if hist_dist_std > 0:
            z_score = abs(cp["distinct_count"] - hist_dist_mean) / hist_dist_std
            if z_score > z_threshold:
                anomalies.append({
                    "dataset": cp["dataset_name"],
                    "column": cp["column_name"],
                    "metric": "distinct_count",
                    "current": cp["distinct_count"],
                    "baseline_mean": round(hist_dist_mean, 2),
                    "z_score": round(z_score, 2),
                    "severity": "warning"
                })

    return anomalies

# Example usage (after multiple profiling runs exist)
# anomalies = detect_anomalies(current_profiles=all_profiles, historical_profiles=load_historical())
# for a in anomalies:
#     print(f"🚨 {a['severity'].upper()}: {a['dataset']}.{a['column']} — "
#           f"{a['metric']} is {a['current']} (baseline: {a['baseline_mean']}, z={a['z_score']})")
```

## Step 4: Store Profiling Results

```python
# Notebook: notebooks/08_store_profiles.py

import pyodbc

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

for p in all_profiles:
    cursor.execute("""
        INSERT INTO dq_profiling
        (run_id, dataset_name, column_name, data_type, row_count,
         null_count, null_percent, distinct_count, min_value, max_value,
         mean_value, stddev_value, median_value, top_values)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        p["run_id"], p["dataset_name"], p["column_name"], p["data_type"],
        p["row_count"], p["null_count"], p["null_percent"], p["distinct_count"],
        p["min_value"], p["max_value"], p["mean_value"], p["stddev_value"],
        p["median_value"], p["top_values"]
    ))

conn.commit()
cursor.close()
conn.close()
print(f"✅ Stored {len(all_profiles)} profiling results")
```

## Interview Talking Points

1. **"How do you detect data drift?"** → Compare current profiling stats against historical baselines using Z-score. Flag columns where null rates, cardinality, or distributions deviate beyond 3 standard deviations.

2. **"What's the difference between profiling and validation?"** → Validation checks rules you define (expectations). Profiling discovers what the data looks like — it surfaces unknowns. Use profiling to build better validation rules.

3. **"How do you handle slowly changing data distributions?"** → Use rolling windows for baselines (e.g., last 30 runs). This adapts to legitimate drift while catching sudden anomalies.

## Next Step

→ [Guide 05: Quarantine Pattern](05_quarantine_pattern.md)
