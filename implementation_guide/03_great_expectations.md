# Guide 03: Great Expectations on Databricks

## What is Great Expectations?

Great Expectations (GE) is the most popular open-source data quality framework. It lets you define **expectations** (assertions about your data) and validates them automatically.

**Why interviewers ask about it:** It's the industry standard. Knowing GE signals you've worked on production quality systems.

## Step 1: Install Great Expectations

On your Databricks cluster, install via PyPI:

- Library: `great-expectations==0.18.8`

Or in a notebook:

```python
%pip install great-expectations==0.18.8
```

## Step 2: Initialize GE Context

```python
# Notebook: notebooks/01_ge_setup.py

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Create an ephemeral context (no file-based config needed)
context = gx.get_context()

# Add a Spark datasource
datasource = context.sources.add_or_update_spark(name="spark_datasource")

# Add data assets for each dataset
orders_asset = datasource.add_dataframe_asset(name="orders")
customers_asset = datasource.add_dataframe_asset(name="customers")
products_asset = datasource.add_dataframe_asset(name="products")

print("✅ GE context initialized with Spark datasource")
```

## Step 3: Build Expectation Suites

### Orders Expectations

```python
# Notebook: notebooks/02_expectations_orders.py

from pyspark.sql import SparkSession
import great_expectations as gx

spark = SparkSession.builder.getOrCreate()

# Read raw orders
df_orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/mnt/raw/orders.csv")

# Create GE context and validator
context = gx.get_context()
datasource = context.sources.add_or_update_spark(name="spark_ds")
asset = datasource.add_dataframe_asset(name="orders")
batch_request = asset.build_batch_request(dataframe=df_orders)

# Create expectation suite
context.add_or_update_expectation_suite("orders_suite")
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="orders_suite"
)

# ── Completeness ──
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_not_be_null("order_date")
validator.expect_column_values_to_not_be_null("total_amount")

# ── Uniqueness ──
validator.expect_column_values_to_be_unique("order_id")

# ── Accuracy ──
validator.expect_column_values_to_be_between(
    "total_amount", min_value=0.01, max_value=100000
)

# ── Validity ──
validator.expect_column_values_to_be_in_set(
    "status", ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
)
validator.expect_column_values_to_match_regex(
    "order_id", r"^ORD\d{6}$"
)

# ── Consistency ──
# order_date should not be in the future
from datetime import datetime
validator.expect_column_values_to_be_between(
    "order_date", max_value=datetime.now().strftime("%Y-%m-%d")
)

# ── Table-level ──
validator.expect_table_row_count_to_be_between(min_value=1, max_value=10000000)

# Save suite
validator.save_expectation_suite(discard_failed_expectations=False)
print(f"✅ Orders suite: {len(validator.get_expectation_suite().expectations)} expectations")
```

### Customers Expectations

```python
# Notebook: notebooks/03_expectations_customers.py

df_customers = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/mnt/raw/customers.csv")

context.add_or_update_expectation_suite("customers_suite")
validator = context.get_validator(
    batch_request=asset.build_batch_request(dataframe=df_customers),
    expectation_suite_name="customers_suite"
)

# Completeness
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_not_be_null("email")
validator.expect_column_values_to_not_be_null("first_name")

# Uniqueness
validator.expect_column_values_to_be_unique("customer_id")
validator.expect_column_values_to_be_unique("email")

# Validity
validator.expect_column_values_to_match_regex(
    "email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)
validator.expect_column_values_to_be_in_set(
    "state", [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
        "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
        "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
        "TX","UT","VT","VA","WA","WV","WI","WY","DC"
    ]
)
validator.expect_column_values_to_match_regex("zip_code", r"^\d{5}(-\d{4})?$")

validator.save_expectation_suite(discard_failed_expectations=False)
print(f"✅ Customers suite: {len(validator.get_expectation_suite().expectations)} expectations")
```

## Step 4: Run Validation

```python
# Notebook: notebooks/04_run_validation.py

import uuid
import json
from datetime import datetime

run_id = str(uuid.uuid4())[:8]
run_timestamp = datetime.utcnow()

def validate_dataset(context, df, suite_name, dataset_name):
    """Run validation and return structured results."""
    datasource = context.sources.add_or_update_spark(name="spark_ds")
    asset = datasource.add_dataframe_asset(name=dataset_name)
    batch_request = asset.build_batch_request(dataframe=df)

    checkpoint = context.add_or_update_checkpoint(
        name=f"checkpoint_{dataset_name}",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": suite_name,
        }]
    )

    result = checkpoint.run()

    # Parse results into structured format
    checks = []
    for validation_result in result.list_validation_results():
        for r in validation_result.results:
            check = {
                "run_id": run_id,
                "run_timestamp": run_timestamp.isoformat(),
                "dataset_name": dataset_name,
                "check_name": r.expectation_config.expectation_type,
                "check_type": classify_check_type(r.expectation_config.expectation_type),
                "column_name": r.expectation_config.kwargs.get("column", None),
                "expectation": str(r.expectation_config.kwargs),
                "passed": r.success,
                "observed_value": str(r.result.get("observed_value", "")),
                "failed_count": r.result.get("unexpected_count", 0),
                "total_count": r.result.get("element_count", 0),
                "failure_percent": r.result.get("unexpected_percent", 0.0),
                "severity": "critical" if "not_be_null" in r.expectation_config.expectation_type
                            or "be_unique" in r.expectation_config.expectation_type
                            else "warning"
            }
            checks.append(check)

    return checks

def classify_check_type(expectation_type):
    """Map GE expectation type to quality dimension."""
    mapping = {
        "not_be_null": "completeness",
        "be_unique": "uniqueness",
        "be_between": "accuracy",
        "be_in_set": "validity",
        "match_regex": "validity",
        "row_count": "completeness",
    }
    for key, dimension in mapping.items():
        if key in expectation_type:
            return dimension
    return "other"

# Run validation for all datasets
df_orders = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/raw/orders.csv")
df_customers = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/raw/customers.csv")

all_results = []
all_results.extend(validate_dataset(context, df_orders, "orders_suite", "orders"))
all_results.extend(validate_dataset(context, df_customers, "customers_suite", "customers"))

# Calculate quality scores
for dataset_name in ["orders", "customers"]:
    dataset_checks = [r for r in all_results if r["dataset_name"] == dataset_name]
    total = len(dataset_checks)
    passed = sum(1 for r in dataset_checks if r["passed"])
    score = (passed / total * 100) if total > 0 else 0
    status = "passed" if score >= 95 else ("warning" if score >= 80 else "failed")

    print(f"{'✅' if status == 'passed' else '⚠️' if status == 'warning' else '❌'} "
          f"{dataset_name}: {score:.1f}% ({passed}/{total} checks passed) — {status}")
```

## Step 5: Store Results in Azure SQL

```python
# Notebook: notebooks/05_store_results.py

import pyodbc

# Connection to Azure SQL
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={dbutils.secrets.get('keyvault-scope', 'sql-server-name')}.database.windows.net;"
    f"DATABASE={dbutils.secrets.get('keyvault-scope', 'sql-database-name')};"
    f"UID={dbutils.secrets.get('keyvault-scope', 'sql-admin-user')};"
    f"PWD={dbutils.secrets.get('keyvault-scope', 'sql-admin-password')};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Insert check results
for r in all_results:
    cursor.execute("""
        INSERT INTO dq_check_results
        (run_id, dataset_name, check_name, check_type, column_name,
         expectation, passed, observed_value, failed_count, total_count,
         failure_percent, severity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        r["run_id"], r["dataset_name"], r["check_name"], r["check_type"],
        r["column_name"], r["expectation"], r["passed"], r["observed_value"],
        r["failed_count"], r["total_count"], r["failure_percent"], r["severity"]
    ))

# Insert quality scores
for dataset_name in ["orders", "customers"]:
    dataset_checks = [r for r in all_results if r["dataset_name"] == dataset_name]
    total = len(dataset_checks)
    passed = sum(1 for r in dataset_checks if r["passed"])
    failed = total - passed
    score = (passed / total * 100) if total > 0 else 0
    status = "passed" if score >= 95 else ("warning" if score >= 80 else "failed")

    cursor.execute("""
        INSERT INTO dq_scores
        (run_id, dataset_name, total_checks, passed_checks, failed_checks,
         quality_score, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (run_id, dataset_name, total, passed, failed, score, status))

conn.commit()
cursor.close()
conn.close()
print(f"✅ Stored {len(all_results)} check results and quality scores in Azure SQL")
```

## Step 6: Quality Trend Query

After running validations over multiple days, query trends:

```sql
-- Quality score trend by dataset
SELECT
    dataset_name,
    CAST(run_timestamp AS DATE) AS run_date,
    quality_score,
    status,
    total_checks,
    failed_checks
FROM dq_scores
ORDER BY dataset_name, run_timestamp DESC;

-- Most commonly failing checks
SELECT
    dataset_name,
    check_name,
    column_name,
    COUNT(*) AS failure_count,
    AVG(failure_percent) AS avg_failure_rate
FROM dq_check_results
WHERE passed = 0
GROUP BY dataset_name, check_name, column_name
ORDER BY failure_count DESC;
```

## Interview Talking Points

1. **"Why Great Expectations over custom checks?"** → Declarative expectations, built-in documentation (Data Docs), checkpoint pattern for orchestration, large community
2. **"How do you handle schema evolution?"** → GE validations catch unexpected schema changes; column-level expectations fail explicitly, alerting the team
3. **"What's a checkpoint?"** → A reusable validation configuration that pairs a batch of data with an expectation suite — the unit of execution in GE

## Next Step

→ [Guide 04: Data Profiling](04_data_profiling.md)
