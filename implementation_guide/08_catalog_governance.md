# Guide 08: Data Catalog & Governance

## Microsoft Purview (Optional)

**Note:** Purview costs ~$0.25/hr. It's powerful for enterprise governance but optional for this project on a learning budget. This guide covers how to set it up and when to use it, but all core governance features work without it using the metadata store you've built.

## When to Use What

| Feature                 | Custom Metadata Store  | Microsoft Purview                           |
| ----------------------- | ---------------------- | ------------------------------------------- |
| Quality scores & trends | ✅ Built in project    | ❌ Not core feature                         |
| Data profiling          | ✅ PySpark profiler    | ✅ Auto-scan profiling                      |
| Data lineage            | ✅ Custom tracker      | ✅ Auto-captured from ADF/Synapse           |
| Data catalog & search   | ❌                     | ✅ Core feature                             |
| Data classification     | ❌                     | ✅ Auto PII/sensitive data detection        |
| Business glossary       | ❌                     | ✅ Define business terms and link to assets |
| Access governance       | Partial (RBAC scripts) | ✅ Policy-based access control              |

**Recommendation:** Build the custom framework first (already done). Add Purview if preparing for a specific interview where it's listed in the JD.

## Step 1: Provision Purview (Optional)

```bash
source setup/variables.sh

# Create Purview account
az purview account create \
    --name $PURVIEW_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --managed-group-name "${RESOURCE_GROUP}-purview-managed"

# Grant Purview MI access to scan ADLS
PURVIEW_MI=$(az purview account show \
    --name $PURVIEW_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query identity.principalId -o tsv)

STORAGE_ID=$(az storage account show \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

az role assignment create \
    --role "Storage Blob Data Reader" \
    --assignee-object-id $PURVIEW_MI \
    --assignee-principal-type ServicePrincipal \
    --scope $STORAGE_ID
```

## Step 2: Register Data Sources

In the Purview Governance Portal:

1. **Data Map → Sources → Register**
2. Register your ADLS Gen2 account
3. Register your Azure SQL Database

## Step 3: Scan Data

1. **Data Map → Sources → Your ADLS → New Scan**
2. Select containers to scan: `raw`, `validated`, `gold`
3. Choose scan rule set: `AdlsGen2` (auto-classifies PII)
4. Set schedule: Weekly (for learning) or trigger manually
5. Run scan

The scan will:

- Discover all files and folders
- Infer schemas from CSV/Parquet/Delta
- Auto-classify sensitive data (SSN, email, phone, credit card)
- Build a searchable catalog

## Step 4: Business Glossary

Create a business glossary to standardize terminology:

In Purview Governance Portal → Manage glossary:

| Term          | Definition                                     | Approved By      | Related Assets        |
| ------------- | ---------------------------------------------- | ---------------- | --------------------- |
| Customer LTV  | Lifetime total order value per customer        | Data Team Lead   | gold/dim_customers    |
| Order         | A single purchase transaction                  | Business Analyst | silver/orders_cleaned |
| Quarantined   | Records that failed critical quality checks    | Data Engineer    | quarantine/\*         |
| Quality Score | % of quality checks passed per dataset per run | Data Engineer    | dq_scores table       |
| SLA           | Maximum allowed delay for pipeline completion  | Platform Team    | dq_sla_tracking table |

## Step 5: Access Governance Framework

Even without Purview, implement governance through RBAC policies:

```python
# Notebook: notebooks/16_access_governance.py

# Define role-based access policies
access_policies = {
    "data_engineers": {
        "description": "Full access to all layers for ETL development",
        "containers": ["raw", "bronze", "silver", "gold", "quarantine", "profiling"],
        "permission": "Storage Blob Data Contributor",
        "sql_role": "db_datareader, db_datawriter"
    },
    "data_analysts": {
        "description": "Read access to silver and gold layers only",
        "containers": ["silver", "gold"],
        "permission": "Storage Blob Data Reader",
        "sql_role": "db_datareader"
    },
    "data_scientists": {
        "description": "Read access to all layers, write to profiling",
        "containers": ["raw", "bronze", "silver", "gold", "profiling"],
        "permission": "Storage Blob Data Reader",  # + Contributor on profiling
        "sql_role": "db_datareader"
    },
    "business_users": {
        "description": "Read access to gold layer only",
        "containers": ["gold"],
        "permission": "Storage Blob Data Reader",
        "sql_role": "db_datareader"  # only on reporting views
    },
}

# Generate RBAC assignment commands
def generate_rbac_commands(policies: dict, storage_account: str, resource_group: str):
    """Generate Azure CLI commands for RBAC assignments."""
    commands = []
    for role_name, policy in policies.items():
        for container in policy["containers"]:
            cmd = (
                f"# {role_name} → {container} ({policy['permission']})\n"
                f"az role assignment create \\\n"
                f"    --role \"{policy['permission']}\" \\\n"
                f"    --assignee-object-id <{role_name}_GROUP_OBJECT_ID> \\\n"
                f"    --assignee-principal-type Group \\\n"
                f"    --scope /subscriptions/<SUB_ID>/resourceGroups/{resource_group}"
                f"/providers/Microsoft.Storage/storageAccounts/{storage_account}"
                f"/blobServices/default/containers/{container}"
            )
            commands.append(cmd)
    return commands

commands = generate_rbac_commands(access_policies, "YOUR_STORAGE_ACCOUNT", "YOUR_RG")
for cmd in commands:
    print(cmd)
    print()
```

## Step 6: Audit Trail

```sql
-- Audit trail view: who accessed what quality data
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
    CONCAT(source_system, ' → ', target_system),
    transformation,
    status
FROM dq_lineage

UNION ALL

SELECT
    'sla_check' AS event_type,
    CAST(sla_id AS NVARCHAR),
    recorded_at,
    pipeline_name,
    CONCAT('Expected by ', FORMAT(expected_by, 'HH:mm'), ', delay: ', delay_minutes, ' min'),
    CASE WHEN sla_met = 1 THEN 'MET' ELSE 'MISSED' END
FROM dq_sla_tracking;

-- Recent audit events
SELECT TOP 100 * FROM v_audit_trail ORDER BY event_timestamp DESC;
```

## Step 7: Data Classification (Without Purview)

```python
# Notebook: notebooks/17_data_classification.py

import re

# Pattern-based PII detection
pii_patterns = {
    "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
    "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
    "phone": r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
    "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
    "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
}

# Column name patterns that suggest PII
pii_column_names = {
    "ssn", "social_security", "tax_id", "ein",
    "email", "email_address",
    "phone", "phone_number", "mobile", "cell",
    "first_name", "last_name", "full_name",
    "address", "street", "zip", "zip_code",
    "date_of_birth", "dob", "birth_date",
    "credit_card", "card_number", "cvv",
    "password", "secret", "token",
}

def classify_columns(df, dataset_name: str) -> list:
    """Classify columns by PII sensitivity."""
    results = []

    for field in df.schema.fields:
        col_name = field.name.lower()
        classification = "non-sensitive"
        detected_type = None

        # Check column name
        if col_name in pii_column_names:
            classification = "pii"
            detected_type = f"column_name_match: {col_name}"

        # Check sample values for patterns (sample only 100 rows for efficiency)
        if classification == "non-sensitive":
            sample = [str(row[field.name]) for row in df.select(field.name).limit(100).collect()
                      if row[field.name] is not None]

            for pii_type, pattern in pii_patterns.items():
                matches = sum(1 for val in sample if re.search(pattern, val))
                if matches > len(sample) * 0.5:  # >50% of samples match
                    classification = "pii"
                    detected_type = f"pattern_match: {pii_type}"
                    break

        results.append({
            "dataset": dataset_name,
            "column": field.name,
            "data_type": str(field.dataType),
            "classification": classification,
            "detected_type": detected_type or "none",
        })

    return results

# Classify all datasets
for dataset_name, path in [("orders", "/mnt/raw/orders.csv"),
                           ("customers", "/mnt/raw/customers.csv")]:
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    classifications = classify_columns(df, dataset_name)

    pii_cols = [c for c in classifications if c["classification"] == "pii"]
    print(f"\n📋 {dataset_name}: {len(pii_cols)} PII columns detected")
    for c in pii_cols:
        print(f"   🔒 {c['column']} — {c['detected_type']}")
```

## Completion Checklist

- [ ] Azure SQL metadata tables created and populated
- [ ] Great Expectations suites running for all datasets
- [ ] Data profiler producing column-level statistics
- [ ] Quarantine pattern routing bad records
- [ ] Lineage tracker instrumenting all transformation steps
- [ ] Azure Monitor alerts configured for critical failures
- [ ] SLA monitoring running on schedule
- [ ] Data classification identifying PII columns
- [ ] (Optional) Purview scanning and cataloging assets
- [ ] Quality score trends visible over multiple runs

## Interview Talking Points

1. **"What's your data governance strategy?"** → Multi-layered: (1) Automated quality checks catch issues early, (2) Quarantine pattern isolates bad data, (3) Lineage tracking enables root cause and impact analysis, (4) RBAC and classification protect sensitive data, (5) Monitoring and SLAs ensure operational reliability.

2. **"How do you handle PII?"** → Classification first — scan columns and values for PII patterns. Apply column-level security, dynamic data masking, and container-level RBAC to restrict access. Audit trail tracks all data access.

3. **"Purview vs. custom governance?"** → Purview excels at auto-discovery, classification, and business glossary at scale. Custom framework gives deeper quality and lineage control. In practice, use both: Purview for catalog and classification, custom framework for quality scoring and quarantine.

## Project Complete! 🎉

You've built a comprehensive data governance and quality framework covering:

- Automated quality validation (Great Expectations)
- Statistical data profiling with anomaly detection
- Quarantine pattern for bad records
- Column-level and pipeline-level lineage tracking
- SLA monitoring and alerting
- PII classification and access governance
