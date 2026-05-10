# Guide 07: Monitoring & Alerts

## Why Monitoring?

Quality checks are useless if nobody sees the failures. This guide builds an alerting system that escalates quality issues to the right people.

## Architecture

```
Quality Results (Azure SQL)
        │
        ▼
Azure Monitor ──── Action Group ──── Email / Teams / PagerDuty
        │
Log Analytics ──── KQL Queries ──── Dashboards / Alerts
        │
Databricks Logs ─── Diagnostic Settings ─── Log Analytics
```

## Step 1: Configure Diagnostic Settings

Send Databricks workspace logs to Log Analytics:

```bash
source setup/variables.sh

# Get resource IDs
DATABRICKS_ID=$(az databricks workspace show \
    --name $DATABRICKS_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

LOG_ANALYTICS_ID=$(az monitor log-analytics workspace show \
    --workspace-name $LOG_ANALYTICS_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

# Enable diagnostic settings
az monitor diagnostic-settings create \
    --name "databricks-diagnostics" \
    --resource $DATABRICKS_ID \
    --workspace $LOG_ANALYTICS_ID \
    --logs '[
        {"category": "clusters", "enabled": true},
        {"category": "jobs", "enabled": true},
        {"category": "notebook", "enabled": true}
    ]'
```

## Step 2: Push Quality Metrics to Log Analytics

```python
# Notebook: notebooks/14_push_metrics.py

import requests
import json
import hashlib
import hmac
import base64
import datetime

class LogAnalyticsClient:
    """Send custom logs to Azure Log Analytics."""

    def __init__(self, workspace_id: str, shared_key: str):
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = "DataQuality"

    def send(self, records: list):
        """Send quality records to Log Analytics custom table."""
        body = json.dumps(records)
        content_length = len(body)

        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

        string_to_sign = f"POST\n{content_length}\napplication/json\nx-ms-date:{rfc1123date}\n/api/logs"
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        ).decode('utf-8')

        authorization = f"SharedKey {self.workspace_id}:{encoded_hash}"

        uri = f"https://{self.workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"

        headers = {
            'content-type': 'application/json',
            'Authorization': authorization,
            'Log-Type': self.log_type,
            'x-ms-date': rfc1123date,
        }

        response = requests.post(uri, data=body, headers=headers)
        if response.status_code in (200, 202):
            print(f"✅ Sent {len(records)} records to Log Analytics")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")

# Usage
workspace_id = dbutils.secrets.get("keyvault-scope", "log-analytics-workspace-id")
shared_key = dbutils.secrets.get("keyvault-scope", "log-analytics-shared-key")

client = LogAnalyticsClient(workspace_id, shared_key)

# Transform quality results for Log Analytics
metrics = []
for r in all_results:
    metrics.append({
        "DatasetName": r["dataset_name"],
        "CheckName": r["check_name"],
        "CheckType": r["check_type"],
        "ColumnName": r["column_name"] or "",
        "Passed": r["passed"],
        "FailurePercent": r["failure_percent"],
        "Severity": r["severity"],
        "RunId": r["run_id"],
    })

client.send(metrics)
```

## Step 3: KQL Queries for Monitoring

Run these in the Log Analytics query editor:

```kql
// Quality failures in the last 24 hours
DataQuality_CL
| where TimeGenerated > ago(24h)
| where Passed_b == false
| summarize FailureCount=count() by DatasetName_s, CheckName_s, Severity_s
| order by FailureCount desc

// Quality score trend per dataset
DataQuality_CL
| where TimeGenerated > ago(7d)
| summarize
    TotalChecks=count(),
    PassedChecks=countif(Passed_b == true)
    by DatasetName_s, bin(TimeGenerated, 1d)
| extend QualityScore = round(toreal(PassedChecks) / TotalChecks * 100, 2)
| project TimeGenerated, DatasetName_s, QualityScore
| render timechart

// Critical failures (require immediate attention)
DataQuality_CL
| where TimeGenerated > ago(1h)
| where Passed_b == false and Severity_s == "critical"
| project TimeGenerated, DatasetName_s, CheckName_s, ColumnName_s, FailurePercent_d
| order by FailurePercent_d desc
```

## Step 4: Create Alert Rules

```bash
# Alert: Critical quality failure detected
az monitor scheduled-query create \
    --name "dq-critical-failure" \
    --resource-group $RESOURCE_GROUP \
    --scopes $LOG_ANALYTICS_ID \
    --condition "count > 0" \
    --condition-query "DataQuality_CL | where Passed_b == false and Severity_s == 'critical' | where TimeGenerated > ago(15m)" \
    --evaluation-frequency 5m \
    --window-size 15m \
    --severity 1 \
    --action-groups $ACTION_GROUP_ID \
    --description "Critical data quality check failed"

# Alert: Quality score dropped below threshold
az monitor scheduled-query create \
    --name "dq-low-quality-score" \
    --resource-group $RESOURCE_GROUP \
    --scopes $LOG_ANALYTICS_ID \
    --condition "count > 0" \
    --condition-query "
        DataQuality_CL
        | where TimeGenerated > ago(1h)
        | summarize Passed=countif(Passed_b == true), Total=count() by DatasetName_s
        | extend Score = toreal(Passed) / Total * 100
        | where Score < 80
    " \
    --evaluation-frequency 15m \
    --window-size 1h \
    --severity 2 \
    --action-groups $ACTION_GROUP_ID \
    --description "Quality score dropped below 80%"
```

## Step 5: Create Action Group

```bash
# Create action group for notifications
az monitor action-group create \
    --name "dq-alerts" \
    --resource-group $RESOURCE_GROUP \
    --short-name "dq-notify" \
    --email-receiver name="DataTeam" email-address="your-email@example.com"

ACTION_GROUP_ID=$(az monitor action-group show \
    --name "dq-alerts" \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)
```

## Step 6: SLA Monitoring

```python
# Notebook: notebooks/15_sla_monitor.py

from datetime import datetime, timedelta
import pyodbc

def check_pipeline_sla(conn_str: str, pipeline_name: str,
                        expected_by_hour: int, expected_by_minute: int = 0):
    """Check if a pipeline completed within its SLA window."""

    now = datetime.utcnow()
    expected_by = now.replace(hour=expected_by_hour, minute=expected_by_minute, second=0, microsecond=0)

    # If expected time hasn't passed yet today, check yesterday
    if now < expected_by:
        expected_by -= timedelta(days=1)

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Check if pipeline completed (look in lineage table)
    cursor.execute("""
        SELECT TOP 1 run_timestamp
        FROM dq_lineage
        WHERE pipeline_name = ? AND run_timestamp >= ? AND status = 'success'
        ORDER BY run_timestamp ASC
    """, (pipeline_name, expected_by - timedelta(hours=24)))

    row = cursor.fetchone()
    actual_arrival = row[0] if row else None
    sla_met = actual_arrival is not None and actual_arrival <= expected_by
    delay_minutes = int((actual_arrival - expected_by).total_seconds() / 60) if actual_arrival and not sla_met else 0

    # Record SLA check
    cursor.execute("""
        INSERT INTO dq_sla_tracking
        (pipeline_name, expected_by, actual_arrival, sla_met, delay_minutes)
        VALUES (?, ?, ?, ?, ?)
    """, (pipeline_name, expected_by, actual_arrival, sla_met, delay_minutes))

    conn.commit()
    cursor.close()
    conn.close()

    status = "✅ MET" if sla_met else "❌ MISSED"
    print(f"{status} | {pipeline_name} | Expected by: {expected_by} | "
          f"Actual: {actual_arrival or 'NOT ARRIVED'} | Delay: {delay_minutes}min")

    return sla_met

# Define SLAs
sla_definitions = [
    {"pipeline": "orders_ingestion", "expected_hour": 6},      # 6 AM UTC
    {"pipeline": "customers_ingestion", "expected_hour": 6},
    {"pipeline": "silver_transformation", "expected_hour": 8},  # 8 AM UTC
    {"pipeline": "gold_aggregation", "expected_hour": 10},      # 10 AM UTC
]

print("📊 SLA Status Report")
print("=" * 80)
for sla in sla_definitions:
    check_pipeline_sla(conn_str, sla["pipeline"], sla["expected_hour"])
```

## Interview Talking Points

1. **"How do you monitor data quality in production?"** → Three-tier approach: (1) Azure SQL stores detailed check results for history and trends, (2) Log Analytics ingests metrics for KQL queries and dashboards, (3) Azure Monitor alerts fire on critical failures and SLA breaches.

2. **"What's your alerting strategy?"** → Severity-based. Critical failures (null keys, referential integrity) trigger immediate Sev-1 alerts via PagerDuty. Warnings (minor format issues) aggregate into daily digest emails. SLA misses escalate based on delay duration.

3. **"How do you track pipeline SLAs?"** → Dedicated SLA tracking table. Each pipeline has an expected completion time. A monitoring job runs every 15 minutes, checks actual completion against expectations, and records the result. Dashboards show SLA compliance trends.

## Next Step

→ [Guide 08: Data Catalog & Governance](08_catalog_governance.md)
