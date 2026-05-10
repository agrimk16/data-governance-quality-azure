# Guide 01: Environment Setup

## Prerequisites

- Azure CLI installed and authenticated (`az login`)
- Pay-as-you-go Azure subscription
- Bash terminal

## Step 1: Configure Variables

```bash
cp setup/variables.sh.template setup/variables.sh
nano setup/variables.sh
```

Update all placeholder values — especially the resource group name, location, and SQL password.

## Step 2: Provision Resources

```bash
chmod +x setup/provision_resources.sh
./setup/provision_resources.sh
```

This creates:

1. **Resource Group** — container for all resources
2. **ADLS Gen2** — data lake with containers: `raw`, `validated`, `quarantine`, `profiling`, `metadata`
3. **Azure SQL Database** — metadata store for quality results, lineage, audit trail
4. **Databricks Workspace** — compute for quality checks and profiling
5. **Key Vault** — secrets management
6. **Log Analytics Workspace** — centralized monitoring

## Step 3: Upload Sample Data

Upload the sample CSV files to the `raw` container:

```bash
source setup/variables.sh

# Upload sample data files
for file in data/raw/*.csv; do
    az storage blob upload \
        --account-name $STORAGE_ACCOUNT \
        --container-name raw \
        --file "$file" \
        --name "$(basename $file)" \
        --auth-mode login \
        --overwrite
done
```

## Step 4: Create Metadata Tables

Connect to Azure SQL and run the schema creation script:

```bash
# Option 1: Via sqlcmd
sqlcmd -S "$SQL_SERVER_NAME.database.windows.net" \
    -d $SQL_DATABASE_NAME \
    -U $SQL_ADMIN_USER \
    -P "$SQL_ADMIN_PASSWORD" \
    -i sql/create_metadata_tables.sql

# Option 2: Via Azure Portal → Query Editor
# Copy and paste the contents of sql/create_metadata_tables.sql
```

## Step 5: Configure Databricks

1. Navigate to the Databricks workspace in Azure Portal
2. Click "Launch Workspace"
3. Create a cluster:
   - **Name:** `quality-engine`
   - **Node type:** Standard_DS3_v2
   - **Workers:** 0 (single node for development)
   - **Auto termination:** 20 minutes
   - **Databricks Runtime:** 13.3 LTS
4. Install libraries on the cluster:
   - `great-expectations` (PyPI)
   - `pyodbc` (PyPI)

## Step 6: Mount ADLS Gen2 in Databricks

Create a notebook and run:

```python
# Using service principal or OAuth — for learning, use access key from Key Vault

storage_account = "YOUR_STORAGE_ACCOUNT"
access_key = dbutils.secrets.get(scope="keyvault-scope", key="storage-access-key")

for container in ["raw", "validated", "quarantine", "profiling", "metadata"]:
    dbutils.fs.mount(
        source=f"wasbs://{container}@{storage_account}.blob.core.windows.net",
        mount_point=f"/mnt/{container}",
        extra_configs={
            f"fs.azure.account.key.{storage_account}.blob.core.windows.net": access_key
        }
    )

# Verify
display(dbutils.fs.ls("/mnt/raw"))
```

> **Note:** For production, use Unity Catalog external locations instead of mounts. Mounts are used here for simplicity.

## Step 7: Verify Setup

Run these checks to confirm everything is working:

```bash
# Check resource group
az resource list --resource-group $RESOURCE_GROUP --output table

# Check storage containers
az storage container list --account-name $STORAGE_ACCOUNT --auth-mode login --output table

# Check SQL connectivity
az sql db show --name $SQL_DATABASE_NAME --server $SQL_SERVER_NAME --resource-group $RESOURCE_GROUP --output table
```

## Next Step

→ [Guide 02: Quality Framework Design](02_quality_framework.md)
