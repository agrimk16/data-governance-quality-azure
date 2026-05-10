#!/bin/bash
# ============================================================
# Provision Azure Resources — Data Governance & Quality
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

echo "============================================"
echo "Provisioning Data Governance Resources"
echo "============================================"

# 1. Resource Group
echo "[1/6] Creating Resource Group..."
az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION \
    --tags environment=$TAG_ENVIRONMENT project=$TAG_PROJECT

# 2. ADLS Gen2
echo "[2/6] Creating ADLS Gen2 Storage..."
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true \
    --tags environment=$TAG_ENVIRONMENT project=$TAG_PROJECT

for container in raw validated quarantine profiling metadata; do
    az storage container create \
        --name $container \
        --account-name $STORAGE_ACCOUNT \
        --auth-mode login 2>/dev/null || true
done

# 3. Key Vault
echo "[3/6] Creating Key Vault..."
az keyvault create \
    --name $KEYVAULT_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --tags environment=$TAG_ENVIRONMENT project=$TAG_PROJECT

# 4. Azure SQL
echo "[4/6] Creating Azure SQL Database..."
az sql server create \
    --name $SQL_SERVER_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --admin-user $SQL_ADMIN_USER \
    --admin-password "$SQL_ADMIN_PASSWORD"

az sql db create \
    --name $SQL_DATABASE_NAME \
    --server $SQL_SERVER_NAME \
    --resource-group $RESOURCE_GROUP \
    --edition Basic \
    --capacity 5

# Allow Azure services
az sql server firewall-rule create \
    --server $SQL_SERVER_NAME \
    --resource-group $RESOURCE_GROUP \
    --name "AllowAzureServices" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0

# Allow your IP
MY_IP=$(curl -s ifconfig.me)
az sql server firewall-rule create \
    --server $SQL_SERVER_NAME \
    --resource-group $RESOURCE_GROUP \
    --name "AllowMyIP" \
    --start-ip-address $MY_IP \
    --end-ip-address $MY_IP

# 5. Databricks Workspace
echo "[5/6] Creating Databricks Workspace..."
az databricks workspace create \
    --name $DATABRICKS_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku $DATABRICKS_SKU \
    --tags environment=$TAG_ENVIRONMENT project=$TAG_PROJECT

# 6. Log Analytics Workspace
echo "[6/6] Creating Log Analytics Workspace..."
az monitor log-analytics workspace create \
    --workspace-name $LOG_ANALYTICS_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku PerGB2018 \
    --tags environment=$TAG_ENVIRONMENT project=$TAG_PROJECT

# Store secrets in Key Vault
echo "Storing secrets in Key Vault..."
az keyvault secret set --vault-name $KEYVAULT_NAME --name "sql-server-name" --value "$SQL_SERVER_NAME" > /dev/null
az keyvault secret set --vault-name $KEYVAULT_NAME --name "sql-database-name" --value "$SQL_DATABASE_NAME" > /dev/null
az keyvault secret set --vault-name $KEYVAULT_NAME --name "sql-admin-user" --value "$SQL_ADMIN_USER" > /dev/null
az keyvault secret set --vault-name $KEYVAULT_NAME --name "sql-admin-password" --value "$SQL_ADMIN_PASSWORD" > /dev/null

STORAGE_KEY=$(az storage account keys list --account-name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query '[0].value' -o tsv)
az keyvault secret set --vault-name $KEYVAULT_NAME --name "storage-access-key" --value "$STORAGE_KEY" > /dev/null

LOG_WORKSPACE_ID=$(az monitor log-analytics workspace show --workspace-name $LOG_ANALYTICS_WORKSPACE --resource-group $RESOURCE_GROUP --query customerId -o tsv)
LOG_SHARED_KEY=$(az monitor log-analytics workspace get-shared-keys --workspace-name $LOG_ANALYTICS_WORKSPACE --resource-group $RESOURCE_GROUP --query primarySharedKey -o tsv)
az keyvault secret set --vault-name $KEYVAULT_NAME --name "log-analytics-workspace-id" --value "$LOG_WORKSPACE_ID" > /dev/null
az keyvault secret set --vault-name $KEYVAULT_NAME --name "log-analytics-shared-key" --value "$LOG_SHARED_KEY" > /dev/null

echo ""
echo "============================================"
echo "✅ All resources provisioned!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Run sql/create_metadata_tables.sql against Azure SQL"
echo "  2. Upload sample data: data/raw/*.csv → ADLS raw container"
echo "  3. Configure Databricks cluster and install great-expectations"
