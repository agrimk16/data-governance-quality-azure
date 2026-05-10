#!/bin/bash
# ============================================================
# Teardown Azure Resources — Data Governance & Quality
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

echo "============================================"
echo "⚠️  TEARDOWN: Data Governance Resources"
echo "============================================"
echo ""
echo "This will DELETE the resource group: $RESOURCE_GROUP"
echo "  - Databricks Workspace"
echo "  - ADLS Gen2 storage and all data"
echo "  - Azure SQL database and metadata"
echo "  - Key Vault and all secrets"
echo "  - Log Analytics workspace"
echo ""
read -p "Are you sure? (yes/no): " CONFIRM

if [[ "$CONFIRM" != "yes" ]]; then
    echo "Teardown cancelled."
    exit 0
fi

echo ""
echo "Deleting resource group: $RESOURCE_GROUP..."
az group delete \
    --name $RESOURCE_GROUP \
    --yes \
    --no-wait

echo ""
echo "✅ Resource group deletion initiated (runs in background)."
echo "   Monitor: az group show --name $RESOURCE_GROUP"
