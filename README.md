# Databricks Multi-User Data Export with Unity Catalog Security

A secure pattern for orchestrating multi-tenant data exports through the Databricks platform, maintaining Unity Catalog security with table access, row-level security, and column-level masking.

## Scenario

Organizations need to facilitate secure data exports across multiple business units or regions while maintaining strict data governance. This solution demonstrates how to implement a scalable multi-tenant data export architecture on Databricks that:

- **Isolates data access** by region/tenant using Unity Catalog row-level security (RLS)
- **Protects sensitive information** through column-level masking (CLS)
- **Enforces least privilege** by using dedicated service principals per scope
- **Maintains auditability** with clear separation between orchestration and data access
- **Scales efficiently** to 50+ concurrent export jobs without platform limitations

The architecture uses service principals instead of user accounts to ensure reproducible, automated workflows that don't depend on individual user availability or permissions.

## Security Principles

### 1. Service Principal-Based Architecture
- **Dedicated service principals per access scope** - Each region/tenant gets its own service principal with minimal required permissions
- **No user-based scheduling** - Avoids production failures when users leave or permissions change
- **API-only access** - Service principals cannot login to UI, reducing attack surface

### 2. Least Privilege Access
- **Orchestrator principals** - Can manage jobs but have no direct data access
- **Data access principals** - Can only read specific tables with RLS/CLS applied
- **Separation of concerns** - Job creation separated from data access permissions

### 3. Defense in Depth
- **Unity Catalog groups** - Centralized permission management
- **Row-level security** - Automatic filtering based on principal identity
- **Column-level masking** - Sensitive data protection (PII, PHI)
- **Workspace isolation** - Service principals scoped to specific workspaces

### 4. Reproducibility & Automation
- **Immutable identities** - Service principals provide consistent execution context
- **Automated lifecycle** - Creation, rotation, and deletion through APIs

## Prerequisites

### Required Databricks Components
- Databricks workspace with Unity Catalog enabled
- Account admin or workspace admin privileges for initial setup
- Personal Access Token (PAT) setup for API authentication
- Azure Service Principal configured

### Required Inputs for Orchestration Notebook

Configure these widget parameters in `orchestration.ipynb`:

```python
# Authentication
pat_token         # Your Databricks Personal Access Token
catalog_name      # Unity Catalog name (e.g., "security_demo")
schema_name       # Schema name (e.g., "orders_data")

# Entra ID Service Principal (if using Azure)
app_sp_client_id      # Application (client) ID from Azure portal
app_sp_client_secret  # Client secret value
app_sp_tenant_id      # Azure AD tenant ID
app_sp_display_name   # Display name (e.g., "marcin-sp-app-orchestrator")
```

### Manual Setup Steps

Before running the orchestration notebook, complete these manual steps:

#### 1. Service Principal Group Membership
After the notebook creates service principals, manually add them to Unity Catalog groups:
- Add `marcin_sp_user_us_region` to group `marcin_us_region_access`
- Add `marcin_sp_user_eu_region` to group `marcin_eu_region_access`
- Assign role: `Service Account User` to both

#### 2. Notebook Permissions
Grant READ permissions on `process_regional_data.ipynb` to:
- `marcin_sp_user_us_region`
- `marcin_sp_user_eu_region`

This allows the service principals to execute the notebook when jobs run.

#### 3. Workspace Access
Ensure service principals have workspace access entitlements:
- `allow-cluster-create`
- `databricks-sql-access`
- `workspace-access`

## Repository Structure

```
dbx-multi-user-extract/
├── notebooks
│   ├── orchestration.ipynb
│   └── process_regional_data.ipynb
├── README.md
└── src
    ├── azure
    │   └── identity
    │       └── entra_sp_manager.py
    └── databricks
        ├── identity
        │   └── workspace_sp_manager.py
        └── jobs
            └── job_manager.py
```

### Component Descriptions

**orchestration.ipynb**
- Creates demo tables using Databricks sample data
- Sets up service principals and Unity Catalog groups
- Implements row-level and column-level security
- Creates and runs jobs with different permission scopes

**process_regional_data.ipynb**
- Executes under service principal context
- Queries data with automatic RLS/CLS applied
- Returns results showing accessible data scope

**Python Modules**
- `entra_sp_manager.py` - Handles Azure AD service principal OAuth and registration
- `workspace_sp_manager.py` - Manages workspace service principals and UC groups
- `job_manager.py` - Creates serverless jobs with proper "run as" configuration

## Usage

1. **Initial Setup**
   ```python
   # Run orchestration notebook with required parameters
   # This creates all infrastructure and demonstrates the pattern
   ```

2. **Job Execution Flow**
   - Orchestrator SP creates jobs configured to run as data access SPs
   - Each job executes with limited permissions based on UC groups
   - RLS automatically filters data by region
   - CLS masks sensitive columns

3. **Monitoring**
   - Jobs show clear attribution (created by X, runs as Y)
   - Audit logs capture all access with service principal identity
   - Failed permission attempts logged for security review

## Production Considerations

This is not a production-grade repository, and is to be used on behalf of demo purposes only. Use at your own risk.
