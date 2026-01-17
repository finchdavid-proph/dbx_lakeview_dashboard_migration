# Databricks Dashboard Migration Tool

A Python script to migrate legacy Databricks SQL dashboards to AI/BI (Lakeview) dashboards, with comprehensive logging, filtering, and email notification capabilities.

## Overview

This tool automates the migration of legacy Databricks SQL dashboards to the new AI/BI (Lakeview) format. It provides:

- **Selective Migration**: Choose specific dashboards by ID, CSV file, or filter patterns
- **Dry-Run Mode**: Preview what will be migrated without making changes
- **Comprehensive Logging**: Detailed CSV output with all dashboard information
- **Resume Capability**: Skip already-migrated dashboards when resuming
- **Email Notifications**: Optional email summaries of migration results
- **Error Handling**: Automatic retries and detailed error reporting

## Features

### Core Functionality
- ✅ List all legacy SQL dashboards
- ✅ List all Lakeview (AI/BI) dashboards
- ✅ Migrate legacy dashboards to Lakeview format
- ✅ Publish migrated dashboards (optional)
- ✅ Delete legacy dashboards after migration (optional)
- ✅ Generate comprehensive CSV reports
- ✅ **Multi-workspace support** - Process multiple Databricks workspaces in a single run

### Advanced Features
- 🔍 Filter dashboards by path, owner, or name (regex patterns)
- 📋 Select specific dashboards by ID or CSV file
- 🔄 Resume from previous migration runs
- 📧 Email summary notifications
- 🧪 Dry-run mode for testing
- ⚙️ Configuration file support (YAML/JSON)
- 🔁 Automatic retry on failures
- 📊 Detailed logging and error reporting
- 🏢 **Workspace identification** - CSV output includes workspace name for each dashboard

## Project Structure

```
dbx_lakeview_dashboard_migration/
├── src/
│   └── migrate_dashboards.py    # Main migration script
├── config/
│   └── config.example.yaml     # Example configuration file
├── data/                          # CSV files (logs, dashboard lists)
│   └── .gitkeep
├── .venv/                      # Virtual environment (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.7 or higher
- Databricks workspace with:
  - Legacy SQL dashboards (to migrate)
  - AI/BI (Lakeview) enabled
  - Personal Access Token (PAT) with appropriate permissions
- Required Python packages:
  - `pandas`
  - `requests`
  - `pyyaml` (optional, for YAML config files)

## Installation

1. **Clone or download this repository**
   ```bash
   git clone <repository-url>
   cd dbx_lakeview_dashboard_migration
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install required packages**:
   ```bash
   pip install -r requirements.txt
   ```
   
   Or install manually:
   ```bash
   pip install pandas requests pyyaml
   ```

## Quick Start

### Basic Usage

**Using command-line arguments:**
```bash
python src/migrate_dashboards.py \
  --host https://your-workspace.cloud.databricks.com \
  --token your-databricks-token
```

**Using a configuration file:**
```bash
python src/migrate_dashboards.py --config config/config.yaml
```

**Dry-run mode** (collect info only, no changes):
```bash
python src/migrate_dashboards.py --config config/config.yaml --dry-run
```

## Configuration

### Configuration File

Create a configuration file (YAML or JSON) to store your settings. See `config/config.example.yaml` for a complete example.

#### Single Workspace Configuration

**Example `config.yaml` (single workspace):**
```yaml
# Databricks Connection
host: "https://your-workspace.cloud.databricks.com"
token: "your-databricks-token"  # Or use DATABRICKS_TOKEN env var
workspace-name: "production"  # Optional workspace identifier

# Migration Options
delete-legacy: false
publish: false
warehouse-id: null

# Execution Options
dry-run: false
log-file: "dashboard_migration_log.csv"
log-level: "INFO"

# Retry Settings
sleep-between-calls: 0.2
max-retries: 3
retry-delay: 1.0
```

#### Multiple Workspaces Configuration

**Example `config.yaml` (multiple workspaces):**
```yaml
# Multiple Databricks Workspaces
workspaces:
  - name: "production"
    host: "https://prod-workspace.cloud.databricks.com"
    token: "dapi_production_token"
  
  - name: "staging"
    host: "https://staging-workspace.cloud.databricks.com"
    token: "dapi_staging_token"
  
  - name: "development"
    host: "https://dev-workspace.cloud.databricks.com"
    token: "dapi_dev_token"

# Migration Options (applied to all workspaces)
delete-legacy: false
publish: false
warehouse-id: null

# Execution Options
dry-run: false
log-file: "dashboard_migration_log.csv"
log-level: "INFO"
```

**Note:** 
- Command-line arguments always override configuration file values
- When using multiple workspaces, all workspaces are processed sequentially
- Each workspace's dashboards are identified in the CSV output with the workspace name
- If a workspace fails, processing continues with the next workspace

### Environment Variables

You can also set these via environment variables:
- `DATABRICKS_HOST` - Workspace URL
- `DATABRICKS_TOKEN` - Personal Access Token
- `EMAIL_FROM` - Sender email address
- `SMTP_SERVER` - SMTP server hostname
- `SMTP_PORT` - SMTP server port
- `SMTP_USERNAME` - SMTP username
- `SMTP_PASSWORD` - SMTP password

## Usage Examples

### 1. Collect Dashboard Information (Dry-Run)

Collect information about all dashboards without making any changes:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --dry-run \
  --log-file "data/all_dashboards_info.csv"
```

### 2. Migrate All Dashboards

Migrate all legacy dashboards to Lakeview:

```bash
python src/migrate_dashboards.py \
  --host https://workspace.cloud.databricks.com \
  --token $DATABRICKS_TOKEN
```

### 3. Migrate and Publish

Migrate dashboards and automatically publish them:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --publish \
  --warehouse-id "your-warehouse-id"
```

### 4. Migrate Specific Dashboards by ID

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --dashboard-ids "123,456,789"
```

### 5. Migrate Dashboards from CSV File

Create a CSV file with a column named `legacy_id`, `id`, `dashboard_id`, or `dashboardId`:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --dashboard-csv "data/selected_dashboards.csv"
```

### 6. Filter Dashboards by Path

Migrate only dashboards in a specific path:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --filter-path "/Users/team.*"
```

### 7. Migrate, Publish, and Delete Legacy

Complete migration workflow:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --publish \
  --delete-legacy \
  --warehouse-id "your-warehouse-id"
```

### 8. Resume from Previous Run

Skip already-migrated dashboards:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --resume
```

### 9. Send Email Summary

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --send-email \
  --email-to "team@company.com" \
  --smtp-username "your-email@gmail.com" \
  --smtp-password "your-app-password"
```

### 10. Combine Filters

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --filter-path "/Users/team" \
  --filter-owner "admin@company.com" \
  --filter-name ".*production.*"
```

### 11. Process Multiple Workspaces

Configure multiple workspaces in your config file and process them all:

```bash
python src/migrate_dashboards.py --config config/config.yaml
```

The script will process each workspace sequentially and combine all results into a single CSV file with workspace identification.

## Command-Line Options

### Required
- `--host` - Databricks workspace URL
- `--token` - Databricks Personal Access Token

### Migration Options
- `--delete-legacy` - Delete legacy dashboards after successful migration
- `--publish` - Publish all migrated dashboards
- `--warehouse-id` - Warehouse ID to use when publishing

### Selection and Filtering
- `--dashboard-ids` - Comma-separated list of dashboard IDs
- `--dashboard-csv` - Path to CSV file with dashboard IDs
- `--filter-path` - Regex pattern to match dashboard path
- `--filter-owner` - Regex pattern to match dashboard owner
- `--filter-name` - Regex pattern to match dashboard name

### Execution Options
- `--config` - Path to configuration file (YAML or JSON)
- `--dry-run` - Simulate without making API calls
- `--resume` - Resume from existing log file
- `--log-file` - Path to CSV log file (default: `data/dashboard_migration_log.csv`)
- `--log-level` - Logging level: DEBUG, INFO, WARNING, ERROR

### Retry and Rate Limiting
- `--sleep-between-calls` - Seconds to sleep between API calls (default: 0.2)
- `--max-retries` - Maximum retry attempts (default: 3)
- `--retry-delay` - Initial delay between retries in seconds (default: 1.0)

### Email Notifications
- `--send-email` - Send email summary after migration
- `--email-to` - Comma-separated recipient email addresses
- `--email-from` - Sender email address
- `--smtp-server` - SMTP server hostname (default: smtp.gmail.com)
- `--smtp-port` - SMTP server port (default: 587)
- `--smtp-username` - SMTP username
- `--smtp-password` - SMTP password

## Output Format

The script generates a CSV file with the following columns:

### Core Fields
- `workspace` - **Workspace identifier/name** (identifies which workspace the dashboard belongs to)
- `legacy_id` - Legacy dashboard ID (empty for Lakeview dashboards)
- `legacy_name` - Legacy dashboard name (empty for Lakeview dashboards)
- `legacy_path` - Legacy dashboard path (empty for Lakeview dashboards)
- `legacy_created_at` - Legacy dashboard creation timestamp
- `lakeview_id` - Lakeview (AI/BI) dashboard ID
- `migrated` - Whether the dashboard is migrated (True/False)
- `migration_datetime` - Migration timestamp (ISO 8601)
- `published` - Whether the dashboard is published (True/False)
- `publish_datetime` - Publication timestamp (ISO 8601)
- `published_date` - Publication date (datetime, ISO 8601)
- `deleted_legacy` - Whether legacy dashboard was deleted (True/False)
- `error` - Error message if migration failed
- `dashboard_type` - Type: "legacy" or "lakeview"

### Dashboard Information Fields
- `name` - Dashboard name
- `path` - Dashboard path (parent directory)
- `created_date` - Creation date (datetime, ISO 8601)
- `updated_at` - Last update timestamp (datetime, ISO 8601)
- `owner` - Dashboard owner (if available in API)
- `description` - Dashboard description (if available in API)

### Example Output

**Single workspace:**
```csv
workspace,legacy_id,legacy_name,legacy_path,lakeview_id,migrated,published,published_date,name,path,created_date,updated_at
production,123,My Dashboard,/Users/team,01effe81425911e090b6a3b21b3d6c52,True,True,2025-03-14T13:06:07.971Z,My Dashboard,/Users/team,2025-03-11T14:00:58.914Z,2025-03-11T14:01:29.208Z
```

**Multiple workspaces:**
```csv
workspace,legacy_id,legacy_name,legacy_path,lakeview_id,migrated,published,published_date,name,path,created_date,updated_at
production,123,Prod Dashboard,/Users/team,01effe81425911e090b6a3b21b3d6c52,True,True,2025-03-14T13:06:07.971Z,Prod Dashboard,/Users/team,2025-03-11T14:00:58.914Z,2025-03-11T14:01:29.208Z
staging,456,Staging Dashboard,/Users/team,01effe8185d012bcb9c429e844a755ef,True,False,,Staging Dashboard,/Users/team,2025-03-11T14:02:52.102Z,2025-03-14T23:32:54.221Z
development,789,Dev Dashboard,/Users/team,01f00122ad981492964b1599e55c9478,True,False,,Dev Dashboard,/Users/team,2025-03-14T22:21:30.143Z,2025-03-14T22:21:30.234Z
```

## API Endpoints Used

The script uses the following Databricks REST API endpoints:

- `GET /api/2.0/preview/sql/dashboards` - List legacy dashboards
- `POST /api/2.0/lakeview/dashboards/migrate` - Migrate legacy to Lakeview
- `GET /api/2.0/lakeview/dashboards` - List Lakeview dashboards
- `GET /api/2.0/lakeview/dashboards/{id}` - Get dashboard details
- `GET /api/2.0/lakeview/dashboards/{id}/published` - Get published status
- `POST /api/2.0/lakeview/dashboards/{id}/published` - Publish dashboard
- `DELETE /api/2.0/preview/sql/dashboards/{id}` - Delete legacy dashboard

## Troubleshooting

### Common Issues

**1. "No dashboards found"**
- Verify your token has appropriate permissions
- Check that you're using the correct workspace URL
- Ensure dashboards exist in the workspace

**2. "Authentication failed"**
- Verify your Personal Access Token is valid
- Check that the token hasn't expired
- Ensure the token has the necessary permissions

**3. "Failed to migrate dashboard"**
- Check the error message in the CSV log file
- Verify the dashboard isn't already migrated
- Ensure you have write permissions

**4. "Published endpoint returns 404"**
- This is normal for unpublished dashboards
- The script handles this gracefully

**5. "SMTP email failed"**
- Verify SMTP credentials are correct
- For Gmail, use an App Password (not your regular password)
- Check firewall settings for SMTP port

### Debug Mode

Enable detailed logging:

```bash
python src/migrate_dashboards.py \
  --config config/config.yaml \
  --log-level DEBUG
```

### Checking Logs

The CSV log file contains detailed information about each dashboard:
- Migration status
- Error messages
- Timestamps
- Published status

## Multi-Workspace Support

The script supports processing multiple Databricks workspaces in a single run. This is useful for:
- Migrating dashboards across production, staging, and development environments
- Centralized reporting across multiple workspaces
- Consistent migration policies across all workspaces

### How It Works

1. **Configure workspaces** in your config file using the `workspaces` list
2. **Each workspace** is processed sequentially with its own authentication
3. **All results** are combined into a single CSV file
4. **Workspace identification** is included in the `workspace` column
5. **If one workspace fails**, processing continues with the next workspace

### Example: Multi-Workspace Configuration

```yaml
workspaces:
  - name: "production"
    host: "https://prod-workspace.cloud.databricks.com"
    token: "dapi_prod_token"
  
  - name: "staging"
    host: "https://staging-workspace.cloud.databricks.com"
    token: "dapi_staging_token"

# All other settings apply to all workspaces
dry-run: false
publish: true
delete-legacy: false
```

### Benefits

- **Unified reporting**: All dashboards from all workspaces in one CSV
- **Consistent processing**: Same filters and options applied to all workspaces
- **Error isolation**: One workspace failure doesn't stop others
- **Workspace tracking**: Easy to identify which workspace each dashboard belongs to

## Best Practices

1. **Always run in dry-run mode first** to preview what will be migrated
2. **Use resume mode** for large migrations to avoid re-processing
3. **Filter dashboards** to migrate in smaller batches
4. **Keep backup** of legacy dashboards before deleting
5. **Review CSV output** before deleting legacy dashboards
6. **Test with a single dashboard** before bulk migration
7. **Monitor rate limits** - adjust `sleep-between-calls` if needed
8. **For multi-workspace**: Test with one workspace first, then add more
9. **Use descriptive workspace names** for easy identification in CSV output

## Limitations

- Owner and description fields may not be available in the API for all dashboards
- Published date is only available for published dashboards
- Large migrations may take significant time (use filters to batch)
- SMTP email requires valid credentials and network access

## License

This script is provided as-is for use with Databricks workspaces.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review the CSV log file for detailed error messages
3. Enable DEBUG logging for more details
4. Verify API permissions and workspace access

## Version History

- **v1.0** - Initial release with full migration capabilities
  - Legacy to Lakeview migration
  - Comprehensive logging
  - Email notifications
  - Filtering and selection options
  - Resume capability
  - Dry-run mode
