#!/usr/bin/env python3
"""
Migrate legacy Databricks SQL dashboards to AI/BI (Lakeview).

This script:
1. Lists all legacy SQL dashboards (or selects specific ones)
2. Migrates them to AI/BI (Lakeview) dashboards
3. Optionally publishes the migrated dashboards
4. Optionally deletes the legacy dashboards after successful migration
5. Logs all actions to a CSV file
6. Optionally sends email summary of migration results

Usage:
    python src/migrate_dashboards.py --host https://workspace.cloud.databricks.com --token <token>
    python src/migrate_dashboards.py --config config/config.yaml
    python src/migrate_dashboards.py --publish --warehouse-id <warehouse-id>
    python src/migrate_dashboards.py --delete-legacy --publish
    python src/migrate_dashboards.py --dry-run --filter-path "/Users/team"
    python src/migrate_dashboards.py --dashboard-ids "123,456" --send-email --email-to "team@company.com"
    python src/migrate_dashboards.py --dashboard-csv data/dashboard_list.csv --publish
"""

import argparse
import email.utils
import json
import logging
import os
import re
import smtplib
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# ============================================================================
# Configuration Constants
# ============================================================================

PAGE_SIZE = 100
REQUEST_TIMEOUT = 30
DEFAULT_SLEEP_BETWEEN_CALLS = 0.2
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1

# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Utility Functions
# ============================================================================

def retry_on_failure(max_retries: int = DEFAULT_MAX_RETRIES, delay: float = DEFAULT_RETRY_DELAY):
    """
    Decorator to retry function calls on transient failures.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries (increases with each attempt)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = delay * (attempt + 1)
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): "
                            f"{type(e).__name__}. Retrying in {sleep_time}s..."
                        )
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts")
            if last_exception:
                raise last_exception
        return wrapper
    return decorator


def extract_error_details(response: requests.Response) -> str:
    """
    Extract detailed error information from API response.
    
    Args:
        response: HTTP response object
        
    Returns:
        str: Error message extracted from response
    """
    try:
        error_json = response.json()
        # Try common error message locations
        if isinstance(error_json, dict):
            error_msg = (
                error_json.get('error', {}).get('message') or
                error_json.get('message') or
                error_json.get('error') or
                str(error_json)
            )
            return error_msg
        return response.text
    except (ValueError, AttributeError):
        return response.text


def normalize_host(host: Optional[str]) -> Optional[str]:
    """
    Normalize host URL by removing trailing slash and validating format.
    
    Args:
        host: Databricks workspace URL
        
    Returns:
        str: Normalized host URL
        
    Raises:
        ValueError: If host format is invalid
    """
    if not host:
        return None
    host = host.rstrip('/')
    if not host.startswith(('http://', 'https://')):
        raise ValueError(f"Host must start with http:// or https://: {host}")
    return host


def validate_warehouse_id(warehouse_id: Optional[str]) -> None:
    """
    Validate warehouse ID format.
    
    Args:
        warehouse_id: Warehouse ID to validate
        
    Raises:
        ValueError: If warehouse ID format is invalid
    """
    if warehouse_id:
        # Databricks warehouse IDs are typically alphanumeric with hyphens
        cleaned = warehouse_id.replace('-', '').replace('_', '')
        if not cleaned.isalnum() or len(warehouse_id) < 8:
            raise ValueError(
                f"Invalid warehouse ID format: {warehouse_id}. "
                "Expected alphanumeric string with optional hyphens/underscores."
            )


def load_dashboard_selection(
    dashboard_ids: Optional[str] = None,
    dashboard_csv: Optional[str] = None
) -> Optional[List[str]]:
    """
    Load dashboard IDs from various sources.
    
    Args:
        dashboard_ids: Comma-separated list of dashboard IDs
        dashboard_csv: Path to CSV file with dashboard info
        
    Returns:
        List of dashboard IDs to migrate, or None if no selection specified
    """
    selected_ids = []
    
    # Load from comma-separated list
    if dashboard_ids:
        selected_ids.extend([id.strip() for id in dashboard_ids.split(',') if id.strip()])
        logger.info(f"Loaded {len(selected_ids)} dashboard IDs from --dashboard-ids")
    
    # Load from CSV file
    if dashboard_csv:
        try:
            df = pd.read_csv(dashboard_csv)
            # Try common column names for dashboard ID
            id_column = None
            for col in ['legacy_id', 'id', 'dashboard_id', 'dashboardId']:
                if col in df.columns:
                    id_column = col
                    break
            
            if id_column:
                csv_ids = df[id_column].dropna().astype(str).tolist()
                selected_ids.extend(csv_ids)
                logger.info(f"Loaded {len(csv_ids)} dashboard IDs from CSV file {dashboard_csv}")
            else:
                logger.warning(
                    f"CSV file {dashboard_csv} does not contain a recognized ID column. "
                    f"Expected one of: legacy_id, id, dashboard_id, dashboardId"
                )
        except Exception as e:
            logger.error(f"Failed to load dashboard IDs from CSV {dashboard_csv}: {e}")
    
    # Remove duplicates while preserving order
    if selected_ids:
        seen = set()
        unique_ids = []
        for id in selected_ids:
            if id not in seen:
                seen.add(id)
                unique_ids.append(id)
        return unique_ids
    
    return None


def filter_dashboards(
    dashboards: List[Dict],
    path_pattern: Optional[str] = None,
    owner_pattern: Optional[str] = None,
    name_pattern: Optional[str] = None,
    selected_ids: Optional[List[str]] = None
) -> List[Dict]:
    """
    Filter dashboards based on path, owner, name patterns, or selected IDs.
    
    Args:
        dashboards: List of dashboard dictionaries
        path_pattern: Regex pattern to match dashboard path
        owner_pattern: Regex pattern to match dashboard owner
        name_pattern: Regex pattern to match dashboard name
        selected_ids: List of specific dashboard IDs to include
        
    Returns:
        List[Dict]: Filtered list of dashboards
    """
    filtered = dashboards
    
    # First, filter by selected IDs if provided (most restrictive - only these dashboards)
    if selected_ids:
        id_set = set(str(id) for id in selected_ids)
        filtered = [d for d in filtered if str(d.get("id", "")) in id_set]
        logger.info(f"Filtered by selected IDs: {len(filtered)} dashboards")
        if len(filtered) == 0:
            logger.warning("No dashboards found matching the provided IDs. Check that IDs are correct.")
            return filtered
    
    # Then apply pattern filters (AND logic - all filters must match)
    if path_pattern:
        pattern = re.compile(path_pattern, re.IGNORECASE)
        before_count = len(filtered)
        filtered = [d for d in filtered if pattern.search(d.get("path", "") or d.get("dashboard_path", ""))]
        logger.info(f"Filtered by path pattern '{path_pattern}': {len(filtered)} dashboards (from {before_count})")
    
    if owner_pattern:
        pattern = re.compile(owner_pattern, re.IGNORECASE)
        before_count = len(filtered)
        # Try multiple possible owner field locations
        filtered = [
            d for d in filtered
            if pattern.search(d.get("owner", "") or 
                            d.get("user", {}).get("user_name", "") or
                            d.get("user_name", "") or
                            str(d.get("user", {}).get("id", "")))
        ]
        logger.info(f"Filtered by owner pattern '{owner_pattern}': {len(filtered)} dashboards (from {before_count})")
    
    if name_pattern:
        pattern = re.compile(name_pattern, re.IGNORECASE)
        before_count = len(filtered)
        filtered = [d for d in filtered if pattern.search(d.get("name", ""))]
        logger.info(f"Filtered by name pattern '{name_pattern}': {len(filtered)} dashboards (from {before_count})")
    
    return filtered


def send_email_summary(
    df: pd.DataFrame,
    args: argparse.Namespace,
    host: str
) -> None:
    """
    Send email summary of migration results via SMTP.
    
    Args:
        df: Migration log DataFrame
        args: Parsed command line arguments
        host: Databricks workspace URL
    """
    if not args.send_email:
        return
    
    if not args.email_to:
        logger.warning("--send-email specified but --email-to not provided. Skipping email.")
        return
    
    # Prepare email content
    total = len(df)
    if total == 0:
        logger.warning("No dashboards to include in email summary")
        return
    
    migrated = df['migrated'].sum()
    failed = (~df['migrated']).sum()
    published = df['published'].sum() if args.publish and 'published' in df.columns else 0
    deleted = df['deleted_legacy'].sum() if args.delete_legacy and 'deleted_legacy' in df.columns else 0
    
    subject = f"Dashboard Migration Summary - {migrated}/{total} migrated"
    
    # Create HTML email body
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #4CAF50; color: white; }}
            .success {{ color: green; }}
            .error {{ color: red; }}
            .summary {{ background-color: #f2f2f2; padding: 15px; margin: 20px 0; }}
        </style>
    </head>
    <body>
        <h2>Dashboard Migration Summary</h2>
        <div class="summary">
            <h3>Migration Statistics</h3>
            <ul>
                <li><strong>Total Dashboards:</strong> {total}</li>
                <li><strong class="success">Successfully Migrated:</strong> {migrated}</li>
                <li><strong class="error">Failed Migrations:</strong> {failed}</li>
                {f'<li><strong>Published:</strong> {published}</li>' if args.publish else ''}
                {f'<li><strong>Deleted Legacy:</strong> {deleted}</li>' if args.delete_legacy else ''}
            </ul>
        </div>
        
        <h3>Migration Details</h3>
        <table>
            <tr>
                <th>Legacy ID</th>
                <th>Name</th>
                <th>Path</th>
                <th>Lakeview ID</th>
                <th>Status</th>
                <th>Error</th>
            </tr>
    """
    
    # Add rows for each dashboard
    for _, row in df.iterrows():
        status = "✅ Migrated" if row['migrated'] else "❌ Failed"
        status_class = "success" if row['migrated'] else "error"
        error = row.get('error', '') or ''
        html_body += f"""
            <tr>
                <td>{row['legacy_id']}</td>
                <td>{row['legacy_name']}</td>
                <td>{row.get('legacy_path', 'N/A')}</td>
                <td>{row.get('lakeview_id', 'N/A')}</td>
                <td class="{status_class}">{status}</td>
                <td>{error[:100] if error else ''}</td>
            </tr>
        """
    
    html_body += """
        </table>
        <p><em>Full details available in log file: """ + args.log_file + """</em></p>
    </body>
    </html>
    """
    
    # Send email via SMTP
    if not args.smtp_username or not args.smtp_password:
        logger.error(
            "SMTP credentials not provided. Set SMTP_USERNAME and SMTP_PASSWORD env vars "
            "or use --smtp-username and --smtp-password."
        )
        return
    
    try:
        send_smtp_email(
            args.email_from,
            args.email_to,
            subject,
            html_body,
            args.smtp_server,
            args.smtp_port,
            args.smtp_username,
            args.smtp_password
        )
        logger.info(f"Email summary sent to {args.email_to}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def send_smtp_email(
    from_addr: str,
    to_addrs: str,
    subject: str,
    html_body: str,
    smtp_server: str,
    smtp_port: int,
    username: str,
    password: str
) -> None:
    """
    Send email via SMTP.
    
    Args:
        from_addr: Sender email address
        to_addrs: Comma-separated recipient email addresses
        subject: Email subject
        html_body: HTML email body
        smtp_server: SMTP server hostname
        smtp_port: SMTP server port
        username: SMTP username
        password: SMTP password
    """
    if not from_addr:
        raise ValueError("Email from address is required")
    
    # Create message
    msg = MIMEMultipart('alternative')
    msg['From'] = from_addr
    msg['To'] = to_addrs
    msg['Subject'] = subject
    msg['Date'] = email.utils.formatdate(localtime=True)
    
    # Create plain text version (extract key info from HTML)
    total_match = re.search(r'<li><strong>Total Dashboards:</strong> (\d+)</li>', html_body)
    migrated_match = re.search(r'<li><strong class="success">Successfully Migrated:</strong> (\d+)</li>', html_body)
    failed_match = re.search(r'<li><strong class="error">Failed Migrations:</strong> (\d+)</li>', html_body)
    
    total = total_match.group(1) if total_match else "N/A"
    migrated = migrated_match.group(1) if migrated_match else "N/A"
    failed = failed_match.group(1) if failed_match else "N/A"
    
    text_body = f"""
Dashboard Migration Summary

Total Dashboards: {total}
Successfully Migrated: {migrated}
Failed Migrations: {failed}

See HTML version for full details.
    """
    
    # Attach both plain text and HTML
    part1 = MIMEText(text_body, 'plain')
    part2 = MIMEText(html_body, 'html')
    msg.attach(part1)
    msg.attach(part2)
    
    # Send email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)


def load_config_file(config_path: str) -> Dict:
    """
    Load configuration from YAML or JSON file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dict: Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file format is invalid
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        if config_path.endswith('.yaml') or config_path.endswith('.yml'):
            if not YAML_AVAILABLE:
                raise ImportError(
                    "YAML support not available. Install PyYAML: pip install pyyaml"
                )
            config = yaml.safe_load(f)
        elif config_path.endswith('.json'):
            config = json.load(f)
        else:
            # Try to auto-detect format
            content = f.read()
            f.seek(0)
            try:
                if YAML_AVAILABLE:
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            except (yaml.YAMLError if YAML_AVAILABLE else ValueError):
                f.seek(0)
                config = json.load(f)
    
    if not isinstance(config, dict):
        raise ValueError(f"Config file must contain a dictionary/object, got {type(config)}")
    
    logger.info(f"Loaded configuration from {config_path}")
    return config


def parse_workspaces_from_config(config: Dict) -> List[Dict]:
    """
    Parse workspace configurations from config file.
    Supports both single workspace (host/token) and multiple workspaces (workspaces list).
    
    Args:
        config: Configuration dictionary from file
        
    Returns:
        List of workspace dictionaries with 'name', 'host', and 'token' keys
    """
    workspaces = []
    
    # Check for multiple workspaces configuration
    if "workspaces" in config and isinstance(config["workspaces"], list):
        for idx, ws in enumerate(config["workspaces"]):
            if isinstance(ws, dict):
                ws_name = ws.get("name") or ws.get("workspace") or f"workspace_{idx + 1}"
                ws_host = ws.get("host")
                ws_token = ws.get("token")
                
                if ws_host and ws_token:
                    workspaces.append({
                        "name": ws_name,
                        "host": ws_host,
                        "token": ws_token
                    })
                else:
                    logger.warning(f"Skipping workspace {ws_name}: missing host or token")
    
    # Fall back to single workspace configuration (backward compatibility)
    elif "host" in config and "token" in config:
        workspaces.append({
            "name": config.get("workspace_name") or config.get("workspace") or "default",
            "host": config["host"],
            "token": config["token"]
        })
    
    return workspaces


def merge_config_with_args(config: Dict, args: argparse.Namespace) -> argparse.Namespace:
    """
    Merge configuration file values with command-line arguments.
    Command-line arguments take precedence over config file values.
    
    Args:
        config: Configuration dictionary from file
        args: Parsed command-line arguments namespace
        
    Returns:
        argparse.Namespace: Merged arguments namespace
    """
    # Map config keys to argument names (handle both snake_case and kebab-case)
    # Priority: CLI args > config file > defaults/env vars
    config_mapping = {
        'delete_legacy': 'delete_legacy',
        'delete-legacy': 'delete_legacy',
        'publish': 'publish',
        'warehouse_id': 'warehouse_id',
        'warehouse-id': 'warehouse_id',
        'host': 'host',
        'token': 'token',
        'log_file': 'log_file',
        'log-file': 'log_file',
        'dry_run': 'dry_run',
        'dry-run': 'dry_run',
        'filter_path': 'filter_path',
        'filter-path': 'filter_path',
        'filter_owner': 'filter_owner',
        'filter-owner': 'filter_owner',
        'filter_name': 'filter_name',
        'filter-name': 'filter_name',
        'sleep_between_calls': 'sleep_between_calls',
        'sleep-between-calls': 'sleep_between_calls',
        'max_retries': 'max_retries',
        'max-retries': 'max_retries',
        'retry_delay': 'retry_delay',
        'retry-delay': 'retry_delay',
        'resume': 'resume',
        'log_level': 'log_level',
        'log-level': 'log_level',
        'dashboard_ids': 'dashboard_ids',
        'dashboard-ids': 'dashboard_ids',
        'dashboard_csv': 'dashboard_csv',
        'dashboard-csv': 'dashboard_csv',
        'send_email': 'send_email',
        'send-email': 'send_email',
        'email_to': 'email_to',
        'email-to': 'email_to',
        'email_from': 'email_from',
        'email-from': 'email_from',
        'smtp_server': 'smtp_server',
        'smtp-server': 'smtp_server',
        'smtp_port': 'smtp_port',
        'smtp-port': 'smtp_port',
        'smtp_username': 'smtp_username',
        'smtp-username': 'smtp_username',
        'smtp_password': 'smtp_password',
        'smtp-password': 'smtp_password',
    }
    
    # Boolean flags that should be checked differently
    boolean_flags = {'delete_legacy', 'publish', 'dry_run', 'resume', 'send_email'}
    
    # Track which args were explicitly set via CLI
    # Check sys.argv to see what was actually provided
    cli_args_provided = set()
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg.startswith('--'):
            arg_name = arg.lstrip('--').replace('-', '_')
            cli_args_provided.add(arg_name)
            # For boolean flags, the presence of the flag means True
            if arg_name in boolean_flags:
                setattr(args, arg_name, True)
    
    # Apply config values only if not explicitly set via CLI
    for config_key, arg_name in config_mapping.items():
        if config_key in config:
            # Skip if CLI argument was explicitly provided
            if arg_name in cli_args_provided:
                continue  # CLI arg was set, skip config
            
            # Apply config value
            config_value = config[config_key]
            if config_value is None:
                continue  # Skip None values from config
            
            # Handle boolean flags
            if arg_name in boolean_flags:
                if isinstance(config_value, bool):
                    setattr(args, arg_name, config_value)
                elif isinstance(config_value, str):
                    setattr(args, arg_name, config_value.lower() in ('true', '1', 'yes', 'on'))
            else:
                setattr(args, arg_name, config_value)
    
    return args


def load_existing_log(log_file: str) -> Dict[str, Dict]:
    """
    Load existing migration log to resume from previous run.
    
    Args:
        log_file: Path to existing log CSV file
        
    Returns:
        Dict: Dictionary mapping "workspace:legacy_id" to log row for successfully migrated dashboards
    """
    if not os.path.exists(log_file):
        return {}
    
    try:
        df = pd.read_csv(log_file)
        # Return only successfully migrated dashboards with lakeview_id
        migrated = df[
            (df['migrated'] == True) & 
            (df['lakeview_id'].notna()) & 
            (df['lakeview_id'] != '')
        ]
        result = {}
        for _, row in migrated.iterrows():
            # Use workspace:legacy_id as key for multi-workspace support
            workspace = str(row.get('workspace', 'default'))
            legacy_id = str(row['legacy_id'])
            key = f"{workspace}:{legacy_id}"
            result[key] = row.to_dict()
        logger.info(f"Loaded {len(result)} previously migrated dashboards from {log_file}")
        return result
    except Exception as e:
        logger.warning(f"Could not load existing log file {log_file}: {e}")
        return {}


# ============================================================================
# Command Line Arguments
# ============================================================================

def parse_arguments() -> argparse.Namespace:
    """Parse and return command line arguments, optionally loading from config file."""
    parser = argparse.ArgumentParser(
        description="Migrate legacy Databricks dashboards to AI/BI (Lakeview)."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to configuration file (YAML or JSON). Config values can be overridden by CLI arguments.",
    )
    parser.add_argument(
        "--delete-legacy",
        action="store_true",
        help="After successful migration, delete legacy dashboards (move to trash).",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Publish all migrated AI/BI dashboards.",
    )
    parser.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="Optional warehouse_id to use when publishing AI/BI dashboards.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.environ.get("DATABRICKS_HOST"),
        help="Databricks workspace URL, e.g. https://abc-123.cloud.databricks.com.",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=os.environ.get("DATABRICKS_TOKEN"),
        help="Databricks PAT (or use DATABRICKS_TOKEN env var).",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="data/dashboard_migration_log.csv",
        help="Path to the CSV log file to write (default: data/dashboard_migration_log.csv).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate migration without making API calls. Useful for testing.",
    )
    parser.add_argument(
        "--filter-path",
        type=str,
        default=None,
        help="Only migrate dashboards matching this path pattern (regex).",
    )
    parser.add_argument(
        "--filter-owner",
        type=str,
        default=None,
        help="Only migrate dashboards owned by users matching this pattern (regex).",
    )
    parser.add_argument(
        "--filter-name",
        type=str,
        default=None,
        help="Only migrate dashboards with names matching this pattern (regex).",
    )
    parser.add_argument(
        "--sleep-between-calls",
        type=float,
        default=DEFAULT_SLEEP_BETWEEN_CALLS,
        help=f"Seconds to sleep between API calls (default: {DEFAULT_SLEEP_BETWEEN_CALLS}).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Maximum retry attempts for failed API calls (default: {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=DEFAULT_RETRY_DELAY,
        help=f"Initial delay between retries in seconds (default: {DEFAULT_RETRY_DELAY}).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing log file, skipping already-migrated dashboards.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: INFO).",
    )
    parser.add_argument(
        "--dashboard-ids",
        type=str,
        default=None,
        help="Comma-separated list of specific dashboard IDs to migrate (e.g., '123,456,789').",
    )
    parser.add_argument(
        "--dashboard-csv",
        type=str,
        default=None,
        help="Path to CSV file containing dashboard information. Expected columns: legacy_id, id, dashboard_id, or dashboardId.",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Send email summary after migration completes.",
    )
    parser.add_argument(
        "--email-to",
        type=str,
        default=None,
        help="Comma-separated list of email recipients.",
    )
    parser.add_argument(
        "--email-from",
        type=str,
        default=os.environ.get("EMAIL_FROM"),
        help="Sender email address (or set EMAIL_FROM env var).",
    )
    parser.add_argument(
        "--smtp-server",
        type=str,
        default=os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
        help="SMTP server hostname (or set SMTP_SERVER env var). Default: smtp.gmail.com",
    )
    parser.add_argument(
        "--smtp-port",
        type=int,
        default=int(os.environ.get("SMTP_PORT", "587")),
        help="SMTP server port (or set SMTP_PORT env var). Default: 587",
    )
    parser.add_argument(
        "--smtp-username",
        type=str,
        default=os.environ.get("SMTP_USERNAME"),
        help="SMTP username (or set SMTP_USERNAME env var).",
    )
    parser.add_argument(
        "--smtp-password",
        type=str,
        default=os.environ.get("SMTP_PASSWORD"),
        help="SMTP password (or set SMTP_PASSWORD env var).",
    )
    
    # Parse arguments first
    args = parser.parse_args()
    
    # Load config file if specified and merge with CLI arguments
    if args.config:
        try:
            config = load_config_file(args.config)
            args = merge_config_with_args(config, args)
        except Exception as e:
            logger.error(f"Failed to load config file {args.config}: {e}")
            raise SystemExit(f"Config file error: {e}")
    
    return args


# ============================================================================
# API Functions
# ============================================================================

def create_session(token: str) -> requests.Session:
    """
    Create and configure a requests session with authentication headers.
    
    Args:
        token: Databricks PAT token
        
    Returns:
        requests.Session: Configured session object
    """
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    return session


def list_all_legacy_dashboards(
    session: requests.Session,
    host: str,
    sleep_between_calls: float = DEFAULT_SLEEP_BETWEEN_CALLS
) -> List[Dict]:
    """
    Return a list of all legacy dashboards from the preview SQL dashboards API.
    Handles pagination until all dashboards are retrieved.
    
    Args:
        session: Authenticated requests session
        host: Databricks workspace URL
        
    Returns:
        list: List of dashboard dictionaries
    """
    url = f"{host}/api/2.0/preview/sql/dashboards"
    page_token = None
    all_dashboards = []
    
    while True:
        params = {"page_size": PAGE_SIZE}
        if page_token:
            params["page_token"] = page_token
            
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        
        all_dashboards.extend(payload.get("results", []))
        page_token = payload.get("next_page_token")
        
        if not page_token:
            break
            
        time.sleep(sleep_between_calls)
    
    return all_dashboards


def list_all_lakeview_dashboards(
    session: requests.Session,
    host: str,
    sleep_between_calls: float = DEFAULT_SLEEP_BETWEEN_CALLS
) -> List[Dict]:
    """
    Return a list of all Lakeview (AI/BI) dashboards from the Lakeview API.
    Handles pagination until all dashboards are retrieved.
    Also checks published status for each dashboard.
    
    Args:
        session: Authenticated requests session
        host: Databricks workspace URL
        
    Returns:
        list: List of dashboard dictionaries with published status
    """
    url = f"{host}/api/2.0/lakeview/dashboards"
    page_token = None
    all_dashboards = []
    
    while True:
        params = {"page_size": PAGE_SIZE}
        if page_token:
            params["page_token"] = page_token
            
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        
        # Lakeview API uses "dashboards" key, not "results"
        dashboards = payload.get("dashboards", []) or payload.get("results", [])
        
        # Fetch full dashboard details and published status for each dashboard
        # The list endpoint only returns limited fields, so we need to fetch details
        for dashboard in dashboards:
            dashboard_id = dashboard.get("id") or dashboard.get("dashboard_id") or dashboard.get("object_id")
            if dashboard_id:
                # Fetch full dashboard details to get all fields (path, update_time, etc.)
                try:
                    detail_url = f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}"
                    detail_resp = session.get(detail_url, timeout=REQUEST_TIMEOUT)
                    if detail_resp.status_code == 200:
                        detail_data = detail_resp.json()
                        # Merge full details into dashboard object
                        dashboard.update(detail_data)
                        # Ensure dashboard_id is set
                        if "dashboard_id" not in dashboard:
                            dashboard["dashboard_id"] = dashboard_id
                    time.sleep(sleep_between_calls)
                except requests.RequestException as e:
                    logger.debug(f"Could not fetch full details for dashboard {dashboard_id}: {e}")
                
                # Try to get published status and published date from the published endpoint
                # This endpoint returns 200 if published, 404 if not published
                try:
                    published_url = f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
                    published_resp = session.get(published_url, timeout=REQUEST_TIMEOUT)
                    if published_resp.status_code == 200:
                        published_data = published_resp.json()
                        dashboard["is_published"] = True
                        # Get published date - use revision_create_time which is when it was published
                        dashboard["published_at"] = (
                            published_data.get("revision_create_time") or
                            published_data.get("published_at") or 
                            published_data.get("publish_time") or
                            published_data.get("created_at") or 
                            published_data.get("updated_at")
                        )
                    else:
                        dashboard["is_published"] = False
                        dashboard["published_at"] = None
                    time.sleep(sleep_between_calls)
                except requests.RequestException as e:
                    # If we can't check (404 means not published, other errors are logged but don't fail)
                    if hasattr(e, 'response') and e.response is not None:
                        if e.response.status_code == 404:
                            dashboard["is_published"] = False
                        else:
                            # Other error - default to False but log it
                            logger.debug(f"Could not check published status for dashboard {dashboard_id}: {e}")
                            dashboard["is_published"] = dashboard.get("published", False)
                    else:
                        dashboard["is_published"] = dashboard.get("published", False)
                    dashboard["published_at"] = None
                    time.sleep(sleep_between_calls)  # Small delay on error
            else:
                dashboard["is_published"] = dashboard.get("published", False)
                dashboard["published_at"] = None
        
        all_dashboards.extend(dashboards)
        page_token = payload.get("next_page_token")
        
        if not page_token:
            break
            
        time.sleep(sleep_between_calls)
    
    return all_dashboards


def migrate_legacy_dashboard(
    session: requests.Session,
    host: str,
    dashboard: Dict,
    dry_run: bool = False
) -> Dict:
    """
    Call the Lakeview migrate API for a single legacy dashboard.
    Uses the dashboard id and name to create the AI/BI dashboard.
    
    Args:
        session: Authenticated requests session
        host: Databricks workspace URL
        dashboard: Legacy dashboard dictionary
        dry_run: If True, simulate without making API call
        
    Returns:
        dict: Response JSON with new AI/BI dashboard information
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would migrate dashboard '{dashboard.get('name')}' ({dashboard.get('id')})")
        return {"id": f"dry-run-{dashboard.get('id')}", "dashboard_id": f"dry-run-{dashboard.get('id')}"}
    
    migrate_url = f"{host}/api/2.0/lakeview/dashboards/migrate"
    body = {
        "source_dashboard_id": dashboard["id"],
        "display_name": dashboard.get("name"),
    }
    resp = session.post(migrate_url, json=body, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def publish_dashboard(
    session: requests.Session,
    host: str,
    dashboard_id: str,
    warehouse_id: Optional[str] = None,
    embed_credentials: bool = True,
    dry_run: bool = False
) -> None:
    """
    Publish an AI/BI dashboard by id.
    Optionally overrides the warehouse and embed_credentials behavior.
    
    Args:
        session: Authenticated requests session
        host: Databricks workspace URL
        dashboard_id: AI/BI dashboard ID to publish
        warehouse_id: Optional warehouse ID to use
        embed_credentials: Whether to embed credentials (default: True)
        dry_run: If True, simulate without making API call
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would publish dashboard {dashboard_id}")
        return
    
    url = f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
    body = {"embed_credentials": embed_credentials}
    if warehouse_id:
        body["warehouse_id"] = warehouse_id
        
    resp = session.post(url, json=body, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()


def delete_legacy_dashboard(
    session: requests.Session,
    host: str,
    dashboard_id: str,
    dry_run: bool = False
) -> None:
    """
    Delete (move to trash) a legacy dashboard by id using the preview SQL API.
    
    Args:
        session: Authenticated requests session
        host: Databricks workspace URL
        dashboard_id: Legacy dashboard ID to delete
        dry_run: If True, simulate without making API call
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would delete legacy dashboard {dashboard_id}")
        return
    
    url = f"{host}/api/2.0/preview/sql/dashboards/{dashboard_id}"
    resp = session.delete(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()


# ============================================================================
# Main Migration Logic
# ============================================================================

def migrate_dashboards(
    session: requests.Session,
    host: str,
    args: argparse.Namespace,
    workspace_name: str = "default"
) -> pd.DataFrame:
    """
    Main migration workflow: list, migrate, optionally publish and delete.
    
    Args:
        session: Authenticated requests session
        host: Databricks workspace URL
        args: Parsed command line arguments
        workspace_name: Name/identifier for the workspace being processed
        
    Returns:
        pandas.DataFrame: Log DataFrame with all migration results
    """
    # Load existing log if resuming
    existing_log = {}
    if args.resume:
        existing_log = load_existing_log(args.log_file)
    
    # Load dashboard selection (IDs or CSV)
    selected_ids = load_dashboard_selection(
        dashboard_ids=args.dashboard_ids,
        dashboard_csv=args.dashboard_csv
    )
    
    # List all legacy dashboards
    logger.info(f"[{workspace_name}] Fetching list of legacy dashboards...")
    legacy_dashboards = list_all_legacy_dashboards(session, host, sleep_between_calls=args.sleep_between_calls)
    logger.info(f"[{workspace_name}] Found {len(legacy_dashboards)} legacy dashboards")
    
    # List all Lakeview dashboards (for information gathering)
    logger.info(f"[{workspace_name}] Fetching list of Lakeview dashboards...")
    lakeview_dashboards = list_all_lakeview_dashboards(session, host, sleep_between_calls=args.sleep_between_calls)
    logger.info(f"[{workspace_name}] Found {len(lakeview_dashboards)} Lakeview dashboards")
    
    # Combine both lists for CSV output
    # Mark legacy dashboards with dashboard_type = "legacy"
    for d in legacy_dashboards:
        d["dashboard_type"] = "legacy"
    
    # Mark Lakeview dashboards with dashboard_type = "lakeview"
    for d in lakeview_dashboards:
        d["dashboard_type"] = "lakeview"
        # Map Lakeview fields to match legacy structure for consistency
        if "id" not in d:
            d["id"] = d.get("dashboard_id") or d.get("object_id")
        if "name" not in d:
            d["name"] = d.get("display_name") or d.get("name")
        # Extract path from parent_path (clean path, not full file path)
        # The API returns path as full file path with .lvdash.json extension
        # We want just the directory path (parent_path)
        if "path" in d and d.get("path"):
            full_path = d.get("path", "")
            # If path contains .lvdash.json, remove the extension and filename, keep directory
            if ".lvdash.json" in full_path:
                # Remove .lvdash.json extension
                path_without_ext = full_path.replace(".lvdash.json", "")
                # Use parent_path if available, otherwise use directory part
                d["path"] = d.get("parent_path", path_without_ext.rsplit("/", 1)[0] if "/" in path_without_ext else path_without_ext)
            else:
                # Use parent_path if available, otherwise use the path as-is
                d["path"] = d.get("parent_path", full_path)
        else:
            # Use parent_path if available
            d["path"] = d.get("parent_path", d.get("path_name", ""))
        # Lakeview dashboards are already migrated (they're already in Lakeview format)
        d["migrated"] = True
    
    # For migration, we only work with legacy dashboards
    dashboards = legacy_dashboards
    
    # Apply filters and selection to legacy dashboards (for migration)
    dashboards = filter_dashboards(
        dashboards,
        path_pattern=args.filter_path,
        owner_pattern=args.filter_owner,
        name_pattern=args.filter_name,
        selected_ids=selected_ids
    )
    
    total = len(dashboards)
    logger.info(f"[{workspace_name}] Found {total} legacy dashboards (after filtering) for migration")
    
    # For CSV output, include all dashboards (legacy + Lakeview)
    all_dashboards_for_csv = legacy_dashboards + lakeview_dashboards
    
    # Only return empty DataFrame if we have no dashboards at all
    if len(all_dashboards_for_csv) == 0:
        logger.info(f"[{workspace_name}] No dashboards found.")
        return pd.DataFrame(columns=[
            "workspace", "legacy_id", "legacy_name", "legacy_path", "legacy_created_at",
            "lakeview_id", "migrated", "migration_datetime", "published",
            "publish_datetime", "deleted_legacy", "error", "dashboard_type",
            "name", "path", "created_date", "published_date",
            "owner", "updated_at", "description"
        ])
    
    if total == 0:
        logger.info(f"[{workspace_name}] No legacy dashboards to migrate, but will include Lakeview dashboards in CSV output.")
    
    # Single in-memory log of all dashboards and actions
    log_rows = []
    skipped_count = 0
    
    # Migrate each legacy dashboard
    for idx, d in enumerate(dashboards, 1):
        legacy_id = str(d.get("id"))
        legacy_name = d.get("name", "Unknown")
        legacy_path = d.get("path") or d.get("dashboard_path")
        legacy_created_at = d.get("created_at") or d.get("create_time")
        
        logger.info(f"[{workspace_name}] [{idx}/{total}] Processing '{legacy_name}' ({legacy_id})...")
        
        # Skip if already migrated and resuming (check by workspace + legacy_id)
        resume_key = f"{workspace_name}:{legacy_id}"
        if args.resume and resume_key in existing_log:
            logger.info(f"[{workspace_name}] Skipping '{legacy_name}' - already migrated (resume mode)")
            existing_row = existing_log[resume_key].copy()
            log_rows.append(existing_row)
            skipped_count += 1
            continue
        
        migrated = False
        published = False
        deleted_legacy = False
        lakeview_id = None
        error_msg = ""
        migration_datetime = None
        publish_datetime = None
        
        try:
            # Record migration timestamp in UTC before calling the API
            migration_datetime = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            
            # Retry logic for migration
            result = None
            for attempt in range(args.max_retries):
                try:
                    result = migrate_legacy_dashboard(session, host, d, dry_run=args.dry_run)
                    break  # Success, exit retry loop
                except requests.RequestException as e:
                    if attempt < args.max_retries - 1:
                        sleep_time = args.retry_delay * (attempt + 1)
                        logger.warning(
                            f"Migration failed (attempt {attempt + 1}/{args.max_retries}): "
                            f"{type(e).__name__}. Retrying in {sleep_time}s..."
                        )
                        time.sleep(sleep_time)
                    else:
                        raise  # Re-raise on final attempt
            
            if result:
                lakeview_id = result.get("id") or result.get("dashboard_id")
                migrated = True
                logger.info(f"[{workspace_name}] Migrated legacy '{legacy_name}' ({legacy_id}) -> AI/BI {lakeview_id}")
        except requests.RequestException as e:
            # Handle both HTTPError and other request exceptions (timeouts, connection errors, etc.)
            error_msg = f"migrate_error: {type(e).__name__}"
            if hasattr(e, 'response') and e.response is not None:
                error_details = extract_error_details(e.response)
                error_msg += f": {error_details}"
            else:
                error_msg += f": {str(e)}"
            migration_datetime = None
            logger.error(f"[{workspace_name}] Failed to migrate '{legacy_name}' ({legacy_id}): {error_msg}")
        
        # Get published status from dashboard data if available
        # Legacy dashboards don't have published status, so it will be False initially
        dashboard_published = d.get("published", False) or d.get("is_published", False)
        
        # Capture additional fields from dashboard data
        owner = d.get("owner") or d.get("user", {}).get("user_name", "") or d.get("user_name", "")
        updated_at = d.get("updated_at") or d.get("update_time") or d.get("modified_at", "")
        description = d.get("description") or d.get("summary", "")
        
        # Ensure dates are in datetime format (ISO 8601)
        created_date = legacy_created_at or ""
        published_date = publish_datetime or ""
        
        # Add initial row for this dashboard to the log
        log_rows.append({
            "workspace": workspace_name,  # Workspace identifier
            "legacy_id": legacy_id,
            "legacy_name": legacy_name,
            "legacy_path": legacy_path,
            "legacy_created_at": legacy_created_at,
            "lakeview_id": lakeview_id,
            "migrated": migrated,  # migrated and converted are the same
            "migration_datetime": migration_datetime,
            "published": dashboard_published,  # Get from dashboard data
            "publish_datetime": publish_datetime,
            "deleted_legacy": deleted_legacy,
            "error": error_msg,
            "dashboard_type": "legacy",
            "name": legacy_name,  # Dashboard name
            "path": legacy_path,  # Dashboard path
            "created_date": created_date,  # Created date (datetime)
            "published_date": published_date,  # Published date (datetime)
            "owner": owner,
            "updated_at": updated_at,
            "description": description,
        })
        
        if not args.dry_run:
            time.sleep(args.sleep_between_calls)
    
    if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} already-migrated dashboards (resume mode)")
    
    # Publish all migrated dashboards when requested
    if args.publish:
        logger.info(f"[{workspace_name}] Publishing migrated dashboards...")
        for row in log_rows:
            legacy_id = row["legacy_id"]
            lakeview_id = row["lakeview_id"]
            if not lakeview_id:
                continue
                
            try:
                # Retry logic for publishing
                for attempt in range(args.max_retries):
                    try:
                        publish_dashboard(
                            session,
                            host,
                            lakeview_id,
                            warehouse_id=args.warehouse_id,
                            dry_run=args.dry_run
                        )
                        break  # Success
                    except requests.RequestException as e:
                        if attempt < args.max_retries - 1:
                            sleep_time = args.retry_delay * (attempt + 1)
                            logger.warning(
                                f"Publish failed (attempt {attempt + 1}/{args.max_retries}): "
                                f"{type(e).__name__}. Retrying in {sleep_time}s..."
                            )
                            time.sleep(sleep_time)
                        else:
                            raise  # Re-raise on final attempt
                
                row["published"] = True
                publish_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                row["publish_datetime"] = publish_time
                row["published_date"] = publish_time  # Update published_date field (datetime)
                logger.info(f"[{workspace_name}] Published AI/BI dashboard {lakeview_id} for legacy {legacy_id}")
            except requests.RequestException as e:
                err = f"publish_error: {type(e).__name__}"
                if hasattr(e, 'response') and e.response is not None:
                    error_details = extract_error_details(e.response)
                    err += f": {error_details}"
                else:
                    err += f": {str(e)}"
                row["error"] = (row["error"] + "; " + err) if row["error"] else err
                logger.error(f"[{workspace_name}] Failed to publish AI/BI {lakeview_id}: {err}")
            
            if not args.dry_run:
                time.sleep(args.sleep_between_calls)
    
    # Delete legacy dashboards when requested and only if they were migrated
    if args.delete_legacy:
        logger.info(f"[{workspace_name}] Deleting legacy dashboards...")
        for row in log_rows:
            legacy_id = row["legacy_id"]
            lakeview_id = row["lakeview_id"]
            if not lakeview_id:
                continue
                
            try:
                # Retry logic for deletion
                for attempt in range(args.max_retries):
                    try:
                        delete_legacy_dashboard(session, host, legacy_id, dry_run=args.dry_run)
                        break  # Success
                    except requests.RequestException as e:
                        if attempt < args.max_retries - 1:
                            sleep_time = args.retry_delay * (attempt + 1)
                            logger.warning(
                                f"Delete failed (attempt {attempt + 1}/{args.max_retries}): "
                                f"{type(e).__name__}. Retrying in {sleep_time}s..."
                            )
                            time.sleep(sleep_time)
                        else:
                            raise  # Re-raise on final attempt
                
                row["deleted_legacy"] = True
                logger.info(f"[{workspace_name}] Deleted legacy dashboard '{row['legacy_name']}' ({legacy_id})")
            except requests.RequestException as e:
                err = f"delete_error: {type(e).__name__}"
                if hasattr(e, 'response') and e.response is not None:
                    error_details = extract_error_details(e.response)
                    err += f": {error_details}"
                else:
                    err += f": {str(e)}"
                row["error"] = (row["error"] + "; " + err) if row["error"] else err
                logger.error(f"[{workspace_name}] Failed to delete legacy '{row['legacy_name']}' ({legacy_id}): {err}")
            
            if not args.dry_run:
                time.sleep(args.sleep_between_calls)
    
    # Add Lakeview dashboards to CSV output (for information only, not for migration)
    logger.info(f"[{workspace_name}] Adding {len(lakeview_dashboards)} Lakeview dashboards to CSV output...")
    for d in lakeview_dashboards:
        lakeview_id = str(d.get("id") or d.get("dashboard_id") or d.get("object_id", ""))
        lakeview_name = d.get("name") or d.get("display_name", "Unknown")
        # Path should already be set from the mapping above
        lakeview_path = d.get("path", "")
        lakeview_created_at = d.get("created_at") or d.get("create_time") or d.get("created_time", "")
        
        # Get published status (already fetched in list_all_lakeview_dashboards)
        is_published = d.get("is_published", False) or d.get("published", False)
        published_at = d.get("published_at") or d.get("publish_datetime", "")
        
        # Capture additional fields from dashboard data (full details should be available now)
        # Owner/creator might not be in API response, try multiple locations
        owner = (
            d.get("owner") or 
            d.get("user", {}).get("user_name", "") if isinstance(d.get("user"), dict) else "" or
            d.get("user_name", "") or 
            d.get("creator", {}).get("user_name", "") if isinstance(d.get("creator"), dict) else "" or
            d.get("created_by", "") or
            ""
        )
        updated_at = d.get("updated_at") or d.get("update_time") or d.get("modified_at") or d.get("last_modified", "")
        description = d.get("description") or d.get("summary", "") or d.get("display_description", "")
        
        # Ensure dates are in datetime format (ISO 8601)
        created_date = lakeview_created_at or ""
        published_date = published_at or ""
        
        log_rows.append({
            "workspace": workspace_name,  # Workspace identifier
            "legacy_id": "",  # Not a legacy dashboard
            "legacy_name": "",
            "legacy_path": "",
            "legacy_created_at": "",
            "lakeview_id": lakeview_id,
            "migrated": True,  # Lakeview dashboards are already migrated (converted)
            "migration_datetime": "",  # No migration date since already in Lakeview
            "published": is_published,  # Get from dashboard data
            "publish_datetime": published_at,
            "deleted_legacy": False,
            "error": "",
            "dashboard_type": "lakeview",
            "name": lakeview_name,  # Dashboard name
            "path": lakeview_path,  # Dashboard path
            "created_date": created_date,  # Created date (datetime)
            "published_date": published_date,  # Published date (datetime)
            "owner": owner,
            "updated_at": updated_at,
            "description": description,
        })
    
    # Build a DataFrame from the log rows
    # Include all columns that might be present
    all_columns = [
        "workspace",  # Workspace identifier
        "legacy_id",
        "legacy_name",
        "legacy_path",
        "legacy_created_at",
        "lakeview_id",
        "migrated",
        "migration_datetime",
        "published",
        "publish_datetime",
        "deleted_legacy",
        "error",
        "dashboard_type",
        "name",  # Dashboard name
        "path",  # Dashboard path
        "created_date",  # Created date (datetime)
        "published_date",  # Published date (datetime)
        "owner",  # Owner/creator
        "updated_at",  # Last updated
        "description",  # Description
    ]
    
    # Only include columns that actually have data
    columns_to_include = [col for col in all_columns if any(col in row for row in log_rows)]
    
    df = pd.DataFrame(log_rows)
    
    # Ensure all expected columns exist (fill missing with empty strings)
    for col in all_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Reorder columns
    df = df[all_columns]
    
    return df


# ============================================================================
# Main Entry Point
# ============================================================================

def main() -> None:
    """
    Entry point: validate configuration, run migration, and save log.
    Supports both single and multiple workspaces.
    """
    args = parse_arguments()
    
    # Set logging level (do this early so config loading messages are visible)
    logger.setLevel(getattr(logging, args.log_level))
    
    # Determine workspaces to process
    workspaces = []
    
    # Check if workspaces are defined in config
    if args.config:
        try:
            config = load_config_file(args.config)
            workspaces = parse_workspaces_from_config(config)
            # Merge other config values
            args = merge_config_with_args(config, args)
        except Exception as e:
            logger.error(f"Failed to load config file {args.config}: {e}")
            raise SystemExit(f"Config file error: {e}")
    
    # If no workspaces from config, use single workspace from CLI/env
    if not workspaces:
        if not args.host or not args.token:
            raise SystemExit(
                "Provide --host and --token, set DATABRICKS_HOST / DATABRICKS_TOKEN, "
                "or configure workspaces in config file."
            )
        workspaces.append({
            "name": "default",
            "host": args.host,
            "token": args.token
        })
    
    # Validate warehouse ID if provided
    try:
        validate_warehouse_id(args.warehouse_id)
    except ValueError as e:
        raise SystemExit(f"Invalid warehouse ID: {e}")
    
    # Warn if dry-run mode
    if args.dry_run:
        logger.warning("DRY RUN MODE: No API calls will be made")
    
    logger.info(f"Processing {len(workspaces)} workspace(s)")
    
    # Process each workspace
    all_dataframes = []
    
    for workspace in workspaces:
        workspace_name = workspace["name"]
        workspace_host = workspace["host"]
        workspace_token = workspace["token"]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing workspace: {workspace_name}")
        logger.info(f"Host: {workspace_host}")
        logger.info(f"{'='*60}")
        
        # Normalize and validate host URL
        try:
            host = normalize_host(workspace_host)
        except ValueError as e:
            logger.error(f"[{workspace_name}] Invalid host URL: {e}")
            continue
        
        # Create authenticated session for this workspace
        session = create_session(workspace_token)
        
        # Run migration for this workspace
        try:
            df = migrate_dashboards(session, host, args, workspace_name=workspace_name)
            if len(df) > 0:
                all_dataframes.append(df)
        except Exception as e:
            logger.error(f"[{workspace_name}] Failed to process workspace: {e}")
            continue
    
    # Combine all dataframes
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
    else:
        # Create empty dataframe with correct columns
        combined_df = pd.DataFrame(columns=[
            "workspace", "legacy_id", "legacy_name", "legacy_path", "legacy_created_at",
            "lakeview_id", "migrated", "migration_datetime", "published",
            "publish_datetime", "deleted_legacy", "error", "dashboard_type",
            "name", "path", "created_date", "published_date",
            "owner", "updated_at", "description"
        ])
    
    # Display summary statistics
    logger.info("\n" + "="*60)
    logger.info("=== Overall Migration Summary ===")
    logger.info("="*60)
    logger.info(f"Total workspaces processed: {len(workspaces)}")
    logger.info(f"Total dashboards found: {len(combined_df)}")
    
    if len(combined_df) > 0:
        logger.info(f"Successfully migrated: {combined_df['migrated'].sum()}")
        logger.info(f"Failed migrations: {(~combined_df['migrated']).sum()}")
        if args.publish and 'published' in combined_df.columns:
            published_count = combined_df['published'].sum()
            logger.info(f"Published: {published_count}")
        if args.delete_legacy and 'deleted_legacy' in combined_df.columns:
            deleted_count = combined_df['deleted_legacy'].sum()
            logger.info(f"Deleted legacy: {deleted_count}")
        
        # Summary by workspace
        if 'workspace' in combined_df.columns:
            logger.info("\n=== Summary by Workspace ===")
            for workspace_name in combined_df['workspace'].unique():
                ws_df = combined_df[combined_df['workspace'] == workspace_name]
                logger.info(f"\n{workspace_name}:")
                logger.info(f"  Total dashboards: {len(ws_df)}")
                logger.info(f"  Migrated: {ws_df['migrated'].sum()}")
                logger.info(f"  Failed: {(~ws_df['migrated']).sum()}")
    
    # Display the first few log rows
    logger.info("\n=== Dashboard migration log (head) ===")
    print(combined_df.head().to_string())
    
    # Persist log to CSV with UTF-8 encoding
    # Ensure data directory exists
    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(args.log_file, index=False, encoding='utf-8')
    logger.info(f"\nWrote log CSV to: {args.log_file}")
    
    # Send email summary if requested
    if args.send_email:
        try:
            # Use first workspace host for email (or combine if multiple)
            email_host = workspaces[0]["host"] if workspaces else args.host
            send_email_summary(combined_df, args, email_host)
        except Exception as e:
            logger.error(f"Failed to send email summary: {e}")


if __name__ == "__main__":
    main()
