#!/usr/bin/env python3
"""
Tourism Database Update CLI

Command-line interface for the Tourism Database Update System.
Provides unified access to all update system functionality.

Usage:
    python3 tourism_update_cli.py --help
    python3 tourism_update_cli.py update --config config.json
    python3 tourism_update_cli.py validate --source-url https://example.com/data.ttl
    python3 tourism_update_cli.py status
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from update_system.orchestrator import (
    UpdateOrchestrator,
    OrchestrationConfig,
    create_default_config,
    console_notification_handler
)
from update_system import DEFAULT_DB_CONFIG, TOURISM_DATA_SOURCE


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'tourism_update_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        sys.exit(1)


def create_orchestration_config(args) -> OrchestrationConfig:
    """Create orchestration configuration from arguments."""
    # Base database configuration
    db_config = DEFAULT_DB_CONFIG.copy()

    # Override with command line arguments
    if args.db_host:
        db_config['host'] = args.db_host
    if args.db_port:
        db_config['port'] = int(args.db_port)
    if args.db_name:
        db_config['database'] = args.db_name
    if args.db_user:
        db_config['user'] = args.db_user
    if args.db_password:
        db_config['password'] = args.db_password

    # Source URL
    source_url = args.source_url or TOURISM_DATA_SOURCE['current_file_url']

    # Create configuration
    config = create_default_config(db_config, source_url)

    # Override with additional arguments (only if available)
    if hasattr(args, 'batch_size') and args.batch_size:
        config.batch_size = int(args.batch_size)
    if hasattr(args, 'no_dry_run') and args.no_dry_run:
        config.dry_run_first = False
    if hasattr(args, 'force_update') and args.force_update:
        config.force_update = True
    if hasattr(args, 'no_cleanup') and args.no_cleanup:
        config.temp_cleanup = False

    return config


def cmd_update(args):
    """Execute full update workflow."""
    print("🚀 Starting Tourism Database Update")
    print("=" * 50)

    # Create configuration
    if args.config:
        config_data = load_config(args.config)
        # TODO: Parse config_data into OrchestrationConfig
        config = create_orchestration_config(args)
    else:
        config = create_orchestration_config(args)

    # Create orchestrator
    orchestrator = UpdateOrchestrator(config)

    # Add console notifications
    orchestrator.add_notification_handler(console_notification_handler)

    # Execute update
    result = orchestrator.execute_full_update_workflow()

    # Print results
    print("\n" + "=" * 50)
    print("📊 UPDATE RESULTS")
    print("=" * 50)

    print(f"Run ID: {result.run_id}")
    print(f"Started: {result.started_at}")
    print(f"Completed: {result.completed_at}")
    print(f"Duration: {result.processing_time:.2f} seconds")
    print(f"Success: {'✅' if result.success else '❌'}")
    print(f"Total Changes: {result.total_changes}")

    if result.error_messages:
        print("\n❌ Errors:")
        for error in result.error_messages:
            print(f"  - {error}")

    if result.warning_messages:
        print("\n⚠️ Warnings:")
        for warning in result.warning_messages:
            print(f"  - {warning}")

    # Phase results summary
    if result.phase_results:
        print(f"\n📋 Phase Results:")
        for phase, phase_result in result.phase_results.items():
            status = "✅" if phase_result.get('success', False) else "❌"
            print(f"  {phase}: {status}")

    return 0 if result.success else 1


def cmd_validate(args):
    """Execute validation-only workflow."""
    print("🔍 Starting Tourism Database Validation")
    print("=" * 50)

    # Create configuration
    config = create_orchestration_config(args)

    # Create orchestrator
    orchestrator = UpdateOrchestrator(config)

    # Execute validation
    result = orchestrator.execute_validation_only(args.ttl_file)

    # Print results
    print("\n" + "=" * 50)
    print("📊 VALIDATION RESULTS")
    print("=" * 50)

    print(f"Run ID: {result.run_id}")
    print(f"Duration: {result.processing_time:.2f} seconds")
    print(f"Success: {'✅' if result.success else '❌'}")
    print(f"Changes Detected: {result.total_changes}")

    if result.success:
        print(f"\n✅ Validation passed! {result.total_changes} changes would be applied.")
    else:
        print(f"\n❌ Validation failed!")
        for error in result.error_messages:
            print(f"  - {error}")

    # Show detailed results if available
    if 'change_detection' in result.phase_results:
        detection = result.phase_results['change_detection']
        if 'changes' in detection:
            changes = detection['changes']
            print(f"\n📋 Change Summary:")
            for table, summary in changes.summary.items():
                total_table_changes = sum(summary.values())
                if total_table_changes > 0:
                    print(f"  {table}: {summary} (total: {total_table_changes})")

    return 0 if result.success else 1


def cmd_status(args):
    """Show system status."""
    print("📊 Tourism Database System Status")
    print("=" * 50)

    # Create configuration
    config = create_orchestration_config(args)

    # Create orchestrator
    orchestrator = UpdateOrchestrator(config)

    # Get status
    status = orchestrator.get_system_status()

    # Print status
    print(f"Timestamp: {status.get('timestamp', 'Unknown')}")
    print(f"System Health: {status.get('system_health', 'Unknown').upper()}")

    # Database status
    db_status = status.get('database', {})
    db_connected = "✅" if db_status.get('connected', False) else "❌"
    print(f"\nDatabase: {db_connected} Connected")
    print(f"  Database: {db_status.get('database', 'Unknown')}")

    # Data source status
    source_status = status.get('data_source', {})
    source_available = "✅" if source_status.get('available', False) else "❌"
    print(f"\nData Source: {source_available} Available")
    print(f"  URL: {args.source_url or TOURISM_DATA_SOURCE['current_file_url']}")
    if source_status.get('content_length'):
        size_mb = source_status['content_length'] / (1024 * 1024)
        print(f"  Size: {size_mb:.1f} MB")

    # Recent activity
    activity = status.get('recent_activity', {})
    print(f"\nRecent Activity:")
    print(f"  Runs (7 days): {activity.get('runs_last_7_days', 0)}")
    print(f"  Changes (30 days): {activity.get('changes_last_30_days', 0)}")

    last_run = activity.get('last_successful_run')
    if last_run:
        print(f"  Last Success: {last_run}")
    else:
        print(f"  Last Success: None")

    if 'error' in status:
        print(f"\n❌ Error: {status['error']}")
        return 1

    return 0


def cmd_backup(args):
    """Create database backup."""
    print("💾 Creating Database Backup")
    print("=" * 30)

    # Create configuration
    config = create_orchestration_config(args)

    # Create orchestrator
    orchestrator = UpdateOrchestrator(config)

    # Create backup
    backup_result = orchestrator.create_backup()

    if backup_result['success']:
        print(f"✅ Backup created successfully")
        print(f"File: {backup_result.get('backup_file', 'Unknown')}")
        print(f"Timestamp: {backup_result.get('timestamp', 'Unknown')}")
        return 0
    else:
        print(f"❌ Backup failed: {backup_result.get('error', 'Unknown error')}")
        return 1


def cmd_downloads(args):
    """Manage downloaded TTL files."""
    from update_system.data_source_manager import DataSourceManager

    print("📁 Tourism TTL Downloads Management")
    print("=" * 40)

    if args.downloads_action == 'list':
        # List all downloaded files
        files = DataSourceManager.list_downloaded_files()

        if not files:
            print("No downloaded files found in downloads/ directory")
            return 0

        print(f"Found {len(files)} downloaded file(s):")
        print()

        for i, file_info in enumerate(files, 1):
            print(f"{i}. {file_info['filename']}")
            print(f"   Size: {file_info['size_mb']:.1f} MB")
            print(f"   Downloaded: {file_info['download_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Hash: {file_info['file_hash'][:16]}...")
            print()

    elif args.downloads_action == 'latest':
        # Show latest file info
        latest = DataSourceManager.get_latest_downloaded_file()

        if not latest:
            print("No downloaded files found")
            return 0

        print("Latest downloaded file:")
        print(f"  File: {latest['filename']}")
        print(f"  Size: {latest['size_mb']:.1f} MB")
        print(f"  Downloaded: {latest['download_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Path: {latest['filepath']}")
        print(f"  Hash: {latest['file_hash']}")

    elif args.downloads_action == 'cleanup':
        # Clean up old files
        days_to_keep = getattr(args, 'days_to_keep', 30)
        removed = DataSourceManager.cleanup_old_downloads(days_to_keep)

        print(f"Cleaned up {removed} file(s) older than {days_to_keep} days")

    elif args.downloads_action == 'summary':
        # Show downloads summary
        dsm_config = {'download_timeout': 60, 'max_retries': 3}

        with DataSourceManager(dsm_config) as dsm:
            summary = dsm.get_downloads_summary()

            print(f"Total Files: {summary['total_files']}")
            print(f"Total Size: {summary['total_size_mb']:.1f} MB")

            if summary['latest_download']:
                print(f"Latest Download: {summary['latest_download']}")
            if summary['oldest_download']:
                print(f"Oldest Download: {summary['oldest_download']}")

            if summary.get('files'):
                print("\nRecent Files:")
                for file_info in summary['files']:
                    print(f"  {file_info['filename']} ({file_info['size_mb']:.1f} MB)")

    return 0


def create_sample_config(args):
    """Create a sample configuration file."""
    config = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "database": "tourism_flanders_corrected",
            "user": "postgres",
            "password": ""
        },
        "source": {
            "url": TOURISM_DATA_SOURCE['current_file_url'],
            "download_timeout": 300,
            "max_retries": 3
        },
        "processing": {
            "batch_size": 100,
            "dry_run_first": True,
            "force_update": False
        },
        "monitoring": {
            "enable_notifications": False,
            "notification_config": {}
        },
        "maintenance": {
            "backup_enabled": True,
            "backup_retention_days": 30,
            "temp_cleanup": True
        }
    }

    config_path = args.output or 'tourism_update_config.json'

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"📄 Sample configuration created: {config_path}")
    print("Edit this file to customize your settings")
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Tourism Database Update System CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full update with default settings
  python3 tourism_update_cli.py update

  # Update with custom database
  python3 tourism_update_cli.py update --db-name my_tourism_db --db-user myuser

  # Validate changes without applying
  python3 tourism_update_cli.py validate --source-url https://example.com/data.ttl

  # Check system status
  python3 tourism_update_cli.py status

  # Create backup
  python3 tourism_update_cli.py backup

  # Generate sample config
  python3 tourism_update_cli.py create-config --output my_config.json
        """
    )

    # Global arguments
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    # Database connection arguments
    db_group = parser.add_argument_group('database connection')
    db_group.add_argument('--db-host', help='Database host')
    db_group.add_argument('--db-port', help='Database port')
    db_group.add_argument('--db-name', help='Database name')
    db_group.add_argument('--db-user', help='Database user')
    db_group.add_argument('--db-password', help='Database password')

    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Update command
    update_parser = subparsers.add_parser(
        'update',
        help='Execute full update workflow'
    )
    update_parser.add_argument(
        '--config',
        help='Configuration file path'
    )
    update_parser.add_argument(
        '--source-url',
        help='TTL source URL'
    )
    update_parser.add_argument(
        '--batch-size',
        help='Processing batch size'
    )
    update_parser.add_argument(
        '--no-dry-run',
        action='store_true',
        help='Skip dry run validation'
    )
    update_parser.add_argument(
        '--force-update',
        action='store_true',
        help='Force update even if no changes detected'
    )
    update_parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='Skip temporary file cleanup'
    )

    # Validate command
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate changes without applying'
    )
    validate_parser.add_argument(
        '--source-url',
        help='TTL source URL'
    )
    validate_parser.add_argument(
        '--ttl-file',
        help='Local TTL file path'
    )

    # Status command
    status_parser = subparsers.add_parser(
        'status',
        help='Show system status'
    )
    status_parser.add_argument(
        '--source-url',
        help='TTL source URL to check'
    )

    # Backup command
    backup_parser = subparsers.add_parser(
        'backup',
        help='Create database backup'
    )

    # Create config command
    config_parser = subparsers.add_parser(
        'create-config',
        help='Create sample configuration file'
    )
    config_parser.add_argument(
        '--output',
        help='Output file path (default: tourism_update_config.json)'
    )

    # Downloads command
    downloads_parser = subparsers.add_parser(
        'downloads',
        help='Manage downloaded TTL files'
    )
    downloads_parser.add_argument(
        'downloads_action',
        choices=['list', 'latest', 'cleanup', 'summary'],
        help='Action to perform on downloads'
    )
    downloads_parser.add_argument(
        '--days-to-keep',
        type=int,
        default=30,
        help='Days to keep files for cleanup action (default: 30)'
    )

    # Parse arguments
    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Execute command
    if args.command == 'update':
        return cmd_update(args)
    elif args.command == 'validate':
        return cmd_validate(args)
    elif args.command == 'status':
        return cmd_status(args)
    elif args.command == 'backup':
        return cmd_backup(args)
    elif args.command == 'downloads':
        return cmd_downloads(args)
    elif args.command == 'create-config':
        return create_sample_config(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        logging.exception("Unexpected error in CLI")
        sys.exit(1)