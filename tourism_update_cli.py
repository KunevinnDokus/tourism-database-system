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
    source_url = getattr(args, 'source_url', None) or TOURISM_DATA_SOURCE['current_file_url']

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


def cmd_monitor(args):
    """Advanced monitoring operations."""
    from update_system.advanced_monitor import AdvancedMonitor, create_default_monitor_config

    print("🔍 Advanced Monitoring System")
    print("=" * 40)

    config = create_orchestration_config(args)
    monitor_config = create_default_monitor_config()
    monitor = AdvancedMonitor(config.db_config, monitor_config)

    if args.monitor_action == 'start':
        print(f"Starting monitoring with {args.interval}s interval...")
        monitor.start_monitoring(args.interval)
        print("✅ Monitoring started (press Ctrl+C to stop)")
        try:
            import time
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            monitor.stop_monitoring()
            print("\n⏹️ Monitoring stopped")

    elif args.monitor_action == 'status':
        health = monitor.get_system_health_summary()
        print(f"Status: {health['status'].upper()}")
        print(f"Message: {health['message']}")
        print(f"Timestamp: {health['timestamp']}")

        if health.get('metrics'):
            metrics = health['metrics']
            print(f"\n📊 Current Metrics:")
            print(f"  CPU: {metrics['cpu_percent']:.1f}%")
            print(f"  Memory: {metrics['memory_percent']:.1f}%")
            print(f"  Disk: {metrics['disk_usage_percent']:.1f}%")
            print(f"  DB Connections: {metrics['database_connections']}")

    elif args.monitor_action == 'alerts':
        alerts = monitor.get_active_alerts()
        if not alerts:
            print("No active alerts")
        else:
            print(f"Active Alerts ({len(alerts)}):")
            for alert in alerts:
                print(f"  🚨 {alert.severity.upper()}: {alert.component}")
                print(f"     {alert.message}")
                print(f"     Time: {alert.timestamp}")

    elif args.monitor_action == 'metrics':
        current_metrics = monitor.get_current_metrics()
        if current_metrics:
            print("Current System Metrics:")
            print(f"  CPU Usage: {current_metrics.cpu_percent:.1f}%")
            print(f"  Memory Usage: {current_metrics.memory_percent:.1f}%")
            print(f"  Memory Available: {current_metrics.memory_available_gb:.1f} GB")
            print(f"  Disk Usage: {current_metrics.disk_usage_percent:.1f}%")
            print(f"  Disk Free: {current_metrics.disk_free_gb:.1f} GB")
            print(f"  DB Connections: {current_metrics.database_connections}")
            print(f"  DB Size: {current_metrics.database_size_gb:.2f} GB")
            print(f"  Active Updates: {current_metrics.active_update_runs}")
            print(f"  Error Rate (24h): {current_metrics.error_rate_24h:.1f}%")
        else:
            print("No metrics available")

    return 0


def cmd_performance(args):
    """Performance optimization operations."""
    from update_system.performance_optimizer import PerformanceOptimizer

    print("⚡ Performance Optimization")
    print("=" * 40)

    config = create_orchestration_config(args)
    optimizer = PerformanceOptimizer(config.db_config)

    if args.perf_action == 'analyze':
        print("Running performance analysis...")
        result = optimizer.optimize_database()

        print(f"\n📊 Analysis Results:")
        print(f"Duration: {result['analysis_duration_ms']:.0f}ms")

        if result['suggestions']:
            print(f"\n💡 Optimization Suggestions:")
            for suggestion in result['suggestions']:
                print(f"  • {suggestion}")
        else:
            print("✅ No optimization suggestions - system performing well")

    elif args.perf_action == 'cache-stats':
        summary = optimizer.get_performance_summary()
        cache_stats = summary['cache']

        print("Cache Statistics:")
        print(f"  Hit Rate: {cache_stats['hit_rate_percent']:.1f}%")
        print(f"  Cache Size: {cache_stats['size']} items")
        print(f"  Memory Usage: {cache_stats['memory_mb']:.1f} MB")
        print(f"  Cache Hits: {cache_stats['hits']:,}")
        print(f"  Cache Misses: {cache_stats['misses']:,}")

    elif args.perf_action == 'clear-cache':
        print("Clearing performance caches...")
        optimizer.clear_caches()
        print("✅ Caches cleared")

    elif args.perf_action == 'optimize':
        print("Starting database optimization...")
        optimizer.start_background_tasks()
        result = optimizer.optimize_database()

        print(f"✅ Optimization completed")
        if result['suggestions']:
            print(f"Found {len(result['suggestions'])} optimization opportunities")

    return 0


def cmd_advanced_backup(args):
    """Advanced backup and recovery operations."""
    from update_system.backup_manager import BackupManager, create_default_backup_config

    print("💾 Advanced Backup System")
    print("=" * 40)

    config = create_orchestration_config(args)
    backup_config = create_default_backup_config()
    backup_manager = BackupManager(config.db_config, backup_config)

    if args.backup_action == 'create':
        backup_type = args.type or 'full'
        print(f"Creating {backup_type} backup...")

        if backup_type == 'full':
            result = backup_manager.create_full_backup(f"Manual {backup_type} backup")
        else:
            result = backup_manager.create_incremental_backup(f"Manual {backup_type} backup")

        if result.success:
            print(f"✅ Backup created successfully")
            print(f"Backup ID: {result.backup_id}")
            print(f"File: {result.file_path}")
            print(f"Size: {result.file_size_bytes / (1024**2):.1f} MB")
        else:
            print(f"❌ Backup failed: {result.error_message}")
            return 1

    elif args.backup_action == 'list':
        backups = backup_manager.list_backups(limit=20)
        if not backups:
            print("No backups found")
        else:
            print(f"Recent Backups ({len(backups)}):")
            for backup in backups:
                status = "✅" if backup['success'] else "❌"
                size_mb = backup['file_size_bytes'] / (1024**2)
                print(f"  {status} {backup['backup_id'][:12]}... ({backup['backup_type']}) - {size_mb:.1f}MB")
                print(f"     Created: {backup['created_at']}")

    elif args.backup_action == 'status':
        status = backup_manager.get_backup_status()
        print(f"Backup System: {'✅ Enabled' if status['backup_system_enabled'] else '❌ Disabled'}")
        print(f"Total Backups: {status['total_backups']}")
        print(f"Success Rate: {status['success_rate']:.1f}%")
        print(f"Total Size: {status['total_size_gb']:.2f} GB")
        print(f"Retention: {status['retention_days']} days")

        if status['latest_backup']:
            latest = status['latest_backup']
            print(f"\nLatest Backup:")
            print(f"  ID: {latest['backup_id']}")
            print(f"  Type: {latest['type']}")
            print(f"  Created: {latest['created_at']}")
            print(f"  Status: {'✅' if latest['success'] else '❌'}")

    elif args.backup_action == 'cleanup':
        print("Cleaning up old backups...")
        result = backup_manager.cleanup_old_backups()
        print(f"✅ Removed {result['removed_count']} backups")
        print(f"Freed {result['size_freed_gb']:.2f} GB")

    elif args.backup_action == 'restore':
        if not args.backup_id:
            print("❌ Backup ID required for restore operation")
            return 1

        print(f"Restoring from backup: {args.backup_id}")
        result = backup_manager.restore_from_backup(args.backup_id)

        if result['success']:
            print(f"✅ Restore completed successfully")
            print(f"Target database: {result['target_database']}")
        else:
            print(f"❌ Restore failed: {result['error']}")
            return 1

    return 0


def cmd_dashboard(args):
    """Start monitoring dashboard."""
    print("🖥️ Starting Tourism Database Dashboard")
    print("=" * 50)
    print(f"Host: {args.host}")
    print(f"Port: {args.port}")
    print("=" * 50)

    try:
        import sys
        import os

        # Add web_dashboard to path
        dashboard_path = os.path.join(os.path.dirname(__file__), 'web_dashboard')
        sys.path.insert(0, dashboard_path)

        from app import app, socketio, initialize_services, emit_real_time_updates

        # Initialize services
        initialize_services()

        # Start real-time updates
        emit_real_time_updates()

        print(f"🌐 Dashboard starting at http://{args.host}:{args.port}")
        print("Press Ctrl+C to stop")

        # Run the dashboard
        socketio.run(app, host=args.host, port=args.port, debug=False)

    except ImportError as e:
        print(f"❌ Dashboard dependencies not available: {e}")
        print("Install dashboard dependencies: pip install flask flask-socketio")
        return 1
    except KeyboardInterrupt:
        print("\n⏹️ Dashboard stopped")
        return 0
    except Exception as e:
        print(f"❌ Dashboard failed to start: {e}")
        return 1


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

  # Advanced monitoring
  python3 tourism_update_cli.py monitor start --interval 30
  python3 tourism_update_cli.py monitor alerts

  # Performance optimization
  python3 tourism_update_cli.py performance analyze
  python3 tourism_update_cli.py performance cache-stats

  # Advanced backup operations
  python3 tourism_update_cli.py backup create --type full
  python3 tourism_update_cli.py backup list
  python3 tourism_update_cli.py backup restore --backup-id <backup-id>

  # Downloads management
  python3 tourism_update_cli.py downloads list
  python3 tourism_update_cli.py downloads cleanup --days-to-keep 30

  # Start monitoring dashboard
  python3 tourism_update_cli.py dashboard --port 5000

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

    # Legacy backup command (redirect to advanced backup)
    legacy_backup_parser = subparsers.add_parser(
        'simple-backup',
        help='Simple database backup (legacy)'
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

    # Monitor command
    monitor_parser = subparsers.add_parser(
        'monitor',
        help='Advanced monitoring and alerting'
    )
    monitor_parser.add_argument(
        'monitor_action',
        choices=['start', 'stop', 'status', 'alerts', 'metrics'],
        help='Monitoring action to perform'
    )
    monitor_parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Monitoring interval in seconds (default: 60)'
    )

    # Performance command
    perf_parser = subparsers.add_parser(
        'performance',
        help='Performance optimization and analysis'
    )
    perf_parser.add_argument(
        'perf_action',
        choices=['analyze', 'optimize', 'cache-stats', 'clear-cache'],
        help='Performance action to perform'
    )

    # Backup command (enhanced)
    backup_parser = subparsers.add_parser(
        'backup',
        help='Advanced backup and recovery operations'
    )
    backup_parser.add_argument(
        'backup_action',
        choices=['create', 'list', 'restore', 'cleanup', 'status'],
        default='create',
        nargs='?',
        help='Backup action (default: create)'
    )
    backup_parser.add_argument(
        '--backup-id',
        help='Backup ID for restore operations'
    )
    backup_parser.add_argument(
        '--type',
        choices=['full', 'incremental'],
        default='full',
        help='Backup type (default: full)'
    )

    # Dashboard command
    dashboard_parser = subparsers.add_parser(
        'dashboard',
        help='Start monitoring dashboard'
    )
    dashboard_parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='Dashboard host (default: 0.0.0.0)'
    )
    dashboard_parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Dashboard port (default: 5000)'
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
        return cmd_advanced_backup(args)
    elif args.command == 'downloads':
        return cmd_downloads(args)
    elif args.command == 'monitor':
        return cmd_monitor(args)
    elif args.command == 'performance':
        return cmd_performance(args)
    elif args.command == 'dashboard':
        return cmd_dashboard(args)
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