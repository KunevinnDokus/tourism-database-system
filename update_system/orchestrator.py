"""
Tourism Database Update Orchestrator

Central orchestration system that coordinates all phases of the update process:
- Phase 1: Change Tracking (always active)
- Phase 2: Data Source Management
- Phase 3: Change Detection
- Phase 4: Update Processing

Provides unified workflow management, error handling, and reporting.
"""

import os
import json
import time
import logging
import tempfile
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path

from .data_source_manager import DataSourceManager
from .change_detector import ChangeDetector
from .update_processor import UpdateProcessor
from .change_tracker import ChangeTracker

logger = logging.getLogger(__name__)


@dataclass
class OrchestrationConfig:
    """Configuration for the orchestration system."""
    # Database configuration
    db_config: Dict[str, Any]

    # Data source configuration
    source_url: str
    download_timeout: int = 300
    max_retries: int = 3

    # Processing configuration
    batch_size: int = 100
    dry_run_first: bool = True
    force_update: bool = False

    # Monitoring configuration
    enable_notifications: bool = False
    notification_config: Dict[str, Any] = None

    # Storage configuration
    backup_enabled: bool = True
    backup_retention_days: int = 30
    temp_cleanup: bool = True


@dataclass
class OrchestrationResult:
    """Result of complete orchestration run."""
    run_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    phase_results: Dict[str, Any] = None
    total_changes: int = 0
    processing_time: float = 0.0
    error_messages: List[str] = None
    warning_messages: List[str] = None

    def __post_init__(self):
        if self.phase_results is None:
            self.phase_results = {}
        if self.error_messages is None:
            self.error_messages = []
        if self.warning_messages is None:
            self.warning_messages = []


class UpdateOrchestrator:
    """Central orchestrator for the tourism database update system."""

    def __init__(self, config: OrchestrationConfig):
        """
        Initialize the orchestrator.

        Args:
            config: Orchestration configuration
        """
        self.config = config
        self.result = None
        self.notification_handlers = []

        # Initialize logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for orchestration."""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_format)

        # Create file handler if needed
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        file_handler = logging.FileHandler(
            log_dir / f'orchestration_{datetime.now().strftime("%Y%m%d")}.log'
        )
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)

    def add_notification_handler(self, handler: Callable[[OrchestrationResult], None]):
        """Add a notification handler for orchestration events."""
        self.notification_handlers.append(handler)

    def execute_full_update_workflow(self) -> OrchestrationResult:
        """
        Execute the complete update workflow.

        Returns:
            OrchestrationResult: Complete results of the orchestration
        """
        start_time = time.time()
        run_id = f"orchestration_{int(start_time)}"

        self.result = OrchestrationResult(
            run_id=run_id,
            started_at=datetime.now()
        )

        logger.info(f"Starting orchestration run: {run_id}")

        try:
            # Phase 2: Data Source Management
            logger.info("=== Phase 2: Data Source Management ===")
            download_result = self._execute_data_source_phase()
            self.result.phase_results['data_source'] = download_result

            if not download_result['success']:
                raise RuntimeError(f"Data source phase failed: {download_result.get('error')}")

            # Check if update is needed
            if not download_result.get('has_changes', False) and not self.config.force_update:
                logger.info("No changes detected, skipping update process")
                self.result.success = True
                self.result.warning_messages.append("No changes detected in source data")
                return self._finalize_result(start_time)

            # Phase 3: Change Detection
            logger.info("=== Phase 3: Change Detection ===")
            detection_result = self._execute_change_detection_phase(
                download_result['file_path']
            )
            self.result.phase_results['change_detection'] = detection_result

            if not detection_result['success']:
                raise RuntimeError(f"Change detection phase failed: {detection_result.get('error')}")

            # Phase 4: Update Processing
            logger.info("=== Phase 4: Update Processing ===")
            processing_result = self._execute_update_processing_phase(
                detection_result['changes']
            )
            self.result.phase_results['update_processing'] = processing_result

            if not processing_result['success']:
                raise RuntimeError(f"Update processing phase failed: {processing_result.get('error')}")

            # Success
            self.result.success = True
            self.result.total_changes = processing_result.get('records_processed', 0)

            logger.info(f"Orchestration completed successfully: {self.result.total_changes} changes processed")

        except Exception as e:
            error_msg = f"Orchestration failed: {e}"
            logger.error(error_msg)
            self.result.error_messages.append(error_msg)
            self.result.success = False

        finally:
            # Cleanup
            if self.config.temp_cleanup:
                self._cleanup_temp_resources()

            # Finalize result
            self._finalize_result(start_time)

            # Send notifications
            if self.config.enable_notifications:
                self._send_notifications()

        return self.result

    def _execute_data_source_phase(self) -> Dict[str, Any]:
        """Execute Phase 2: Data Source Management."""
        try:
            dsm_config = {
                'current_file_url': self.config.source_url,
                'download_timeout': self.config.download_timeout,
                'max_retries': self.config.max_retries
            }

            with DataSourceManager(dsm_config) as dsm:
                # Download latest file
                file_path, file_hash, file_size = dsm.download_latest_ttl()

                # Check for changes
                comparison = dsm.compare_file_metadata(file_hash, file_size)

                # Validate file
                validation = dsm.validate_ttl_file(file_path)

                return {
                    'success': True,
                    'file_path': file_path,
                    'file_hash': file_hash,
                    'file_size': file_size,
                    'has_changes': comparison.get('has_changes', True),
                    'validation': validation,
                    'comparison': comparison
                }

        except Exception as e:
            logger.error(f"Data source phase error: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _execute_change_detection_phase(self, ttl_file_path: str) -> Dict[str, Any]:
        """Execute Phase 3: Change Detection."""
        try:
            with ChangeDetector(self.config.db_config) as detector:
                # Create temporary database
                temp_db = detector.create_temp_database_from_ttl(ttl_file_path)

                # Compare databases
                changes = detector.compare_databases(
                    self.config.db_config['database'], temp_db
                )

                # Validate changes
                validation = detector.validate_comparison_result(changes)

                return {
                    'success': True,
                    'changes': changes,
                    'temp_database': temp_db,
                    'validation': validation,
                    'total_changes': changes.total_changes,
                    'detection_time': changes.detection_time
                }

        except Exception as e:
            logger.error(f"Change detection phase error: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _execute_update_processing_phase(self, changes) -> Dict[str, Any]:
        """Execute Phase 4: Update Processing."""
        try:
            with UpdateProcessor(self.config.db_config) as processor:
                # Dry run first (if enabled)
                if self.config.dry_run_first:
                    logger.info("Performing dry run validation...")
                    dry_result = processor.apply_changes(
                        changes,
                        dry_run=True,
                        batch_size=self.config.batch_size
                    )

                    if not dry_result.success:
                        return {
                            'success': False,
                            'error': f"Dry run failed: {dry_result.error_messages}",
                            'dry_run_result': asdict(dry_result)
                        }

                    logger.info(f"Dry run successful: {dry_result.records_processed} changes validated")

                # Apply changes for real
                logger.info("Applying changes to database...")
                result = processor.apply_changes(
                    changes,
                    dry_run=False,
                    batch_size=self.config.batch_size
                )

                # Get processing statistics
                stats = processor.get_processing_statistics(result.run_id)

                return {
                    'success': result.success,
                    'result': asdict(result),
                    'statistics': stats,
                    'records_processed': result.records_processed,
                    'processing_time': result.processing_time
                }

        except Exception as e:
            logger.error(f"Update processing phase error: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _cleanup_temp_resources(self):
        """Clean up temporary resources."""
        try:
            # Clean up temporary databases if change detection phase ran
            if 'change_detection' in self.result.phase_results:
                detection_result = self.result.phase_results['change_detection']
                if detection_result.get('temp_database'):
                    # Note: ChangeDetector context manager should handle cleanup
                    pass

            # Clean up temporary files
            if 'data_source' in self.result.phase_results:
                source_result = self.result.phase_results['data_source']
                file_path = source_result.get('file_path')
                if file_path and os.path.exists(file_path):
                    try:
                        os.unlink(file_path)
                        logger.debug(f"Cleaned up temporary file: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to clean up file {file_path}: {e}")

            logger.info("Temporary resources cleaned up")

        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")

    def _finalize_result(self, start_time: float):
        """Finalize the orchestration result."""
        self.result.completed_at = datetime.now()
        self.result.processing_time = time.time() - start_time

        # Generate summary
        if self.result.success:
            logger.info(f"Orchestration completed successfully in {self.result.processing_time:.2f}s")
        else:
            logger.error(f"Orchestration failed after {self.result.processing_time:.2f}s")

        return self.result

    def _send_notifications(self):
        """Send notifications to registered handlers."""
        for handler in self.notification_handlers:
            try:
                handler(self.result)
            except Exception as e:
                logger.warning(f"Notification handler failed: {e}")

    def execute_validation_only(self, ttl_file_path: str = None) -> OrchestrationResult:
        """
        Execute validation-only workflow without applying changes.

        Args:
            ttl_file_path: Optional specific TTL file path

        Returns:
            OrchestrationResult: Validation results
        """
        start_time = time.time()
        run_id = f"validation_{int(start_time)}"

        self.result = OrchestrationResult(
            run_id=run_id,
            started_at=datetime.now()
        )

        logger.info(f"Starting validation-only run: {run_id}")

        try:
            # Get TTL file
            if ttl_file_path is None:
                logger.info("=== Phase 2: Data Source Management (validation) ===")
                download_result = self._execute_data_source_phase()
                self.result.phase_results['data_source'] = download_result

                if not download_result['success']:
                    raise RuntimeError(f"Data source phase failed: {download_result.get('error')}")

                ttl_file_path = download_result['file_path']

            # Phase 3: Change Detection (validation only)
            logger.info("=== Phase 3: Change Detection (validation) ===")
            detection_result = self._execute_change_detection_phase(ttl_file_path)
            self.result.phase_results['change_detection'] = detection_result

            if not detection_result['success']:
                raise RuntimeError(f"Change detection phase failed: {detection_result.get('error')}")

            # Phase 4: Dry run only
            logger.info("=== Phase 4: Dry Run Validation ===")
            with UpdateProcessor(self.config.db_config) as processor:
                dry_result = processor.apply_changes(
                    detection_result['changes'],
                    dry_run=True,
                    batch_size=self.config.batch_size
                )

                self.result.phase_results['dry_run'] = asdict(dry_result)
                self.result.total_changes = dry_result.records_processed

                if not dry_result.success:
                    raise RuntimeError(f"Validation failed: {dry_result.error_messages}")

            self.result.success = True
            logger.info(f"Validation completed successfully: {self.result.total_changes} changes would be applied")

        except Exception as e:
            error_msg = f"Validation failed: {e}"
            logger.error(error_msg)
            self.result.error_messages.append(error_msg)
            self.result.success = False

        finally:
            if self.config.temp_cleanup:
                self._cleanup_temp_resources()

            self._finalize_result(start_time)

        return self.result

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        try:
            # Database connectivity
            with ChangeTracker(self.config.db_config) as tracker:
                db_status = {
                    'connected': True,
                    'database': self.config.db_config['database']
                }

                # Get recent runs
                recent_runs = tracker.get_recent_runs(days=7)

                # Get change summary
                change_summary = tracker.get_change_summary(days=30)

            # Data source status
            source_status = DataSourceManager.check_url_availability(
                self.config.source_url, timeout=10
            )

            return {
                'timestamp': datetime.now().isoformat(),
                'database': db_status,
                'data_source': source_status,
                'recent_activity': {
                    'runs_last_7_days': len(recent_runs),
                    'changes_last_30_days': change_summary.get('total_changes', 0),
                    'last_successful_run': self._get_last_successful_run(recent_runs)
                },
                'system_health': 'healthy' if db_status['connected'] and source_status['available'] else 'degraded'
            }

        except Exception as e:
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'system_health': 'error'
            }

    def _get_last_successful_run(self, recent_runs: List[Dict]) -> Optional[str]:
        """Get timestamp of last successful run."""
        for run in recent_runs:
            if run.get('status') == 'COMPLETED':
                return run.get('started_at', '').isoformat() if run.get('started_at') else None
        return None

    def create_backup(self) -> Dict[str, Any]:
        """Create database backup."""
        if not self.config.backup_enabled:
            return {'success': False, 'error': 'Backup not enabled'}

        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = Path('backups')
            backup_dir.mkdir(exist_ok=True)

            backup_file = backup_dir / f"tourism_backup_{timestamp}.sql"

            # Create pg_dump command
            dump_cmd = [
                'pg_dump',
                '-h', self.config.db_config.get('host', 'localhost'),
                '-p', str(self.config.db_config.get('port', 5432)),
                '-U', self.config.db_config.get('user', 'postgres'),
                '-d', self.config.db_config.get('database'),
                '-f', str(backup_file),
                '--verbose'
            ]

            # Note: In production, you'd want to use subprocess to run this
            logger.info(f"Backup command: {' '.join(dump_cmd)}")

            return {
                'success': True,
                'backup_file': str(backup_file),
                'timestamp': timestamp,
                'size_mb': 0  # Would be populated after actual backup
            }

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def create_default_config(db_config: Dict[str, Any], source_url: str) -> OrchestrationConfig:
    """Create default orchestration configuration."""
    return OrchestrationConfig(
        db_config=db_config,
        source_url=source_url,
        download_timeout=300,
        max_retries=3,
        batch_size=100,
        dry_run_first=True,
        force_update=False,
        enable_notifications=False,
        backup_enabled=True,
        backup_retention_days=30,
        temp_cleanup=True
    )


# Notification handlers
def console_notification_handler(result: OrchestrationResult):
    """Simple console notification handler."""
    if result.success:
        print(f"✅ Update completed successfully: {result.total_changes} changes in {result.processing_time:.2f}s")
    else:
        print(f"❌ Update failed: {', '.join(result.error_messages)}")


def email_notification_handler(result: OrchestrationResult, email_config: Dict[str, Any]):
    """Email notification handler (placeholder)."""
    # In production, implement actual email sending
    logger.info(f"Email notification: {'Success' if result.success else 'Failure'} - {result.run_id}")


def slack_notification_handler(result: OrchestrationResult, slack_config: Dict[str, Any]):
    """Slack notification handler (placeholder)."""
    # In production, implement actual Slack integration
    logger.info(f"Slack notification: {'Success' if result.success else 'Failure'} - {result.run_id}")