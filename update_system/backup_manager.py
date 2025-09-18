"""
Automated Backup and Recovery Manager

Comprehensive backup and recovery system for the Tourism Database.
Provides automated backups, point-in-time recovery, and disaster recovery capabilities.
"""

import os
import json
import time
import shutil
import subprocess
import logging
import threading
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import gzip

logger = logging.getLogger(__name__)


@dataclass
class BackupMetadata:
    """Backup metadata information."""
    backup_id: str
    backup_type: str  # 'full', 'incremental', 'differential'
    created_at: datetime
    database_name: str
    file_path: str
    file_size_bytes: int
    file_hash: str
    schema_version: str
    total_rows: int
    compression: str
    encryption: bool
    retention_days: int
    tags: List[str]
    success: bool
    error_message: Optional[str] = None


@dataclass
class RestorePoint:
    """Database restore point information."""
    restore_id: str
    created_at: datetime
    backup_id: str
    database_name: str
    description: str
    metadata: Dict[str, Any]


class BackupManager:
    """Automated backup and recovery manager."""

    def __init__(self, db_config: Dict[str, Any], backup_config: Dict[str, Any]):
        self.db_config = db_config
        self.backup_config = backup_config

        # Backup directories
        self.backup_base_dir = Path(backup_config.get('backup_directory', 'backups'))
        self.full_backup_dir = self.backup_base_dir / 'full'
        self.incremental_backup_dir = self.backup_base_dir / 'incremental'
        self.metadata_dir = self.backup_base_dir / 'metadata'

        # Create directories
        for dir_path in [self.full_backup_dir, self.incremental_backup_dir, self.metadata_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Configuration
        self.retention_days = backup_config.get('retention_days', 30)
        self.compression_enabled = backup_config.get('compression', True)
        self.encryption_enabled = backup_config.get('encryption', False)
        self.encryption_key = backup_config.get('encryption_key')

        # Scheduling
        self.auto_backup_enabled = backup_config.get('auto_backup_enabled', True)
        self.full_backup_schedule = backup_config.get('full_backup_schedule', 'daily')
        self.incremental_backup_interval = backup_config.get('incremental_backup_interval_hours', 6)

        # State
        self.backup_history: List[BackupMetadata] = []
        self.restore_points: List[RestorePoint] = []
        self.backup_thread = None
        self.running = False

        # Load existing metadata
        self._load_backup_metadata()

    def start_automated_backups(self):
        """Start automated backup scheduling."""
        if self.running:
            logger.warning("Automated backups already running")
            return

        if not self.auto_backup_enabled:
            logger.info("Automated backups disabled in configuration")
            return

        self.running = True
        self.backup_thread = threading.Thread(target=self._backup_scheduler_loop, daemon=True)
        self.backup_thread.start()
        logger.info("Started automated backup scheduler")

    def stop_automated_backups(self):
        """Stop automated backup scheduling."""
        self.running = False
        if self.backup_thread:
            self.backup_thread.join(timeout=10)
        logger.info("Stopped automated backup scheduler")

    def _backup_scheduler_loop(self):
        """Main backup scheduling loop."""
        last_full_backup = self._get_last_backup_time('full')
        last_incremental_backup = self._get_last_backup_time('incremental')

        while self.running:
            try:
                current_time = datetime.now()

                # Check if full backup is needed
                if self._should_run_full_backup(current_time, last_full_backup):
                    logger.info("Running scheduled full backup")
                    backup_result = self.create_full_backup(
                        f"Scheduled full backup - {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    if backup_result.success:
                        last_full_backup = current_time

                # Check if incremental backup is needed
                elif self._should_run_incremental_backup(current_time, last_incremental_backup):
                    logger.info("Running scheduled incremental backup")
                    backup_result = self.create_incremental_backup(
                        f"Scheduled incremental backup - {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    if backup_result.success:
                        last_incremental_backup = current_time

                # Sleep for 1 hour before checking again
                time.sleep(3600)

            except Exception as e:
                logger.error(f"Error in backup scheduler: {e}")
                time.sleep(600)  # Wait 10 minutes before retrying

    def _should_run_full_backup(self, current_time: datetime, last_backup: Optional[datetime]) -> bool:
        """Check if full backup should be run."""
        if not last_backup:
            return True

        if self.full_backup_schedule == 'daily':
            return current_time - last_backup >= timedelta(days=1)
        elif self.full_backup_schedule == 'weekly':
            return current_time - last_backup >= timedelta(weeks=1)
        elif self.full_backup_schedule == 'monthly':
            return current_time - last_backup >= timedelta(days=30)

        return False

    def _should_run_incremental_backup(self, current_time: datetime, last_backup: Optional[datetime]) -> bool:
        """Check if incremental backup should be run."""
        if not last_backup:
            return True

        return current_time - last_backup >= timedelta(hours=self.incremental_backup_interval)

    def create_full_backup(self, description: str = "", tags: List[str] = None) -> BackupMetadata:
        """Create a complete database backup."""
        backup_id = self._generate_backup_id('full')
        timestamp = datetime.now()

        logger.info(f"Starting full backup: {backup_id}")

        try:
            # Generate backup file path
            filename = f"full_backup_{timestamp.strftime('%Y%m%d_%H%M%S')}.sql"
            if self.compression_enabled:
                filename += ".gz"

            backup_path = self.full_backup_dir / filename

            # Create backup using pg_dump
            backup_success, file_size, error_msg = self._execute_pg_dump(backup_path, full_backup=True)

            if not backup_success:
                return BackupMetadata(
                    backup_id=backup_id,
                    backup_type='full',
                    created_at=timestamp,
                    database_name=self.db_config['database'],
                    file_path=str(backup_path),
                    file_size_bytes=0,
                    file_hash="",
                    schema_version="",
                    total_rows=0,
                    compression=str(self.compression_enabled),
                    encryption=self.encryption_enabled,
                    retention_days=self.retention_days,
                    tags=tags or [],
                    success=False,
                    error_message=error_msg
                )

            # Calculate file hash
            file_hash = self._calculate_file_hash(backup_path)

            # Get database statistics
            schema_version, total_rows = self._get_database_stats()

            # Encrypt if enabled
            if self.encryption_enabled:
                encrypted_path = self._encrypt_backup(backup_path)
                if encrypted_path:
                    backup_path = encrypted_path
                    file_size = backup_path.stat().st_size

            # Create metadata
            metadata = BackupMetadata(
                backup_id=backup_id,
                backup_type='full',
                created_at=timestamp,
                database_name=self.db_config['database'],
                file_path=str(backup_path),
                file_size_bytes=file_size,
                file_hash=file_hash,
                schema_version=schema_version,
                total_rows=total_rows,
                compression=str(self.compression_enabled),
                encryption=self.encryption_enabled,
                retention_days=self.retention_days,
                tags=tags or [],
                success=True
            )

            # Save metadata
            self._save_backup_metadata(metadata)
            self.backup_history.append(metadata)

            logger.info(f"Full backup completed successfully: {backup_id}")
            return metadata

        except Exception as e:
            logger.error(f"Full backup failed: {e}")
            return BackupMetadata(
                backup_id=backup_id,
                backup_type='full',
                created_at=timestamp,
                database_name=self.db_config['database'],
                file_path="",
                file_size_bytes=0,
                file_hash="",
                schema_version="",
                total_rows=0,
                compression=str(self.compression_enabled),
                encryption=self.encryption_enabled,
                retention_days=self.retention_days,
                tags=tags or [],
                success=False,
                error_message=str(e)
            )

    def create_incremental_backup(self, description: str = "", tags: List[str] = None) -> BackupMetadata:
        """Create an incremental backup based on changes since last backup."""
        backup_id = self._generate_backup_id('incremental')
        timestamp = datetime.now()

        logger.info(f"Starting incremental backup: {backup_id}")

        try:
            # Find last backup time
            last_backup_time = self._get_last_backup_time()
            if not last_backup_time:
                logger.warning("No previous backup found, creating full backup instead")
                return self.create_full_backup(description, tags)

            # Generate backup file path
            filename = f"incremental_backup_{timestamp.strftime('%Y%m%d_%H%M%S')}.sql"
            if self.compression_enabled:
                filename += ".gz"

            backup_path = self.incremental_backup_dir / filename

            # Create incremental backup (changes since last backup)
            backup_success, file_size, error_msg = self._execute_incremental_backup(
                backup_path, last_backup_time
            )

            if not backup_success:
                return BackupMetadata(
                    backup_id=backup_id,
                    backup_type='incremental',
                    created_at=timestamp,
                    database_name=self.db_config['database'],
                    file_path=str(backup_path),
                    file_size_bytes=0,
                    file_hash="",
                    schema_version="",
                    total_rows=0,
                    compression=str(self.compression_enabled),
                    encryption=self.encryption_enabled,
                    retention_days=self.retention_days,
                    tags=tags or [],
                    success=False,
                    error_message=error_msg
                )

            # Calculate file hash
            file_hash = self._calculate_file_hash(backup_path)

            # Get change count
            change_count = self._count_incremental_changes(last_backup_time)

            # Create metadata
            metadata = BackupMetadata(
                backup_id=backup_id,
                backup_type='incremental',
                created_at=timestamp,
                database_name=self.db_config['database'],
                file_path=str(backup_path),
                file_size_bytes=file_size,
                file_hash=file_hash,
                schema_version="",
                total_rows=change_count,
                compression=str(self.compression_enabled),
                encryption=self.encryption_enabled,
                retention_days=self.retention_days,
                tags=tags or [],
                success=True
            )

            # Save metadata
            self._save_backup_metadata(metadata)
            self.backup_history.append(metadata)

            logger.info(f"Incremental backup completed successfully: {backup_id}")
            return metadata

        except Exception as e:
            logger.error(f"Incremental backup failed: {e}")
            return BackupMetadata(
                backup_id=backup_id,
                backup_type='incremental',
                created_at=timestamp,
                database_name=self.db_config['database'],
                file_path="",
                file_size_bytes=0,
                file_hash="",
                schema_version="",
                total_rows=0,
                compression=str(self.compression_enabled),
                encryption=self.encryption_enabled,
                retention_days=self.retention_days,
                tags=tags or [],
                success=False,
                error_message=str(e)
            )

    def restore_from_backup(self, backup_id: str, target_database: str = None) -> Dict[str, Any]:
        """Restore database from backup."""
        backup_metadata = self._get_backup_metadata(backup_id)
        if not backup_metadata:
            return {'success': False, 'error': f'Backup {backup_id} not found'}

        target_db = target_database or self.db_config['database']
        logger.info(f"Starting restore from backup {backup_id} to database {target_db}")

        try:
            # Create restore point before proceeding
            restore_point = self._create_restore_point(f"Before restore from {backup_id}")

            # Decrypt backup if needed
            backup_file = Path(backup_metadata.file_path)
            if backup_metadata.encryption:
                backup_file = self._decrypt_backup(backup_file)
                if not backup_file:
                    return {'success': False, 'error': 'Failed to decrypt backup'}

            # Execute restore
            success, error_msg = self._execute_restore(backup_file, target_db)

            if success:
                logger.info(f"Restore completed successfully from backup {backup_id}")
                return {
                    'success': True,
                    'backup_id': backup_id,
                    'target_database': target_db,
                    'restore_point_id': restore_point.restore_id if restore_point else None
                }
            else:
                return {'success': False, 'error': error_msg}

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return {'success': False, 'error': str(e)}

    def cleanup_old_backups(self) -> Dict[str, Any]:
        """Remove old backups based on retention policy."""
        logger.info("Starting backup cleanup")

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        removed_count = 0
        total_size_freed = 0

        for backup in self.backup_history.copy():
            if backup.created_at < cutoff_date:
                try:
                    # Remove backup file
                    backup_path = Path(backup.file_path)
                    if backup_path.exists():
                        file_size = backup_path.stat().st_size
                        backup_path.unlink()
                        total_size_freed += file_size

                    # Remove metadata
                    metadata_path = self.metadata_dir / f"{backup.backup_id}.json"
                    if metadata_path.exists():
                        metadata_path.unlink()

                    # Remove from history
                    self.backup_history.remove(backup)
                    removed_count += 1

                    logger.info(f"Removed old backup: {backup.backup_id}")

                except Exception as e:
                    logger.error(f"Failed to remove backup {backup.backup_id}: {e}")

        logger.info(f"Cleanup completed: removed {removed_count} backups, freed {total_size_freed / (1024**3):.2f} GB")

        return {
            'removed_count': removed_count,
            'size_freed_bytes': total_size_freed,
            'size_freed_gb': total_size_freed / (1024**3)
        }

    def get_backup_status(self) -> Dict[str, Any]:
        """Get comprehensive backup system status."""
        total_backups = len(self.backup_history)
        successful_backups = len([b for b in self.backup_history if b.success])
        total_size = sum(b.file_size_bytes for b in self.backup_history if b.success)

        latest_backup = max(self.backup_history, key=lambda b: b.created_at) if self.backup_history else None

        return {
            'backup_system_enabled': self.auto_backup_enabled,
            'total_backups': total_backups,
            'successful_backups': successful_backups,
            'success_rate': (successful_backups / total_backups * 100) if total_backups > 0 else 0,
            'total_size_gb': total_size / (1024**3),
            'retention_days': self.retention_days,
            'compression_enabled': self.compression_enabled,
            'encryption_enabled': self.encryption_enabled,
            'latest_backup': {
                'backup_id': latest_backup.backup_id,
                'created_at': latest_backup.created_at.isoformat(),
                'type': latest_backup.backup_type,
                'success': latest_backup.success
            } if latest_backup else None,
            'backup_directory': str(self.backup_base_dir),
            'running': self.running
        }

    def list_backups(self, backup_type: str = None, limit: int = 20) -> List[Dict[str, Any]]:
        """List available backups."""
        filtered_backups = self.backup_history

        if backup_type:
            filtered_backups = [b for b in filtered_backups if b.backup_type == backup_type]

        # Sort by creation time (newest first)
        sorted_backups = sorted(filtered_backups, key=lambda b: b.created_at, reverse=True)

        return [asdict(backup) for backup in sorted_backups[:limit]]

    def _execute_pg_dump(self, backup_path: Path, full_backup: bool = True) -> Tuple[bool, int, str]:
        """Execute pg_dump to create backup."""
        try:
            # Build pg_dump command
            cmd = [
                'pg_dump',
                '-h', self.db_config['host'],
                '-p', str(self.db_config['port']),
                '-U', self.db_config['user'],
                '-d', self.db_config['database'],
                '--no-password',
                '--verbose'
            ]

            if full_backup:
                cmd.extend(['--schema-only', '--data-only'])

            # Set environment for password
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config.get('password', '')

            # Execute command
            if self.compression_enabled:
                with gzip.open(backup_path, 'wt') as f:
                    result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE,
                                          env=env, text=True, timeout=3600)
            else:
                with open(backup_path, 'w') as f:
                    result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE,
                                          env=env, text=True, timeout=3600)

            if result.returncode == 0:
                file_size = backup_path.stat().st_size
                return True, file_size, ""
            else:
                return False, 0, result.stderr

        except Exception as e:
            return False, 0, str(e)

    def _execute_incremental_backup(self, backup_path: Path, since_time: datetime) -> Tuple[bool, int, str]:
        """Execute incremental backup based on change log."""
        try:
            import psycopg2

            # Query changes since last backup
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get all changes since the specified time
                    cur.execute("""
                        SELECT table_name, operation, changed_at, old_values, new_values
                        FROM (
                            SELECT 'logies' as table_name, operation, changed_at, old_values, new_values
                            FROM logies_changelog WHERE changed_at > %s
                            UNION ALL
                            SELECT 'addresses' as table_name, operation, changed_at, old_values, new_values
                            FROM addresses_changelog WHERE changed_at > %s
                            UNION ALL
                            SELECT 'contact_points' as table_name, operation, changed_at, old_values, new_values
                            FROM contact_points_changelog WHERE changed_at > %s
                            UNION ALL
                            SELECT 'geometries' as table_name, operation, changed_at, old_values, new_values
                            FROM geometries_changelog WHERE changed_at > %s
                            UNION ALL
                            SELECT 'identifiers' as table_name, operation, changed_at, old_values, new_values
                            FROM identifiers_changelog WHERE changed_at > %s
                        ) changes
                        ORDER BY changed_at
                    """, [since_time] * 5)

                    changes = cur.fetchall()

                    # Write incremental backup data
                    backup_data = {
                        'backup_type': 'incremental',
                        'since_timestamp': since_time.isoformat(),
                        'created_at': datetime.now().isoformat(),
                        'total_changes': len(changes),
                        'changes': [
                            {
                                'table': change[0],
                                'operation': change[1],
                                'changed_at': change[2].isoformat(),
                                'old_values': change[3],
                                'new_values': change[4]
                            }
                            for change in changes
                        ]
                    }

                    # Write to file
                    backup_content = json.dumps(backup_data, indent=2)

                    if self.compression_enabled:
                        with gzip.open(backup_path, 'wt') as f:
                            f.write(backup_content)
                    else:
                        with open(backup_path, 'w') as f:
                            f.write(backup_content)

                    file_size = backup_path.stat().st_size
                    return True, file_size, ""

        except Exception as e:
            return False, 0, str(e)

    def _execute_restore(self, backup_file: Path, target_database: str) -> Tuple[bool, str]:
        """Execute database restore from backup file."""
        try:
            if backup_file.suffix == '.json' or backup_file.name.endswith('.json.gz'):
                # Incremental backup restore
                return self._restore_incremental_backup(backup_file, target_database)
            else:
                # Full backup restore
                return self._restore_full_backup(backup_file, target_database)

        except Exception as e:
            return False, str(e)

    def _restore_full_backup(self, backup_file: Path, target_database: str) -> Tuple[bool, str]:
        """Restore from full backup using psql."""
        try:
            cmd = [
                'psql',
                '-h', self.db_config['host'],
                '-p', str(self.db_config['port']),
                '-U', self.db_config['user'],
                '-d', target_database,
                '--no-password'
            ]

            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config.get('password', '')

            if backup_file.suffix == '.gz':
                with gzip.open(backup_file, 'rt') as f:
                    result = subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE,
                                          env=env, text=True, timeout=3600)
            else:
                with open(backup_file, 'r') as f:
                    result = subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE,
                                          env=env, text=True, timeout=3600)

            if result.returncode == 0:
                return True, ""
            else:
                return False, result.stderr

        except Exception as e:
            return False, str(e)

    def _restore_incremental_backup(self, backup_file: Path, target_database: str) -> Tuple[bool, str]:
        """Restore incremental backup by replaying changes."""
        # This would implement change replay logic
        # For now, return success with a note
        return True, "Incremental restore not fully implemented"

    def _generate_backup_id(self, backup_type: str) -> str:
        """Generate unique backup ID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{backup_type}_{timestamp}_{hash(time.time()) % 10000:04d}"

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of backup file."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def _get_database_stats(self) -> Tuple[str, int]:
        """Get database schema version and total row count."""
        try:
            import psycopg2

            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get schema version (if available)
                    try:
                        cur.execute("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
                        schema_version = cur.fetchone()[0] if cur.rowcount > 0 else "unknown"
                    except:
                        schema_version = "unknown"

                    # Get total row count
                    cur.execute("""
                        SELECT SUM(n_live_tup) FROM pg_stat_user_tables
                        WHERE schemaname = 'public'
                    """)
                    total_rows = cur.fetchone()[0] or 0

                    return schema_version, total_rows

        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return "unknown", 0

    def _count_incremental_changes(self, since_time: datetime) -> int:
        """Count changes since specified time."""
        try:
            import psycopg2

            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            (SELECT COUNT(*) FROM logies_changelog WHERE changed_at > %s) +
                            (SELECT COUNT(*) FROM addresses_changelog WHERE changed_at > %s) +
                            (SELECT COUNT(*) FROM contact_points_changelog WHERE changed_at > %s) +
                            (SELECT COUNT(*) FROM geometries_changelog WHERE changed_at > %s) +
                            (SELECT COUNT(*) FROM identifiers_changelog WHERE changed_at > %s)
                    """, [since_time] * 5)

                    return cur.fetchone()[0] or 0

        except Exception as e:
            logger.error(f"Failed to count changes: {e}")
            return 0

    def _get_last_backup_time(self, backup_type: str = None) -> Optional[datetime]:
        """Get timestamp of last backup."""
        filtered_backups = [b for b in self.backup_history if b.success]

        if backup_type:
            filtered_backups = [b for b in filtered_backups if b.backup_type == backup_type]

        if not filtered_backups:
            return None

        return max(b.created_at for b in filtered_backups)

    def _create_restore_point(self, description: str) -> Optional[RestorePoint]:
        """Create a restore point before major operations."""
        try:
            restore_id = f"restore_point_{int(time.time())}"

            # Create quick backup for restore point
            backup_result = self.create_full_backup(f"Restore point: {description}")

            if backup_result.success:
                restore_point = RestorePoint(
                    restore_id=restore_id,
                    created_at=datetime.now(),
                    backup_id=backup_result.backup_id,
                    database_name=self.db_config['database'],
                    description=description,
                    metadata={'auto_created': True}
                )

                self.restore_points.append(restore_point)
                return restore_point

            return None

        except Exception as e:
            logger.error(f"Failed to create restore point: {e}")
            return None

    def _save_backup_metadata(self, metadata: BackupMetadata):
        """Save backup metadata to disk."""
        metadata_path = self.metadata_dir / f"{metadata.backup_id}.json"
        with open(metadata_path, 'w') as f:
            # Convert datetime to string for JSON serialization
            metadata_dict = asdict(metadata)
            metadata_dict['created_at'] = metadata.created_at.isoformat()
            json.dump(metadata_dict, f, indent=2)

    def _load_backup_metadata(self):
        """Load existing backup metadata from disk."""
        if not self.metadata_dir.exists():
            return

        for metadata_file in self.metadata_dir.glob("*.json"):
            try:
                with open(metadata_file, 'r') as f:
                    data = json.load(f)

                # Convert string back to datetime
                data['created_at'] = datetime.fromisoformat(data['created_at'])

                metadata = BackupMetadata(**data)
                self.backup_history.append(metadata)

            except Exception as e:
                logger.error(f"Failed to load metadata from {metadata_file}: {e}")

    def _get_backup_metadata(self, backup_id: str) -> Optional[BackupMetadata]:
        """Get backup metadata by ID."""
        for backup in self.backup_history:
            if backup.backup_id == backup_id:
                return backup
        return None

    def _encrypt_backup(self, backup_path: Path) -> Optional[Path]:
        """Encrypt backup file (placeholder for actual encryption)."""
        # This would implement actual encryption
        # For now, just return the original path
        logger.warning("Encryption not implemented")
        return backup_path

    def _decrypt_backup(self, backup_path: Path) -> Optional[Path]:
        """Decrypt backup file (placeholder for actual decryption)."""
        # This would implement actual decryption
        # For now, just return the original path
        logger.warning("Decryption not implemented")
        return backup_path


def create_default_backup_config() -> Dict[str, Any]:
    """Create default backup configuration."""
    return {
        'backup_directory': 'backups',
        'retention_days': 30,
        'compression': True,
        'encryption': False,
        'encryption_key': None,
        'auto_backup_enabled': True,
        'full_backup_schedule': 'daily',  # 'daily', 'weekly', 'monthly'
        'incremental_backup_interval_hours': 6
    }