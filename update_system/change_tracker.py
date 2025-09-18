"""
Change Tracker Module

Handles database change tracking and audit logging for the tourism database.
Provides utilities for managing update runs, setting run contexts, and querying change history.
"""

import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)


class ChangeTracker:
    """Manages change tracking and audit logging for the tourism database."""

    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize change tracker with database configuration.

        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.connection = None
        self.current_run_id = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.connection.autocommit = False
            logger.info(f"Connected to database: {self.db_config['database']}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def create_update_run(self, source_file_url: str = None, source_file_hash: str = None,
                         source_file_size: int = None) -> str:
        """
        Create a new update run record.

        Args:
            source_file_url: URL of the source TTL file
            source_file_hash: Hash of the source file for integrity checking
            source_file_size: Size of the source file in bytes

        Returns:
            str: UUID of the created update run
        """
        run_id = str(uuid.uuid4())

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO update_runs (
                        run_id, status, source_file_url, source_file_hash, source_file_size
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (run_id, 'RUNNING', source_file_url, source_file_hash, source_file_size))

                self.connection.commit()
                self.current_run_id = run_id
                logger.info(f"Created update run: {run_id}")
                return run_id

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to create update run: {e}")
            raise

    def set_run_context(self, run_id: str) -> None:
        """
        Set the current run ID in the database session for trigger tracking.

        Args:
            run_id: UUID of the update run
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT set_current_run_id(%s)", (run_id,))
                self.current_run_id = run_id
                logger.debug(f"Set run context to: {run_id}")
        except Exception as e:
            logger.error(f"Failed to set run context: {e}")
            raise

    def clear_run_context(self) -> None:
        """Clear the current run ID from the database session."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT clear_current_run_id()")
                self.current_run_id = None
                logger.debug("Cleared run context")
        except Exception as e:
            logger.error(f"Failed to clear run context: {e}")
            raise

    def complete_update_run(self, run_id: str, status: str = 'COMPLETED',
                           records_added: int = 0, records_updated: int = 0,
                           records_deleted: int = 0, error_message: str = None) -> None:
        """
        Mark an update run as completed with final statistics.

        Args:
            run_id: UUID of the update run
            status: Final status ('COMPLETED', 'FAILED', 'CANCELLED')
            records_added: Number of records added during the run
            records_updated: Number of records updated during the run
            records_deleted: Number of records deleted during the run
            error_message: Error message if status is 'FAILED'
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE update_runs
                    SET completed_at = CURRENT_TIMESTAMP,
                        status = %s,
                        records_added = %s,
                        records_updated = %s,
                        records_deleted = %s,
                        error_message = %s
                    WHERE run_id = %s
                """, (status, records_added, records_updated, records_deleted,
                     error_message, run_id))

                self.connection.commit()
                logger.info(f"Update run {run_id} completed with status: {status}")

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to complete update run: {e}")
            raise

    def get_run_status(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status and details of an update run.

        Args:
            run_id: UUID of the update run

        Returns:
            Dict containing run details or None if not found
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM update_runs WHERE run_id = %s
                """, (run_id,))

                result = cursor.fetchone()
                return dict(result) if result else None

        except Exception as e:
            logger.error(f"Failed to get run status: {e}")
            raise

    def get_recent_runs(self, days: int = 30, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent update runs.

        Args:
            days: Number of days to look back
            limit: Maximum number of runs to return

        Returns:
            List of update run details
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM update_runs
                    WHERE started_at >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY started_at DESC
                    LIMIT %s
                """, (days, limit))

                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get recent runs: {e}")
            raise

    def get_entity_changes(self, entity_id: str, table_name: str,
                          limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get change history for a specific entity.

        Args:
            entity_id: UUID of the entity
            table_name: Name of the table ('logies', 'addresses', etc.)
            limit: Maximum number of changes to return

        Returns:
            List of change records
        """
        changelog_table = f"{table_name}_changelog"

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"""
                    SELECT * FROM {changelog_table}
                    WHERE entity_id = %s
                    ORDER BY changed_at DESC
                    LIMIT %s
                """, (entity_id, limit))

                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get entity changes: {e}")
            raise

    def get_changes_by_run(self, run_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all changes for a specific update run.

        Args:
            run_id: UUID of the update run

        Returns:
            Dict with table names as keys and lists of changes as values
        """
        tables = ['logies', 'addresses', 'contact_points', 'geometries', 'identifiers']
        changes = {}

        try:
            for table in tables:
                changelog_table = f"{table}_changelog"

                with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"""
                        SELECT * FROM {changelog_table}
                        WHERE run_id = %s
                        ORDER BY changed_at ASC
                    """, (run_id,))

                    changes[table] = [dict(row) for row in cursor.fetchall()]

            return changes

        except Exception as e:
            logger.error(f"Failed to get changes by run: {e}")
            raise

    def get_change_summary(self, run_id: str = None, days: int = None) -> Dict[str, Any]:
        """
        Get summary statistics for changes.

        Args:
            run_id: Specific run ID to summarize (optional)
            days: Number of days to look back (optional)

        Returns:
            Dict with change count summaries
        """
        tables = ['logies', 'addresses', 'contact_points', 'geometries', 'identifiers']
        summary = {
            'total_changes': 0,
            'by_table': {},
            'by_operation': {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
        }

        try:
            for table in tables:
                changelog_table = f"{table}_changelog"
                where_clause = ""
                params = []

                if run_id:
                    where_clause = "WHERE run_id = %s"
                    params.append(run_id)
                elif days:
                    where_clause = "WHERE changed_at >= CURRENT_DATE - INTERVAL '%s days'"
                    params.append(days)

                with self.connection.cursor() as cursor:
                    # Count by operation type
                    cursor.execute(f"""
                        SELECT operation_type, COUNT(*)
                        FROM {changelog_table}
                        {where_clause}
                        GROUP BY operation_type
                    """, params)

                    table_summary = {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0, 'total': 0}

                    for operation, count in cursor.fetchall():
                        table_summary[operation] = count
                        table_summary['total'] += count
                        summary['by_operation'][operation] += count
                        summary['total_changes'] += count

                    summary['by_table'][table] = table_summary

            return summary

        except Exception as e:
            logger.error(f"Failed to get change summary: {e}")
            raise

    def disable_triggers(self) -> None:
        """Disable all audit triggers for bulk operations."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT disable_audit_triggers()")
                self.connection.commit()
                logger.info("Audit triggers disabled")
        except Exception as e:
            logger.error(f"Failed to disable triggers: {e}")
            raise

    def enable_triggers(self) -> None:
        """Re-enable all audit triggers after bulk operations."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT enable_audit_triggers()")
                self.connection.commit()
                logger.info("Audit triggers enabled")
        except Exception as e:
            logger.error(f"Failed to enable triggers: {e}")
            raise

    def cleanup_old_changes(self, retention_days: int = 365) -> int:
        """
        Clean up old change records beyond retention period.

        Args:
            retention_days: Number of days to retain change records

        Returns:
            Number of records deleted
        """
        tables = ['logies', 'addresses', 'contact_points', 'geometries', 'identifiers']
        total_deleted = 0

        try:
            for table in tables:
                changelog_table = f"{table}_changelog"

                with self.connection.cursor() as cursor:
                    cursor.execute(f"""
                        DELETE FROM {changelog_table}
                        WHERE changed_at < CURRENT_DATE - INTERVAL '%s days'
                    """, (retention_days,))

                    deleted = cursor.rowcount
                    total_deleted += deleted
                    logger.info(f"Deleted {deleted} old records from {changelog_table}")

            self.connection.commit()
            logger.info(f"Cleanup completed. Total records deleted: {total_deleted}")
            return total_deleted

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to cleanup old changes: {e}")
            raise