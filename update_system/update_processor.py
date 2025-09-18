"""
Update Processor Module

Applies incremental changes to production database while coordinating with change tracking.
Provides transaction safety, rollback capabilities, and validation of update success.
"""

import uuid
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

from .change_detector import ChangeDetectionResult, EntityChange
from .change_tracker import ChangeTracker

logger = logging.getLogger(__name__)


@dataclass
class UpdateResult:
    """Result of applying updates to database."""
    run_id: str
    success: bool
    records_processed: int
    records_applied: int
    records_failed: int
    processing_time: float
    error_messages: List[str]
    summary: Dict[str, Dict[str, int]]  # table -> {INSERT: count, UPDATE: count, DELETE: count}


class UpdateProcessor:
    """Processes incremental database updates with change tracking and transaction safety."""

    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize update processor.

        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.connection = None
        self.change_tracker = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 5432),
                database=self.db_config.get('database', 'tourism_flanders_corrected'),
                user=self.db_config.get('user', 'postgres'),
                password=self.db_config.get('password', '')
            )
            self.connection.autocommit = False
            logger.info(f"Connected to database: {self.db_config.get('database')}")

            # Initialize change tracker with same config
            tracker_config = self.db_config.copy()
            self.change_tracker = ChangeTracker(tracker_config)
            self.change_tracker.connect()

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connections."""
        if self.change_tracker:
            self.change_tracker.disconnect()
            self.change_tracker = None

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

    def apply_changes(self, change_result: ChangeDetectionResult,
                     dry_run: bool = False,
                     batch_size: int = 100) -> UpdateResult:
        """
        Apply detected changes to the database.

        Args:
            change_result: Result from change detection
            dry_run: If True, validate changes but don't apply them
            batch_size: Number of changes to process in each batch

        Returns:
            UpdateResult: Results of the update operation
        """
        start_time = time.time()

        # Create update run record
        run_id = self.change_tracker.create_update_run(
            source_file_url=f"change_detection_{int(time.time())}"
        )
        self.change_tracker.set_run_context(run_id)

        update_result = UpdateResult(
            run_id=run_id,
            success=False,
            records_processed=0,
            records_applied=0,
            records_failed=0,
            processing_time=0.0,
            error_messages=[],
            summary={}
        )

        try:
            logger.info(f"Starting update processing (run_id: {run_id}, dry_run: {dry_run})")

            # Process changes by table in dependency order
            table_order = ['identifiers', 'geometries', 'contact_points', 'addresses', 'logies']

            for table_name in table_order:
                if table_name not in change_result.changes_by_table:
                    continue

                table_changes = change_result.changes_by_table[table_name]
                if not table_changes:
                    continue

                logger.info(f"Processing {len(table_changes)} changes for table: {table_name}")

                table_summary = self._apply_table_changes(
                    table_name, table_changes, dry_run, batch_size
                )

                update_result.summary[table_name] = table_summary
                update_result.records_processed += len(table_changes)
                update_result.records_applied += sum(table_summary.values())

            # If not dry run, commit the transaction
            if not dry_run:
                self.connection.commit()
                logger.info("Changes committed to database")
            else:
                self.connection.rollback()
                logger.info("Dry run completed - changes rolled back")

            update_result.success = True
            update_result.processing_time = time.time() - start_time

            # Complete the update run
            self.change_tracker.complete_update_run(
                run_id,
                'COMPLETED' if not dry_run else 'DRY_RUN',
                records_added=update_result.summary.get('INSERT', 0),
                records_updated=update_result.summary.get('UPDATE', 0),
                records_deleted=update_result.summary.get('DELETE', 0)
            )

            logger.info(f"Update processing completed successfully in {update_result.processing_time:.2f}s")

        except Exception as e:
            # Rollback transaction on error
            self.connection.rollback()
            error_msg = f"Update processing failed: {e}"
            update_result.error_messages.append(error_msg)
            logger.error(error_msg)

            # Mark run as failed
            self.change_tracker.complete_update_run(
                run_id, 'FAILED', error_message=str(e)
            )

        finally:
            self.change_tracker.clear_run_context()

        return update_result

    def _apply_table_changes(self, table_name: str, changes: List[EntityChange],
                           dry_run: bool, batch_size: int) -> Dict[str, int]:
        """
        Apply changes for a specific table.

        Args:
            table_name: Name of table to update
            changes: List of changes to apply
            dry_run: If True, validate but don't execute
            batch_size: Batch size for processing

        Returns:
            Dict with operation counts
        """
        summary = {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}

        # Group changes by operation type
        operations = {'INSERT': [], 'UPDATE': [], 'DELETE': []}
        for change in changes:
            operations[change.operation].append(change)

        # Process in order: DELETE, UPDATE, INSERT (to avoid constraint conflicts)
        for operation_type in ['DELETE', 'UPDATE', 'INSERT']:
            operation_changes = operations[operation_type]
            if not operation_changes:
                continue

            logger.info(f"  {operation_type}: {len(operation_changes)} changes")

            # Process in batches
            for i in range(0, len(operation_changes), batch_size):
                batch = operation_changes[i:i + batch_size]
                batch_result = self._apply_operation_batch(
                    table_name, operation_type, batch, dry_run
                )
                summary[operation_type] += batch_result

        return summary

    def _apply_operation_batch(self, table_name: str, operation_type: str,
                             changes: List[EntityChange], dry_run: bool) -> int:
        """
        Apply a batch of operations of the same type.

        Args:
            table_name: Table name
            operation_type: 'INSERT', 'UPDATE', or 'DELETE'
            changes: Changes to apply
            dry_run: If True, validate but don't execute

        Returns:
            Number of successful operations
        """
        if not changes:
            return 0

        successful_count = 0

        try:
            with self.connection.cursor() as cursor:
                for change in changes:
                    try:
                        if operation_type == 'INSERT':
                            self._execute_insert(cursor, table_name, change, dry_run)
                        elif operation_type == 'UPDATE':
                            self._execute_update(cursor, table_name, change, dry_run)
                        elif operation_type == 'DELETE':
                            self._execute_delete(cursor, table_name, change, dry_run)

                        successful_count += 1

                    except Exception as e:
                        error_msg = f"Failed to {operation_type} {table_name} {change.entity_id}: {e}"
                        logger.warning(error_msg)
                        # Continue with other changes in batch

        except Exception as e:
            logger.error(f"Batch operation failed for {table_name} {operation_type}: {e}")
            raise

        logger.debug(f"    Batch completed: {successful_count}/{len(changes)} successful")
        return successful_count

    def _execute_insert(self, cursor, table_name: str, change: EntityChange, dry_run: bool) -> None:
        """Execute INSERT operation."""
        if not change.new_values:
            raise ValueError("INSERT operation requires new_values")

        # Build INSERT SQL
        values = change.new_values.copy()

        # Remove timestamp fields - they'll be set automatically
        values.pop('created_at', None)
        values.pop('updated_at', None)

        columns = list(values.keys())
        placeholders = [f'%({col})s' for col in columns]

        sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        if dry_run:
            logger.debug(f"DRY RUN INSERT: {sql} with {values}")
        else:
            cursor.execute(sql, values)
            logger.debug(f"Inserted {table_name}: {change.entity_id}")

    def _execute_update(self, cursor, table_name: str, change: EntityChange, dry_run: bool) -> None:
        """Execute UPDATE operation."""
        if not change.new_values or not change.changed_fields:
            raise ValueError("UPDATE operation requires new_values and changed_fields")

        # Build UPDATE SQL for only changed fields
        update_fields = []
        values = {'entity_id': change.entity_id}

        for field in change.changed_fields:
            if field in ['created_at', 'updated_at', 'id']:
                continue  # Skip system fields

            if field in change.new_values:
                update_fields.append(f"{field} = %({field})s")
                values[field] = change.new_values[field]

        if not update_fields:
            logger.debug(f"No updatable fields for {table_name}: {change.entity_id}")
            return

        sql = f"""
            UPDATE {table_name}
            SET {', '.join(update_fields)}
            WHERE id = %(entity_id)s
        """

        if dry_run:
            logger.debug(f"DRY RUN UPDATE: {sql} with {values}")
        else:
            cursor.execute(sql, values)
            if cursor.rowcount == 0:
                logger.warning(f"UPDATE affected 0 rows for {table_name}: {change.entity_id}")
            else:
                logger.debug(f"Updated {table_name}: {change.entity_id}")

    def _execute_delete(self, cursor, table_name: str, change: EntityChange, dry_run: bool) -> None:
        """Execute DELETE operation."""
        sql = f"DELETE FROM {table_name} WHERE id = %s"

        if dry_run:
            logger.debug(f"DRY RUN DELETE: {sql} with {change.entity_id}")
        else:
            cursor.execute(sql, (change.entity_id,))
            if cursor.rowcount == 0:
                logger.warning(f"DELETE affected 0 rows for {table_name}: {change.entity_id}")
            else:
                logger.debug(f"Deleted {table_name}: {change.entity_id}")

    def validate_changes_before_apply(self, change_result: ChangeDetectionResult) -> Dict[str, Any]:
        """
        Validate changes before applying them.

        Args:
            change_result: Changes to validate

        Returns:
            Dict with validation results
        """
        validation = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'statistics': {
                'total_changes': change_result.total_changes,
                'by_operation': {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0},
                'by_table': {}
            }
        }

        try:
            # Check for potential issues
            for table_name, changes in change_result.changes_by_table.items():
                table_stats = {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}

                for change in changes:
                    table_stats[change.operation] += 1
                    validation['statistics']['by_operation'][change.operation] += 1

                    # Validate individual changes
                    if change.operation == 'INSERT' and not change.new_values:
                        validation['errors'].append(f"INSERT {table_name} {change.entity_id} missing new_values")
                        validation['is_valid'] = False

                    if change.operation == 'UPDATE' and not change.changed_fields:
                        validation['warnings'].append(f"UPDATE {table_name} {change.entity_id} has no changed fields")

                    if change.operation == 'DELETE' and not change.old_values:
                        validation['warnings'].append(f"DELETE {table_name} {change.entity_id} missing old_values")

                validation['statistics']['by_table'][table_name] = table_stats

                # Check for large operations
                total_table_changes = sum(table_stats.values())
                if total_table_changes > 1000:
                    validation['warnings'].append(f"Large operation on {table_name}: {total_table_changes} changes")

            # Check for referential integrity issues
            self._check_referential_integrity(change_result, validation)

        except Exception as e:
            validation['errors'].append(f"Validation error: {e}")
            validation['is_valid'] = False

        return validation

    def _check_referential_integrity(self, change_result: ChangeDetectionResult,
                                   validation: Dict[str, Any]) -> None:
        """Check for potential referential integrity violations."""

        # Check if deleting logies that have dependent records
        logies_deletes = set()
        if 'logies' in change_result.changes_by_table:
            for change in change_result.changes_by_table['logies']:
                if change.operation == 'DELETE':
                    logies_deletes.add(change.entity_id)

        if logies_deletes:
            dependent_tables = ['addresses', 'contact_points', 'geometries']
            for table in dependent_tables:
                if table in change_result.changes_by_table:
                    for change in change_result.changes_by_table[table]:
                        if (change.operation == 'INSERT' and
                            change.new_values and
                            change.new_values.get('logies_id') in logies_deletes):
                            validation['warnings'].append(
                                f"Inserting {table} record with deleted logies_id: {change.new_values.get('logies_id')}"
                            )

    def get_processing_statistics(self, run_id: str = None) -> Dict[str, Any]:
        """
        Get statistics about update processing.

        Args:
            run_id: Specific run ID to analyze (optional)

        Returns:
            Dict with processing statistics
        """
        if not self.change_tracker:
            raise RuntimeError("Change tracker not initialized")

        if run_id:
            # Get specific run statistics
            run_status = self.change_tracker.get_run_status(run_id)
            if not run_status:
                return {}

            changes = self.change_tracker.get_changes_by_run(run_id)

            return {
                'run_id': run_id,
                'status': run_status['status'],
                'started_at': run_status['started_at'],
                'completed_at': run_status['completed_at'],
                'records_added': run_status['records_added'],
                'records_updated': run_status['records_updated'],
                'records_deleted': run_status['records_deleted'],
                'total_changes': (run_status['records_added'] +
                                run_status['records_updated'] +
                                run_status['records_deleted']),
                'change_details': changes
            }
        else:
            # Get summary statistics
            recent_runs = self.change_tracker.get_recent_runs(days=30)
            summary = self.change_tracker.get_change_summary(days=30)

            return {
                'recent_runs': len(recent_runs),
                'total_changes_30_days': summary['total_changes'],
                'successful_runs': len([r for r in recent_runs if r['status'] == 'COMPLETED']),
                'failed_runs': len([r for r in recent_runs if r['status'] == 'FAILED']),
                'summary_by_operation': summary['by_operation'],
                'summary_by_table': summary['by_table']
            }