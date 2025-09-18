"""
Change Detection Engine

Compares tourism databases to identify changes between master database and new TTL data.
Performs entity-level diff operations and generates detailed change reports.
"""

import uuid
import tempfile
import os
import shutil
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Import our existing parser for TTL processing
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from corrected_ttl_to_postgres_ENHANCED import EnhancedTourismDataImporter

logger = logging.getLogger(__name__)


@dataclass
class EntityChange:
    """Represents a change to a single entity."""
    entity_id: str
    entity_type: str  # 'logies', 'addresses', etc.
    operation: str    # 'INSERT', 'UPDATE', 'DELETE'
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    changed_fields: Optional[List[str]] = None


@dataclass
class ChangeDetectionResult:
    """Complete result of change detection between two database states."""
    master_db: str
    comparison_db: str
    total_changes: int
    changes_by_table: Dict[str, List[EntityChange]]
    summary: Dict[str, Dict[str, int]]  # table -> {INSERT: count, UPDATE: count, DELETE: count}
    detection_time: float

    def get_changes_for_table(self, table_name: str) -> List[EntityChange]:
        """Get all changes for a specific table."""
        return self.changes_by_table.get(table_name, [])

    def get_changes_by_operation(self, operation: str) -> List[EntityChange]:
        """Get all changes of a specific operation type."""
        changes = []
        for table_changes in self.changes_by_table.values():
            changes.extend([c for c in table_changes if c.operation == operation])
        return changes


class ChangeDetector:
    """Detects changes between tourism database states."""

    # Define the core tables to compare
    CORE_TABLES = ['logies', 'addresses', 'contact_points', 'geometries', 'identifiers']

    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize change detector.

        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.temp_db_name = None

    def create_temp_database_from_ttl(self, ttl_file_path: str) -> str:
        """
        Create a temporary database from TTL file for comparison.

        Args:
            ttl_file_path: Path to TTL file to import

        Returns:
            str: Name of created temporary database

        Raises:
            Exception: If database creation or import fails
        """
        # Generate unique temp database name
        temp_suffix = str(uuid.uuid4()).replace('-', '')[:8]
        temp_db_name = f"tourism_temp_compare_{temp_suffix}"

        logger.info(f"Creating temporary database: {temp_db_name}")

        try:
            # Create temporary database
            self._create_database(temp_db_name)

            # Install schema
            self._install_schema(temp_db_name)

            # Import TTL data
            self._import_ttl_data(temp_db_name, ttl_file_path)

            self.temp_db_name = temp_db_name
            logger.info(f"Temporary database ready: {temp_db_name}")

            return temp_db_name

        except Exception as e:
            # Clean up on failure
            if temp_db_name:
                self._drop_database(temp_db_name)
            raise RuntimeError(f"Failed to create temporary database: {e}")

    def compare_databases(self, master_db: str, comparison_db: str) -> ChangeDetectionResult:
        """
        Compare two databases and detect all changes.

        Args:
            master_db: Name of master/baseline database
            comparison_db: Name of comparison database (new data)

        Returns:
            ChangeDetectionResult: Complete change detection results
        """
        import time
        start_time = time.time()

        logger.info(f"Starting database comparison: {master_db} vs {comparison_db}")

        changes_by_table = {}
        summary = {}
        total_changes = 0

        for table_name in self.CORE_TABLES:
            logger.info(f"Comparing table: {table_name}")

            table_changes = self._compare_table(master_db, comparison_db, table_name)
            changes_by_table[table_name] = table_changes

            # Calculate summary
            table_summary = {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
            for change in table_changes:
                table_summary[change.operation] += 1
                total_changes += 1

            summary[table_name] = table_summary
            logger.info(f"  {table_name}: {len(table_changes)} changes")

        detection_time = time.time() - start_time
        logger.info(f"Change detection completed in {detection_time:.2f} seconds")
        logger.info(f"Total changes detected: {total_changes}")

        return ChangeDetectionResult(
            master_db=master_db,
            comparison_db=comparison_db,
            total_changes=total_changes,
            changes_by_table=changes_by_table,
            summary=summary,
            detection_time=detection_time
        )

    def _compare_table(self, master_db: str, comparison_db: str, table_name: str) -> List[EntityChange]:
        """
        Compare a specific table between two databases.

        Args:
            master_db: Master database name
            comparison_db: Comparison database name
            table_name: Table to compare

        Returns:
            List[EntityChange]: List of detected changes
        """
        changes = []

        # Get data from both databases
        master_data = self._get_table_data(master_db, table_name)
        comparison_data = self._get_table_data(comparison_db, table_name)

        # Convert to dictionaries keyed by ID for easy comparison
        master_dict = {row['id']: dict(row) for row in master_data}
        comparison_dict = {row['id']: dict(row) for row in comparison_data}

        master_ids = set(master_dict.keys())
        comparison_ids = set(comparison_dict.keys())

        # Find deletions (in master but not in comparison)
        deleted_ids = master_ids - comparison_ids
        for entity_id in deleted_ids:
            changes.append(EntityChange(
                entity_id=entity_id,
                entity_type=table_name,
                operation='DELETE',
                old_values=master_dict[entity_id],
                new_values=None
            ))

        # Find insertions (in comparison but not in master)
        inserted_ids = comparison_ids - master_ids
        for entity_id in inserted_ids:
            changes.append(EntityChange(
                entity_id=entity_id,
                entity_type=table_name,
                operation='INSERT',
                old_values=None,
                new_values=comparison_dict[entity_id]
            ))

        # Find updates (in both, but with differences)
        common_ids = master_ids & comparison_ids
        for entity_id in common_ids:
            master_row = master_dict[entity_id]
            comparison_row = comparison_dict[entity_id]

            # Compare rows and find changed fields
            changed_fields = []
            for field_name in master_row.keys():
                if field_name in ['created_at', 'updated_at']:
                    continue  # Skip timestamp fields

                master_value = master_row.get(field_name)
                comparison_value = comparison_row.get(field_name)

                if master_value != comparison_value:
                    changed_fields.append(field_name)

            if changed_fields:
                changes.append(EntityChange(
                    entity_id=entity_id,
                    entity_type=table_name,
                    operation='UPDATE',
                    old_values=master_row,
                    new_values=comparison_row,
                    changed_fields=changed_fields
                ))

        return changes

    def _get_table_data(self, db_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all data from a table in specified database.

        Args:
            db_name: Database name
            table_name: Table name

        Returns:
            List of row dictionaries
        """
        connection = psycopg2.connect(
            host=self.db_config.get('host', 'localhost'),
            port=self.db_config.get('port', 5432),
            database=db_name,
            user=self.db_config.get('user', 'postgres'),
            password=self.db_config.get('password', '')
        )

        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {table_name}")
                return [dict(row) for row in cursor.fetchall()]
        finally:
            connection.close()

    def _create_database(self, db_name: str) -> None:
        """Create a new database."""
        # Connect to default database to create new one
        config = self.db_config.copy()
        config['database'] = 'postgres'  # Default admin database

        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        connection.autocommit = True

        try:
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE "{db_name}"')
        finally:
            connection.close()

    def _drop_database(self, db_name: str) -> None:
        """Drop a database."""
        config = self.db_config.copy()
        config['database'] = 'postgres'

        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        connection.autocommit = True

        try:
            with connection.cursor() as cursor:
                # Terminate existing connections to the database
                cursor.execute(f"""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
                """)

                cursor.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        finally:
            connection.close()

    def _install_schema(self, db_name: str) -> None:
        """Install the tourism database schema in the specified database."""
        connection = psycopg2.connect(
            host=self.db_config.get('host', 'localhost'),
            port=self.db_config.get('port', 5432),
            database=db_name,
            user=self.db_config.get('user', 'postgres'),
            password=self.db_config.get('password', '')
        )

        try:
            # Read and execute schema file
            schema_file = 'corrected_tourism_schema.sql'
            if not os.path.exists(schema_file):
                raise FileNotFoundError(f"Schema file not found: {schema_file}")

            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()

            # Filter out database creation commands and connection changes
            filtered_lines = []
            for line in schema_sql.split('\n'):
                line = line.strip()
                if (line.startswith('CREATE DATABASE') or
                    line.startswith('\\c ') or
                    line.startswith('\\connect')):
                    continue
                filtered_lines.append(line)

            filtered_schema = '\n'.join(filtered_lines)

            with connection.cursor() as cursor:
                cursor.execute(filtered_schema)

            connection.commit()

        finally:
            connection.close()

    def _import_ttl_data(self, db_name: str, ttl_file_path: str) -> None:
        """Import TTL data into the specified database."""
        if not os.path.exists(ttl_file_path):
            raise FileNotFoundError(f"TTL file not found: {ttl_file_path}")

        # Configure importer for temporary database - filter to psycopg2 compatible fields
        temp_config = {
            'host': self.db_config.get('host', 'localhost'),
            'port': self.db_config.get('port', 5432),
            'database': db_name,
            'user': self.db_config.get('user', 'postgres'),
            'password': self.db_config.get('password', '')
        }

        # Use our existing enhanced importer
        importer = EnhancedTourismDataImporter(temp_config)

        try:
            importer.connect_db()
            logger.info(f"Importing TTL data from {ttl_file_path}")

            # Parse and import TTL file
            importer.parse_ttl_file(ttl_file_path)
            importer.apply_entity_relationships()
            importer.save_to_database()

            logger.info("TTL import completed successfully")

        except Exception as e:
            logger.error(f"TTL import failed: {e}")
            raise
        finally:
            importer.disconnect_db()

    def cleanup_temp_database(self) -> None:
        """Clean up temporary database if it exists."""
        if self.temp_db_name:
            try:
                self._drop_database(self.temp_db_name)
                logger.info(f"Cleaned up temporary database: {self.temp_db_name}")
                self.temp_db_name = None
            except Exception as e:
                logger.warning(f"Failed to cleanup temporary database: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup temporary database."""
        self.cleanup_temp_database()

    def validate_comparison_result(self, result: ChangeDetectionResult,
                                 expected_changes: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Validate change detection results against expected outcomes.

        Args:
            result: Change detection result to validate
            expected_changes: Expected changes dict (from test fixtures)

        Returns:
            Dict with validation results
        """
        validation = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary_match': False,
            'detail_match': False
        }

        if not expected_changes:
            validation['warnings'].append("No expected changes provided for validation")
            return validation

        try:
            expected_summary = expected_changes.get('summary', {})

            # Check total changes
            expected_total = expected_summary.get('total_changes', 0)
            if result.total_changes != expected_total:
                validation['errors'].append(
                    f"Total changes mismatch: expected {expected_total}, got {result.total_changes}"
                )
                validation['is_valid'] = False

            # Check per-table summaries
            expected_tables = expected_changes.get('expected_changes', {})
            for table_name, expected_table_changes in expected_tables.items():
                if table_name not in result.summary:
                    validation['errors'].append(f"Missing table in results: {table_name}")
                    validation['is_valid'] = False
                    continue

                actual_summary = result.summary[table_name]

                for operation in ['INSERT', 'UPDATE', 'DELETE']:
                    expected_count = len(expected_table_changes.get(operation.lower() + 's', []))
                    actual_count = actual_summary.get(operation, 0)

                    if expected_count != actual_count:
                        validation['errors'].append(
                            f"{table_name}.{operation}: expected {expected_count}, got {actual_count}"
                        )
                        validation['is_valid'] = False

            validation['summary_match'] = len(validation['errors']) == 0

        except Exception as e:
            validation['errors'].append(f"Validation error: {e}")
            validation['is_valid'] = False

        return validation