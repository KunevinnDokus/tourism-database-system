"""
Unit Tests for Update System Components
======================================

Tests for change detection, update processing, and change tracking.
"""

import unittest
import tempfile
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from update_system.change_detector import ChangeDetector, EntityChange, ChangeDetectionResult
from update_system.update_processor import UpdateProcessor, UpdateResult
from update_system.change_tracker import ChangeTracker
from update_system.data_source_manager import DataSourceManager


class TestChangeDetector(unittest.TestCase):
    """Test change detection functionality."""

    def setUp(self):
        """Set up test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

    @patch('update_system.change_detector.psycopg2.connect')
    def test_change_detector_initialization(self, mock_connect):
        """Test that change detector initializes correctly."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        detector = ChangeDetector(self.db_config)
        self.assertEqual(detector.db_config, self.db_config)
        self.assertIsNone(detector.connection)

    def test_entity_change_creation(self):
        """Test EntityChange dataclass creation."""
        change = EntityChange(
            entity_id='test-123',
            operation='UPDATE',
            table_name='logies',
            old_values={'name': 'Old Hotel'},
            new_values={'name': 'New Hotel'},
            changed_fields=['name']
        )

        self.assertEqual(change.entity_id, 'test-123')
        self.assertEqual(change.operation, 'UPDATE')
        self.assertEqual(change.table_name, 'logies')
        self.assertEqual(change.changed_fields, ['name'])

    def test_change_detection_result_structure(self):
        """Test ChangeDetectionResult structure."""
        changes = [
            EntityChange('id1', 'INSERT', 'logies', {}, {'name': 'Hotel 1'}, []),
            EntityChange('id2', 'UPDATE', 'logies', {'name': 'Old'}, {'name': 'New'}, ['name'])
        ]

        result = ChangeDetectionResult(
            total_changes=2,
            changes_by_table={'logies': changes},
            processing_time=1.5,
            summary={'INSERT': 1, 'UPDATE': 1, 'DELETE': 0}
        )

        self.assertEqual(result.total_changes, 2)
        self.assertIn('logies', result.changes_by_table)
        self.assertEqual(len(result.changes_by_table['logies']), 2)


class TestUpdateProcessor(unittest.TestCase):
    """Test update processor functionality."""

    def setUp(self):
        """Set up test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

    @patch('update_system.update_processor.psycopg2.connect')
    @patch('update_system.update_processor.ChangeTracker')
    def test_update_processor_initialization(self, mock_tracker_class, mock_connect):
        """Test update processor initialization."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_tracker = Mock()
        mock_tracker_class.return_value = mock_tracker

        processor = UpdateProcessor(self.db_config)
        processor.connect()

        self.assertEqual(processor.db_config, self.db_config)
        self.assertIsNotNone(processor.connection)
        self.assertIsNotNone(processor.change_tracker)

    def test_table_processing_order(self):
        """Test that tables are processed in correct dependency order."""
        processor = UpdateProcessor(self.db_config)

        # Mock the _apply_table_changes method to capture order
        call_order = []

        def mock_apply_table_changes(table_name, changes, dry_run, batch_size):
            call_order.append(table_name)
            return {'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}

        with patch.object(processor, '_apply_table_changes', side_effect=mock_apply_table_changes):
            with patch.object(processor, 'connection', Mock()):
                with patch.object(processor, 'change_tracker', Mock()) as mock_tracker:
                    mock_tracker.create_update_run.return_value = 'test-run-id'

                    # Create test changes for multiple tables
                    changes_by_table = {
                        'logies': [EntityChange('id1', 'INSERT', 'logies', {}, {'name': 'Hotel'}, [])],
                        'addresses': [EntityChange('id2', 'INSERT', 'addresses', {}, {'street': 'Main St'}, [])],
                        'identifiers': [EntityChange('id3', 'INSERT', 'identifiers', {}, {'value': '123'}, [])],
                        'tourist_attractions': [EntityChange('id4', 'INSERT', 'tourist_attractions', {}, {'name': 'Museum'}, [])]
                    }

                    change_result = ChangeDetectionResult(
                        total_changes=4,
                        changes_by_table=changes_by_table,
                        processing_time=1.0,
                        summary={'INSERT': 4}
                    )

                    processor.apply_changes(change_result, dry_run=True)

                    # Verify tables were processed in dependency order
                    expected_order = ['identifiers', 'addresses', 'logies', 'tourist_attractions']
                    self.assertEqual(call_order, expected_order)


class TestChangeTracker(unittest.TestCase):
    """Test change tracking functionality."""

    def setUp(self):
        """Set up test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

    @patch('update_system.change_tracker.psycopg2.connect')
    def test_change_tracker_initialization(self, mock_connect):
        """Test change tracker initialization."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        tracker = ChangeTracker(self.db_config)
        tracker.connect()

        self.assertEqual(tracker.db_config, self.db_config)
        self.assertIsNotNone(tracker.connection)

    @patch('update_system.change_tracker.psycopg2.connect')
    def test_update_run_creation(self, mock_connect):
        """Test update run creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        tracker = ChangeTracker(self.db_config)
        tracker.connect()

        run_id = tracker.create_update_run(
            source_file_url='test://url',
            source_file_hash='abc123',
            source_file_size=1000
        )

        # Verify run_id is a valid UUID string
        self.assertIsInstance(run_id, str)
        self.assertEqual(len(run_id), 36)  # UUID length

        # Verify database insert was called
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()

    @patch('update_system.change_tracker.psycopg2.connect')
    def test_run_context_management(self, mock_connect):
        """Test run context setting and clearing."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        tracker = ChangeTracker(self.db_config)
        tracker.connect()

        test_run_id = 'test-run-id-123'

        # Test setting context
        tracker.set_run_context(test_run_id)
        self.assertEqual(tracker.current_run_id, test_run_id)

        # Test clearing context
        tracker.clear_run_context()
        self.assertIsNone(tracker.current_run_id)


class TestDataSourceManager(unittest.TestCase):
    """Test data source management functionality."""

    def setUp(self):
        """Set up test environment."""
        self.config = {
            'current_file_url': 'https://example.com/test.ttl',
            'download_timeout': 60,
            'max_retries': 2
        }

    def test_data_source_manager_initialization(self):
        """Test data source manager initialization."""
        manager = DataSourceManager(self.config)

        self.assertEqual(manager.config, self.config)
        self.assertEqual(manager.download_timeout, 60)
        self.assertEqual(manager.max_retries, 2)

    def test_context_manager(self):
        """Test context manager functionality."""
        with patch('tempfile.mkdtemp') as mock_mkdtemp:
            with patch('os.path.exists') as mock_exists:
                with patch('shutil.rmtree') as mock_rmtree:
                    mock_mkdtemp.return_value = '/tmp/test_dir'
                    mock_exists.return_value = True

                    manager = DataSourceManager(self.config)

                    with manager:
                        self.assertIsNotNone(manager.temp_dir)

                    # Verify cleanup was called
                    mock_rmtree.assert_called_once()

    def test_file_hash_calculation(self):
        """Test file hash calculation."""
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content for hashing")
            test_file = f.name

        try:
            hash_value = DataSourceManager.calculate_file_hash(test_file)
            self.assertIsInstance(hash_value, str)
            self.assertEqual(len(hash_value), 64)  # SHA256 hex length

            # Same content should produce same hash
            hash_value2 = DataSourceManager.calculate_file_hash(test_file)
            self.assertEqual(hash_value, hash_value2)

        finally:
            os.unlink(test_file)

    @patch('requests.head')
    def test_url_availability_check(self, mock_head):
        """Test URL availability checking."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            'content-length': '1000',
            'content-type': 'text/turtle',
            'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT'
        }
        mock_head.return_value = mock_response

        result = DataSourceManager.check_url_availability('https://example.com/test.ttl')

        self.assertTrue(result['available'])
        self.assertEqual(result['status_code'], 200)
        self.assertEqual(result['content_length'], 1000)
        self.assertEqual(result['content_type'], 'text/turtle')

    @patch('requests.head')
    def test_url_availability_check_failure(self, mock_head):
        """Test URL availability check with failure."""
        mock_head.side_effect = Exception("Connection timeout")

        result = DataSourceManager.check_url_availability('https://example.com/test.ttl')

        self.assertFalse(result['available'])
        self.assertIsNotNone(result['error_message'])


class TestIntegrationValidation(unittest.TestCase):
    """Test integration between update system components."""

    def setUp(self):
        """Set up test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

    def test_change_validation_before_apply(self):
        """Test change validation before applying updates."""
        processor = UpdateProcessor(self.db_config)

        # Create test changes
        changes = [
            EntityChange('id1', 'INSERT', 'logies', {}, {'name': 'Hotel 1'}, []),
            EntityChange('id2', 'UPDATE', 'logies', {'name': 'Old'}, {'name': 'New'}, ['name']),
            EntityChange('id3', 'DELETE', 'logies', {'name': 'Deleted Hotel'}, {}, [])
        ]

        change_result = ChangeDetectionResult(
            total_changes=3,
            changes_by_table={'logies': changes},
            processing_time=1.0,
            summary={'INSERT': 1, 'UPDATE': 1, 'DELETE': 1}
        )

        validation = processor.validate_changes_before_apply(change_result)

        self.assertTrue(validation['is_valid'])
        self.assertEqual(validation['statistics']['total_changes'], 3)
        self.assertEqual(validation['statistics']['by_operation']['INSERT'], 1)
        self.assertEqual(validation['statistics']['by_operation']['UPDATE'], 1)
        self.assertEqual(validation['statistics']['by_operation']['DELETE'], 1)

    def test_table_inclusion_consistency(self):
        """Test that all components use consistent table definitions."""
        # This test ensures all components use the same table lists
        # to prevent the kind of inconsistency that caused issues

        from update_system.change_detector import CORE_TABLES, RELATIONSHIP_TABLES
        from update_system.update_processor import UpdateProcessor

        # Verify core tables include tourist_attractions
        self.assertIn('tourist_attractions', CORE_TABLES)
        self.assertIn('logies', CORE_TABLES)
        self.assertIn('addresses', CORE_TABLES)

        # Verify relationship tables are comprehensive
        expected_relationship_tables = [
            'logies_addresses', 'logies_contacts', 'logies_geometries',
            'attraction_addresses', 'attraction_contacts', 'attraction_geometries'
        ]

        for table in expected_relationship_tables:
            self.assertIn(table, RELATIONSHIP_TABLES)


if __name__ == '__main__':
    unittest.main()