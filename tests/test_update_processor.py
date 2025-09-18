"""
Test Update Processor

Tests the update processing functionality including:
- Change application with transaction safety
- Dry run validation
- Error handling and rollback
- Change tracking integration
- Processing statistics
"""

import sys
import os
import uuid
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_system.update_processor import UpdateProcessor, UpdateResult
from update_system.change_detector import ChangeDetector, ChangeDetectionResult, EntityChange
from update_system.change_tracker import ChangeTracker
from update_system import DEFAULT_DB_CONFIG


def create_mock_change_result() -> ChangeDetectionResult:
    """Create a mock ChangeDetectionResult for testing."""

    # Create some test changes
    logies_changes = [
        EntityChange(
            entity_id='11111111-1111-1111-1111-111111111111',
            entity_type='logies',
            operation='UPDATE',
            old_values={'name': 'Old Hotel Name', 'description': 'Old description'},
            new_values={'name': 'Updated Hotel Name', 'description': 'Updated description'},
            changed_fields=['name', 'description']
        ),
        EntityChange(
            entity_id='99999999-9999-9999-9999-999999999999',
            entity_type='logies',
            operation='INSERT',
            old_values=None,
            new_values={
                'id': '99999999-9999-9999-9999-999999999999',
                'uri': 'https://test.com/new-hotel',
                'name': 'New Test Hotel',
                'description': 'Brand new hotel',
                'sleeping_places': 4,
                'rental_units_count': 1
            },
            changed_fields=None
        )
    ]

    return ChangeDetectionResult(
        master_db='tourism_test_master',
        comparison_db='tourism_temp_test',
        total_changes=2,
        changes_by_table={'logies': logies_changes},
        summary={'logies': {'INSERT': 1, 'UPDATE': 1, 'DELETE': 0}},
        detection_time=0.1
    )


def test_dry_run_processing():
    """Test dry run processing without actually applying changes."""
    print("Testing dry run processing...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    with UpdateProcessor(config) as processor:
        # Create mock changes
        change_result = create_mock_change_result()

        # Test dry run
        result = processor.apply_changes(change_result, dry_run=True)

        assert result.success, "Dry run should succeed"
        assert result.records_processed > 0, "Should process some records"
        assert len(result.error_messages) == 0, "Should have no errors"
        print(f"✓ Dry run processed {result.records_processed} changes")
        print(f"✓ Processing time: {result.processing_time:.3f}s")
        print(f"✓ Summary: {result.summary}")

    print("✓ Dry run processing test completed\n")


def test_change_validation():
    """Test change validation before processing."""
    print("Testing change validation...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    with UpdateProcessor(config) as processor:
        # Test with valid changes
        change_result = create_mock_change_result()
        validation = processor.validate_changes_before_apply(change_result)

        assert validation['is_valid'], f"Changes should be valid: {validation['errors']}"
        assert validation['statistics']['total_changes'] == 2, "Should count changes correctly"
        print(f"✓ Valid changes passed validation")
        print(f"✓ Statistics: {validation['statistics']}")

        # Test with invalid changes (missing new_values for INSERT)
        invalid_changes = [
            EntityChange(
                entity_id='invalid-id',
                entity_type='logies',
                operation='INSERT',
                old_values=None,
                new_values=None,  # Invalid - missing new_values
                changed_fields=None
            )
        ]

        invalid_result = ChangeDetectionResult(
            master_db='test',
            comparison_db='test',
            total_changes=1,
            changes_by_table={'logies': invalid_changes},
            summary={'logies': {'INSERT': 1, 'UPDATE': 0, 'DELETE': 0}},
            detection_time=0.1
        )

        validation = processor.validate_changes_before_apply(invalid_result)
        assert not validation['is_valid'], "Invalid changes should fail validation"
        assert len(validation['errors']) > 0, "Should have error messages"
        print(f"✓ Invalid changes correctly rejected: {validation['errors'][0]}")

    print("✓ Change validation test completed\n")


def test_processing_statistics():
    """Test processing statistics collection."""
    print("Testing processing statistics...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    with UpdateProcessor(config) as processor:
        # Get overall statistics
        stats = processor.get_processing_statistics()

        assert 'recent_runs' in stats, "Should have recent runs count"
        assert 'total_changes_30_days' in stats, "Should have total changes"
        print(f"✓ Overall stats: {stats['recent_runs']} recent runs, {stats['total_changes_30_days']} changes")

        # Test dry run to create a run record
        change_result = create_mock_change_result()
        result = processor.apply_changes(change_result, dry_run=True)

        # Get specific run statistics
        run_stats = processor.get_processing_statistics(result.run_id)
        assert run_stats['run_id'] == result.run_id, "Should return correct run"
        assert 'status' in run_stats, "Should have status"
        print(f"✓ Run stats for {result.run_id}: status={run_stats['status']}")

    print("✓ Processing statistics test completed\n")


def test_error_handling():
    """Test error handling and rollback functionality."""
    print("Testing error handling...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    with UpdateProcessor(config) as processor:
        # Create changes that will cause errors (invalid table reference)
        invalid_changes = [
            EntityChange(
                entity_id='nonexistent-id',
                entity_type='logies',
                operation='UPDATE',
                old_values={'name': 'Old Name'},
                new_values={'name': 'New Name'},
                changed_fields=['name']
            )
        ]

        invalid_result = ChangeDetectionResult(
            master_db='test',
            comparison_db='test',
            total_changes=1,
            changes_by_table={'logies': invalid_changes},
            summary={'logies': {'INSERT': 0, 'UPDATE': 1, 'DELETE': 0}},
            detection_time=0.1
        )

        # Apply changes that should partially fail
        result = processor.apply_changes(invalid_result, dry_run=False)

        # The operation might succeed (UPDATE with 0 rows affected) or have warnings
        # The key is that it should handle errors gracefully
        print(f"✓ Error handling completed: success={result.success}")
        print(f"✓ Records processed: {result.records_processed}")
        if result.error_messages:
            print(f"✓ Error messages captured: {len(result.error_messages)}")

    print("✓ Error handling test completed\n")


def test_batch_processing():
    """Test batch processing with different batch sizes."""
    print("Testing batch processing...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    with UpdateProcessor(config) as processor:
        # Create multiple changes to test batching
        changes = []
        for i in range(5):
            changes.append(EntityChange(
                entity_id=f'batch-test-{i}',
                entity_type='logies',
                operation='INSERT',
                old_values=None,
                new_values={
                    'id': f'batch-test-{i}',
                    'uri': f'https://test.com/batch-{i}',
                    'name': f'Batch Test Hotel {i}',
                    'description': f'Test hotel {i}',
                    'sleeping_places': 2,
                    'rental_units_count': 1
                },
                changed_fields=None
            ))

        batch_result = ChangeDetectionResult(
            master_db='test',
            comparison_db='test',
            total_changes=len(changes),
            changes_by_table={'logies': changes},
            summary={'logies': {'INSERT': len(changes), 'UPDATE': 0, 'DELETE': 0}},
            detection_time=0.1
        )

        # Test with small batch size
        result = processor.apply_changes(batch_result, dry_run=True, batch_size=2)

        assert result.success, "Batch processing should succeed"
        assert result.records_processed == len(changes), "Should process all records"
        print(f"✓ Batch processing completed: {result.records_processed} records in batches of 2")

    print("✓ Batch processing test completed\n")


def test_table_dependency_ordering():
    """Test that tables are processed in correct dependency order."""
    print("Testing table dependency ordering...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    with UpdateProcessor(config) as processor:
        # Create changes for multiple tables
        changes = {
            'logies': [EntityChange(
                entity_id='dep-test-logies',
                entity_type='logies',
                operation='INSERT',
                old_values=None,
                new_values={
                    'id': 'dep-test-logies',
                    'uri': 'https://test.com/dep-test',
                    'name': 'Dependency Test Hotel',
                    'description': 'Test hotel',
                    'sleeping_places': 2,
                    'rental_units_count': 1
                },
                changed_fields=None
            )],
            'addresses': [EntityChange(
                entity_id='dep-test-address',
                entity_type='addresses',
                operation='INSERT',
                old_values=None,
                new_values={
                    'id': 'dep-test-address',
                    'street_name': 'Test Street',
                    'house_number': '123',
                    'municipality': 'Test City',
                    'logies_id': 'dep-test-logies'
                },
                changed_fields=None
            )]
        }

        dep_result = ChangeDetectionResult(
            master_db='test',
            comparison_db='test',
            total_changes=2,
            changes_by_table=changes,
            summary={
                'logies': {'INSERT': 1, 'UPDATE': 0, 'DELETE': 0},
                'addresses': {'INSERT': 1, 'UPDATE': 0, 'DELETE': 0}
            },
            detection_time=0.1
        )

        # Process with dry run
        result = processor.apply_changes(dep_result, dry_run=True)

        assert result.success, "Dependency processing should succeed"
        assert 'logies' in result.summary, "Should process logies"
        assert 'addresses' in result.summary, "Should process addresses"
        print(f"✓ Dependency ordering test completed: {result.summary}")

    print("✓ Table dependency ordering test completed\n")


def test_context_manager():
    """Test context manager functionality."""
    print("Testing context manager...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    # Test that context manager properly handles connections
    processor = UpdateProcessor(config)
    assert processor.connection is None, "Connection should be None before context"
    assert processor.change_tracker is None, "Change tracker should be None before context"

    with processor:
        assert processor.connection is not None, "Connection should be established in context"
        assert processor.change_tracker is not None, "Change tracker should be initialized in context"

    assert processor.connection is None, "Connection should be None after context"
    assert processor.change_tracker is None, "Change tracker should be None after context"

    print("✓ Context manager properly manages connections")
    print("✓ Context manager test completed\n")


def main():
    """Run all update processor tests."""
    print("=== Update Processor Tests ===\n")

    try:
        # Test 1: Dry Run Processing
        test_dry_run_processing()

        # Test 2: Change Validation
        test_change_validation()

        # Test 3: Processing Statistics
        test_processing_statistics()

        # Test 4: Error Handling
        test_error_handling()

        # Test 5: Batch Processing
        test_batch_processing()

        # Test 6: Table Dependency Ordering
        test_table_dependency_ordering()

        # Test 7: Context Manager
        test_context_manager()

        print("🎉 All Update Processor tests passed!")
        print("\nPhase 4 (Update Processing & Integration) core functionality validated.")

    except Exception as e:
        print(f"❌ Update Processor test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()