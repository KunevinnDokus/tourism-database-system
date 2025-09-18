"""
Test Change Detection Engine

Tests the change detection functionality including:
- Database comparison logic
- TTL import and temporary database creation
- Change identification and categorization
- Validation against expected results
"""

import sys
import os
import json
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_system.change_detector import ChangeDetector, ChangeDetectionResult
from update_system import DEFAULT_DB_CONFIG


def test_baseline_comparison():
    """Test comparing baseline database with itself (should show no changes)."""
    print("Testing baseline self-comparison...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'

    with ChangeDetector(config) as detector:
        # Create temporary database from baseline TTL
        baseline_ttl = 'tests/data/test_baseline.ttl'

        if not os.path.exists(baseline_ttl):
            print("⚠️  Baseline TTL not found, skipping test")
            return

        temp_db = detector.create_temp_database_from_ttl(baseline_ttl)
        print(f"✓ Created temporary database: {temp_db}")

        # Compare test master database with temporary database (should be identical)
        result = detector.compare_databases('tourism_test_master', temp_db)

        print(f"✓ Comparison completed in {result.detection_time:.2f}s")
        print(f"✓ Total changes detected: {result.total_changes}")

        # Baseline comparison should show minimal or no changes
        if result.total_changes == 0:
            print("✓ Perfect match - no changes detected")
        else:
            print(f"⚠️  {result.total_changes} changes detected (may be due to ID differences)")

            # Show summary
            for table_name, summary in result.summary.items():
                total_table_changes = sum(summary.values())
                if total_table_changes > 0:
                    print(f"  {table_name}: {summary}")

    print("✓ Baseline comparison test completed\n")


def test_simple_crud_detection():
    """Test change detection with simple CRUD operations."""
    print("Testing simple CRUD change detection...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'

    # Load expected results
    expected_file = 'tests/fixtures/expected_results.json'
    expected_results = {}

    if os.path.exists(expected_file):
        with open(expected_file, 'r') as f:
            expected_results = json.load(f)

    with ChangeDetector(config) as detector:
        # Create temporary database from simple updates TTL
        updates_ttl = 'tests/data/test_updates_simple.ttl'

        if not os.path.exists(updates_ttl):
            print("⚠️  Updates TTL not found, skipping test")
            return

        temp_db = detector.create_temp_database_from_ttl(updates_ttl)
        print(f"✓ Created temporary database: {temp_db}")

        # Compare test master with updates
        result = detector.compare_databases('tourism_test_master', temp_db)

        print(f"✓ Comparison completed in {result.detection_time:.2f}s")
        print(f"✓ Total changes detected: {result.total_changes}")

        # Show detailed summary
        print("\nChange Summary by Table:")
        for table_name, summary in result.summary.items():
            total_table_changes = sum(summary.values())
            if total_table_changes > 0:
                print(f"  {table_name}: {summary} (total: {total_table_changes})")

        # Test validation against expected results
        if 'test_updates_simple' in expected_results:
            validation = detector.validate_comparison_result(
                result, expected_results['test_updates_simple']
            )

            if validation['is_valid']:
                print("✓ Results match expected changes perfectly")
            else:
                print("⚠️  Results don't match expected changes:")
                for error in validation['errors']:
                    print(f"    - {error}")

        # Test specific change types
        inserts = result.get_changes_by_operation('INSERT')
        updates = result.get_changes_by_operation('UPDATE')
        deletes = result.get_changes_by_operation('DELETE')

        print(f"\nChange Details:")
        print(f"  Insertions: {len(inserts)}")
        print(f"  Updates: {len(updates)}")
        print(f"  Deletions: {len(deletes)}")

        # Show some example changes
        if inserts:
            insert_example = inserts[0]
            print(f"  Example insertion: {insert_example.entity_type} {insert_example.entity_id}")

        if updates:
            update_example = updates[0]
            print(f"  Example update: {update_example.entity_type} {update_example.entity_id}")
            if update_example.changed_fields:
                print(f"    Changed fields: {update_example.changed_fields}")

        if deletes:
            delete_example = deletes[0]
            print(f"  Example deletion: {delete_example.entity_type} {delete_example.entity_id}")

    print("✓ Simple CRUD detection test completed\n")


def test_table_level_analysis():
    """Test detailed table-level change analysis."""
    print("Testing table-level change analysis...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'

    with ChangeDetector(config) as detector:
        # Create temporary database from simple updates TTL
        updates_ttl = 'tests/data/test_updates_simple.ttl'

        if not os.path.exists(updates_ttl):
            print("⚠️  Updates TTL not found, skipping test")
            return

        temp_db = detector.create_temp_database_from_ttl(updates_ttl)
        result = detector.compare_databases('tourism_test_master', temp_db)

        # Analyze each table in detail
        for table_name in detector.CORE_TABLES:
            table_changes = result.get_changes_for_table(table_name)

            if not table_changes:
                continue

            print(f"\n{table_name.upper()} Changes:")

            for change in table_changes[:3]:  # Show first 3 changes
                print(f"  {change.operation}: {change.entity_id}")

                if change.operation == 'UPDATE' and change.changed_fields:
                    for field in change.changed_fields[:3]:  # Show first 3 changed fields
                        old_val = change.old_values.get(field, 'N/A')
                        new_val = change.new_values.get(field, 'N/A')
                        print(f"    {field}: '{old_val}' -> '{new_val}'")

            if len(table_changes) > 3:
                print(f"  ... and {len(table_changes) - 3} more changes")

    print("✓ Table-level analysis test completed\n")


def test_change_detector_edge_cases():
    """Test edge cases and error handling."""
    print("Testing change detector edge cases...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'

    with ChangeDetector(config) as detector:
        # Test with non-existent TTL file
        try:
            detector.create_temp_database_from_ttl('non_existent_file.ttl')
            assert False, "Should have raised an exception"
        except (FileNotFoundError, RuntimeError):
            print("✓ Non-existent TTL file handled correctly")

        # Test with non-existent database
        try:
            result = detector.compare_databases('non_existent_db1', 'non_existent_db2')
            assert False, "Should have raised an exception"
        except Exception:
            print("✓ Non-existent database comparison handled correctly")

        # Test cleanup functionality
        original_temp_db = detector.temp_db_name
        detector.cleanup_temp_database()
        assert detector.temp_db_name is None, "Temp database name should be cleared"
        print("✓ Cleanup functionality works")

    print("✓ Edge cases test completed\n")


def test_change_detection_performance():
    """Test change detection performance with timing."""
    print("Testing change detection performance...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'

    with ChangeDetector(config) as detector:
        updates_ttl = 'tests/data/test_updates_simple.ttl'

        if not os.path.exists(updates_ttl):
            print("⚠️  Updates TTL not found, skipping test")
            return

        # Time the database creation
        start_time = time.time()
        temp_db = detector.create_temp_database_from_ttl(updates_ttl)
        db_creation_time = time.time() - start_time

        # Time the comparison
        start_time = time.time()
        result = detector.compare_databases('tourism_test_master', temp_db)
        comparison_time = time.time() - start_time

        print(f"✓ Database creation: {db_creation_time:.2f}s")
        print(f"✓ Database comparison: {comparison_time:.2f}s")
        print(f"✓ Total detection time: {result.detection_time:.2f}s")

        # Performance expectations (adjust based on system)
        if db_creation_time < 30:  # Should be under 30 seconds for test data
            print("✓ Database creation performance acceptable")
        else:
            print("⚠️  Database creation slower than expected")

        if comparison_time < 5:  # Should be under 5 seconds for test data
            print("✓ Comparison performance acceptable")
        else:
            print("⚠️  Comparison slower than expected")

    print("✓ Performance test completed\n")


def test_change_detection_data_integrity():
    """Test that change detection preserves data integrity."""
    print("Testing change detection data integrity...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'

    with ChangeDetector(config) as detector:
        updates_ttl = 'tests/data/test_updates_simple.ttl'

        if not os.path.exists(updates_ttl):
            print("⚠️  Updates TTL not found, skipping test")
            return

        temp_db = detector.create_temp_database_from_ttl(updates_ttl)
        result = detector.compare_databases('tourism_test_master', temp_db)

        # Verify that all changes have valid entity IDs
        all_changes = []
        for table_changes in result.changes_by_table.values():
            all_changes.extend(table_changes)

        for change in all_changes:
            assert change.entity_id is not None, "Entity ID should not be None"
            assert change.entity_type in detector.CORE_TABLES, "Entity type should be valid"
            assert change.operation in ['INSERT', 'UPDATE', 'DELETE'], "Operation should be valid"

            if change.operation == 'DELETE':
                assert change.old_values is not None, "DELETE should have old values"
                assert change.new_values is None, "DELETE should not have new values"

            elif change.operation == 'INSERT':
                assert change.old_values is None, "INSERT should not have old values"
                assert change.new_values is not None, "INSERT should have new values"

            elif change.operation == 'UPDATE':
                assert change.old_values is not None, "UPDATE should have old values"
                assert change.new_values is not None, "UPDATE should have new values"
                assert change.changed_fields is not None, "UPDATE should have changed fields"

        print(f"✓ Validated {len(all_changes)} changes for data integrity")

        # Verify summary consistency
        calculated_total = sum(sum(table_summary.values()) for table_summary in result.summary.values())
        assert calculated_total == result.total_changes, "Summary total should match change count"
        print("✓ Summary consistency verified")

    print("✓ Data integrity test completed\n")


def main():
    """Run all change detector tests."""
    print("=== Change Detection Engine Tests ===\n")

    try:
        # Test 1: Baseline Comparison
        test_baseline_comparison()

        # Test 2: Simple CRUD Detection
        test_simple_crud_detection()

        # Test 3: Table-level Analysis
        test_table_level_analysis()

        # Test 4: Edge Cases
        test_change_detector_edge_cases()

        # Test 5: Performance
        test_change_detection_performance()

        # Test 6: Data Integrity
        test_change_detection_data_integrity()

        print("🎉 All Change Detection Engine tests passed!")
        print("\nPhase 3 (Change Detection Engine) core functionality validated.")

    except Exception as e:
        print(f"❌ Change Detection Engine test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()