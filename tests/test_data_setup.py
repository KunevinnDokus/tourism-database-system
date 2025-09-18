"""
Test Data Setup Validation

Validates that the test data infrastructure is properly set up:
- Test master database has correct data
- Test TTL files are valid RDF
- Expected results match test scenarios
"""

import sys
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_system import DEFAULT_DB_CONFIG


def test_master_database_setup():
    """Test that the test master database is properly set up."""
    print("Testing test master database setup...")

    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    connection = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )

    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        # Test table existence and record counts
        cursor.execute("SELECT * FROM test_data_summary ORDER BY table_name")
        results = cursor.fetchall()

        expected_counts = {
            'logies': 5,
            'addresses': 5,
            'contact_points': 5,
            'geometries': 5,
            'identifiers': 5
        }

        for row in results:
            table_name = row['table_name']
            count = row['record_count']
            expected = expected_counts.get(table_name, 0)

            assert count == expected, f"Table {table_name}: expected {expected} records, got {count}"
            print(f"✓ {table_name}: {count} records")

        # Test specific data integrity
        cursor.execute("""
            SELECT l.id, l.name, a.municipality, c.email, g.latitude, i.identifier_value
            FROM logies l
            LEFT JOIN addresses a ON l.id = a.logies_id
            LEFT JOIN contact_points c ON l.id = c.logies_id
            LEFT JOIN geometries g ON l.id = g.logies_id
            LEFT JOIN identifiers i ON l.id = i.related_entity_id
            ORDER BY l.name
        """)

        complete_records = cursor.fetchall()
        assert len(complete_records) == 5, f"Expected 5 complete records, got {len(complete_records)}"

        # Verify specific test data
        test_hotel = next((r for r in complete_records if r['name'] == 'Test Hotel Brussels'), None)
        assert test_hotel is not None, "Test Hotel Brussels not found"
        assert test_hotel['municipality'] == 'Brussels', "Test Hotel Brussels municipality mismatch"
        assert test_hotel['email'] == 'info@testhotelbrussels.be', "Test Hotel Brussels email mismatch"
        assert test_hotel['identifier_value'] == 'TVL001', "Test Hotel Brussels identifier mismatch"

        print("✓ Data integrity checks passed")

    connection.close()
    print("✓ Test master database setup validated\n")


def test_ttl_file_validity():
    """Test that TTL files are valid RDF."""
    print("Testing TTL file validity...")

    try:
        from rdflib import Graph
    except ImportError:
        print("⚠️  rdflib not available, skipping TTL validation")
        return

    ttl_files = [
        'tests/data/test_baseline.ttl',
        'tests/data/test_updates_simple.ttl'
    ]

    for ttl_file in ttl_files:
        if not os.path.exists(ttl_file):
            print(f"❌ TTL file not found: {ttl_file}")
            continue

        try:
            g = Graph()
            g.parse(ttl_file, format='turtle')
            triple_count = len(g)
            print(f"✓ {ttl_file}: {triple_count} triples")

            # Basic validation - check for key entities
            if 'baseline' in ttl_file:
                # Should have 5 logies entities
                logies_query = """
                    SELECT (COUNT(?logies) AS ?count)
                    WHERE {
                        ?logies a <https://data.vlaanderen.be/ns/logies#Logies> .
                    }
                """
                results = list(g.query(logies_query))
                logies_count = int(results[0][0])
                assert logies_count == 5, f"Baseline should have 5 logies, found {logies_count}"

            elif 'simple' in ttl_file:
                # Should have logies entities (including new ones)
                logies_query = """
                    SELECT (COUNT(?logies) AS ?count)
                    WHERE {
                        ?logies a <https://data.vlaanderen.be/ns/logies#Logies> .
                    }
                """
                results = list(g.query(logies_query))
                logies_count = int(results[0][0])
                # Simple updates: 3 existing + 2 new = 5 total (2 deleted from original 5)
                assert logies_count == 5, f"Simple updates should have 5 logies, found {logies_count}"

        except Exception as e:
            print(f"❌ Error parsing {ttl_file}: {e}")
            raise

    print("✓ TTL file validity tests passed\n")


def test_expected_results_file():
    """Test that expected results file is valid JSON with correct structure."""
    print("Testing expected results file...")

    expected_file = 'tests/fixtures/expected_results.json'

    if not os.path.exists(expected_file):
        print(f"❌ Expected results file not found: {expected_file}")
        return

    try:
        with open(expected_file, 'r') as f:
            expected_results = json.load(f)

        # Test structure
        assert 'test_updates_simple' in expected_results, "Missing test_updates_simple scenario"
        assert 'test_baseline_to_baseline' in expected_results, "Missing baseline comparison scenario"

        simple_test = expected_results['test_updates_simple']
        assert 'expected_changes' in simple_test, "Missing expected_changes in simple test"
        assert 'summary' in simple_test, "Missing summary in simple test"

        # Test that all entity types are covered
        changes = simple_test['expected_changes']
        required_entities = ['logies', 'addresses', 'contact_points', 'geometries', 'identifiers']

        for entity in required_entities:
            assert entity in changes, f"Missing entity type: {entity}"
            entity_changes = changes[entity]
            assert 'inserts' in entity_changes, f"Missing inserts for {entity}"
            assert 'updates' in entity_changes, f"Missing updates for {entity}"
            assert 'deletes' in entity_changes, f"Missing deletes for {entity}"

        # Test summary numbers make sense
        summary = simple_test['summary']
        assert summary['total_changes'] > 0, "Total changes should be > 0 for simple test"
        assert summary['total_changes'] == (summary['total_inserts'] +
                                          summary['total_updates'] +
                                          summary['total_deletes']), "Summary totals don't add up"

        # Test baseline scenario
        baseline_test = expected_results['test_baseline_to_baseline']
        baseline_summary = baseline_test['summary']
        assert baseline_summary['total_changes'] == 0, "Baseline comparison should have 0 changes"

        print(f"✓ Expected results structure valid")
        print(f"✓ Simple test expects {summary['total_changes']} total changes")
        print(f"✓ Baseline test expects {baseline_summary['total_changes']} changes")

    except Exception as e:
        print(f"❌ Error validating expected results: {e}")
        raise

    print("✓ Expected results file validation passed\n")


def test_file_structure():
    """Test that all required test files and directories exist."""
    print("Testing file structure...")

    required_dirs = [
        'tests/data',
        'tests/fixtures',
        'tests/integration',
        'tests/performance'
    ]

    required_files = [
        'tests/fixtures/setup_test_master.sql',
        'tests/fixtures/expected_results.json',
        'tests/data/test_baseline.ttl',
        'tests/data/test_updates_simple.ttl'
    ]

    for directory in required_dirs:
        assert os.path.exists(directory), f"Missing directory: {directory}"
        print(f"✓ Directory exists: {directory}")

    for file_path in required_files:
        assert os.path.exists(file_path), f"Missing file: {file_path}"
        print(f"✓ File exists: {file_path}")

    print("✓ File structure validation passed\n")


def main():
    """Run all test data setup validation tests."""
    print("=== Test Data Setup Validation ===\n")

    try:
        # Test 1: File Structure
        test_file_structure()

        # Test 2: Database Setup
        test_master_database_setup()

        # Test 3: TTL File Validity
        test_ttl_file_validity()

        # Test 4: Expected Results File
        test_expected_results_file()

        print("🎉 All test data setup validation tests passed!")
        print("\nPhase 1.5 (Test Data Infrastructure) is complete and validated.")
        print("Ready to proceed with Phase 2: Data Source & Download Management")

    except Exception as e:
        print(f"❌ Test data setup validation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()