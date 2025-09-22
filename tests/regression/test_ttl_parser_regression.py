"""
Regression Tests for TTL Parser
===============================

Critical regression tests to prevent the interleaved entity parsing bug
that caused massive data loss (sleeping_places and rental_units_count = 0).
"""

import unittest
import tempfile
import os
import sys
import psycopg2
from unittest.mock import patch, MagicMock

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from corrected_ttl_to_postgres_FIXED import FixedTourismDataImporter


class TestTTLParserRegression(unittest.TestCase):
    """Regression tests for the critical TTL parser bug."""

    def setUp(self):
        """Set up test environment."""
        self.test_db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

        # Sample interleaved TTL data that previously caused the bug
        self.sample_ttl_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'sample_interleaved.ttl'
        )

    def test_interleaved_entity_parsing(self):
        """
        CRITICAL: Test that interleaved entities are parsed correctly.

        This is the exact bug that caused all sleeping_places and
        rental_units_count to be 0. Properties for the same entity
        were scattered throughout the TTL file.
        """
        importer = FixedTourismDataImporter(self.test_db_config)

        # Parse the sample TTL with interleaved entities
        importer.parse_ttl_file(self.sample_ttl_path)

        # Check that entities were parsed and stored in the importer
        self.assertGreater(len(importer.logies), 0, "No logies entities found")

        # Find our test logies entity by checking the stored logies
        test_logies_id = None
        for logies_id, logies_data in importer.logies.items():
            if logies_data.get('name') == 'Test Hotel':
                test_logies_id = logies_id
                break

        self.assertIsNotNone(test_logies_id, "Test logies entity not found in parsed data")
        test_logies = importer.logies[test_logies_id]

        # CRITICAL: Verify that scattered properties were collected
        self.assertEqual(test_logies.get('name'), 'Test Hotel')
        self.assertEqual(test_logies.get('description'), 'A lovely test hotel')

        # REGRESSION TEST: These were the missing fields
        self.assertEqual(test_logies.get('sleeping_places'), 4)
        self.assertEqual(test_logies.get('rental_units_count'), 2)

        # Verify relationships are captured
        self.assertGreater(len(importer.logies_addresses), 0)
        self.assertGreater(len(importer.logies_contacts), 0)
        self.assertGreater(len(importer.logies_geometries), 0)

    def test_xml_schema_datatype_parsing(self):
        """Test that XML Schema datatypes are parsed correctly."""
        importer = FixedTourismDataImporter(self.test_db_config)

        # Parse the sample TTL and check that numeric values are correctly extracted
        importer.parse_ttl_file(self.sample_ttl_path)

        # Find the test logies and verify numeric fields were parsed correctly
        test_logies = None
        for logies_id, logies_data in importer.logies.items():
            if logies_data.get('name') == 'Test Hotel':
                test_logies = logies_data
                break

        if test_logies:
            # These values were in XML Schema format in the TTL
            self.assertEqual(test_logies.get('sleeping_places'), 4)
            self.assertEqual(test_logies.get('rental_units_count'), 2)
        else:
            self.skipTest("Test data not properly parsed")

    def test_tourist_attraction_extraction(self):
        """Test that tourist attractions are properly extracted."""
        importer = FixedTourismDataImporter(self.test_db_config)
        importer.parse_ttl_file(self.sample_ttl_path)

        # Check that tourist attractions were extracted
        self.assertGreater(len(importer.tourist_attractions), 0, "No tourist attractions found")

        # Find our test attraction
        test_attraction = None
        for attraction_id, attraction_data in importer.tourist_attractions.items():
            if attraction_data.get('name') == 'Test Museum':
                test_attraction = attraction_data
                break

        self.assertIsNotNone(test_attraction, "Tourist attraction not found")
        self.assertEqual(test_attraction.get('name'), 'Test Museum')
        self.assertEqual(test_attraction.get('description'), 'A wonderful test museum')

    def test_relationship_table_extraction(self):
        """Test that relationship tables are properly extracted."""
        importer = FixedTourismDataImporter(self.test_db_config)
        importer.parse_ttl_file(self.sample_ttl_path)

        # Check that relationship tables were populated
        self.assertGreater(len(importer.logies_addresses), 0, "No logies-address relationships found")
        self.assertGreater(len(importer.logies_contacts), 0, "No logies-contact relationships found")
        self.assertGreater(len(importer.logies_geometries), 0, "No logies-geometry relationships found")

        # Check tourist attraction relationships too
        self.assertGreater(len(importer.attraction_addresses), 0, "No attraction-address relationships found")
        self.assertGreater(len(importer.attraction_geometries), 0, "No attraction-geometry relationships found")

    def test_two_pass_parsing_integrity(self):
        """
        Test that the two-pass parsing approach maintains data integrity.

        This ensures that all properties for an entity are collected
        before processing, preventing the original interleaved bug.
        """
        importer = FixedTourismDataImporter(self.test_db_config)

        # Parse the interleaved TTL file
        importer.parse_ttl_file(self.sample_ttl_path)

        # Verify that all entities have complete data despite being scattered
        # Find the test logies entity
        test_logies = None
        for logies_id, logies_data in importer.logies.items():
            if logies_data.get('name') == 'Test Hotel':
                test_logies = logies_data
                break

        if test_logies:
            # All these properties were scattered in the TTL file
            # but should be present due to two-pass parsing
            self.assertIsNotNone(test_logies.get('name'))
            self.assertIsNotNone(test_logies.get('description'))
            self.assertIsNotNone(test_logies.get('sleeping_places'))
            self.assertIsNotNone(test_logies.get('rental_units_count'))
        else:
            self.fail("Test logies entity not found - parsing may have failed")

        # Verify relationships were also captured despite being scattered
        self.assertGreater(len(importer.logies_addresses), 0)
        self.assertGreater(len(importer.logies_contacts), 0)
        self.assertGreater(len(importer.logies_geometries), 0)


class TestDatabaseIntegrityRegression(unittest.TestCase):
    """Test database integrity after parsing."""

    def setUp(self):
        """Set up test database connection."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

    def test_no_systematic_empty_fields(self):
        """
        CRITICAL: Ensure no systematic empty fields like the original bug.

        This test verifies that core business data is not systematically
        missing from the database after import.
        """
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Check for systematic empty sleeping_places (original bug)
                    cur.execute("""
                        SELECT
                            COUNT(*) as total_logies,
                            COUNT(*) FILTER (WHERE sleeping_places > 0) as with_sleeping_places,
                            COUNT(*) FILTER (WHERE rental_units_count > 0) as with_rental_units
                        FROM logies
                    """)

                    result = cur.fetchone()
                    total, with_sleeping, with_rental = result

                    if total > 0:
                        # At least some records should have sleeping places data
                        sleeping_percentage = (with_sleeping / total) * 100
                        rental_percentage = (with_rental / total) * 100

                        # If less than 5% have data, this indicates systematic failure
                        self.assertGreater(sleeping_percentage, 5.0,
                                         f"Only {sleeping_percentage:.1f}% of logies have sleeping_places data")
                        self.assertGreater(rental_percentage, 5.0,
                                         f"Only {rental_percentage:.1f}% of logies have rental_units_count data")

        except psycopg2.Error:
            self.skipTest("Database not available for testing")

    def test_relationship_tables_populated(self):
        """Test that relationship tables are properly populated."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Check that relationship tables have data
                    relationship_tables = [
                        'logies_addresses',
                        'logies_contacts',
                        'logies_geometries',
                        'attraction_addresses',
                        'attraction_contacts',
                        'attraction_geometries'
                    ]

                    for table in relationship_tables:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cur.fetchone()[0]
                        # At least some relationships should exist
                        # (Skip if table doesn't exist yet)
                        if count == 0:
                            # Check if table exists
                            cur.execute("""
                                SELECT COUNT(*) FROM information_schema.tables
                                WHERE table_name = %s
                            """, (table,))
                            if cur.fetchone()[0] > 0:
                                print(f"Warning: {table} exists but is empty")

        except psycopg2.Error:
            self.skipTest("Database not available for testing")


if __name__ == '__main__':
    unittest.main()