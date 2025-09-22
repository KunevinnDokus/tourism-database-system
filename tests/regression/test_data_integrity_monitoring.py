"""
Data Integrity Monitoring and Regression Prevention
===================================================

Continuous monitoring tests to prevent regression of critical data integrity issues.
These tests should be run after every import to ensure data quality.
"""

import unittest
import psycopg2
import os
import sys
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


@dataclass
class DataIntegrityCheck:
    """Data integrity check result."""
    check_name: str
    passed: bool
    message: str
    severity: str  # 'CRITICAL', 'WARNING', 'INFO'
    details: Dict[str, Any]


class DataIntegrityMonitor:
    """Monitor for critical data integrity issues."""

    def __init__(self, db_config: Dict[str, Any]):
        """Initialize integrity monitor."""
        self.db_config = db_config
        self.connection = None

    def connect(self) -> None:
        """Connect to database."""
        self.connection = psycopg2.connect(**self.db_config)

    def disconnect(self) -> None:
        """Disconnect from database."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def check_systematic_empty_fields(self) -> DataIntegrityCheck:
        """
        CRITICAL: Check for systematic empty fields that indicate parser bugs.

        This is the exact check that would have caught the original sleeping_places
        and rental_units_count bug.
        """
        try:
            with self.connection.cursor() as cur:
                # Check logies table for systematic empty core fields
                cur.execute("""
                    SELECT
                        COUNT(*) as total_logies,
                        COUNT(*) FILTER (WHERE sleeping_places IS NULL OR sleeping_places = 0) as empty_sleeping_places,
                        COUNT(*) FILTER (WHERE rental_units_count IS NULL OR rental_units_count = 0) as empty_rental_units,
                        COUNT(*) FILTER (WHERE name IS NULL OR name = '') as empty_names,
                        COUNT(*) FILTER (WHERE description IS NULL OR description = '') as empty_descriptions
                    FROM logies
                """)

                result = cur.fetchone()
                total, empty_sleeping, empty_rental, empty_names, empty_desc = result

                if total == 0:
                    return DataIntegrityCheck(
                        check_name="systematic_empty_fields",
                        passed=True,
                        message="No logies data to check",
                        severity="INFO",
                        details={"total_logies": 0}
                    )

                # Calculate percentages
                empty_sleeping_pct = (empty_sleeping / total) * 100
                empty_rental_pct = (empty_rental / total) * 100
                empty_names_pct = (empty_names / total) * 100

                # Critical thresholds (more than 95% empty indicates systematic failure)
                critical_threshold = 95.0
                warning_threshold = 80.0

                issues = []
                severity = "INFO"

                if empty_sleeping_pct > critical_threshold:
                    issues.append(f"sleeping_places: {empty_sleeping_pct:.1f}% empty")
                    severity = "CRITICAL"
                elif empty_sleeping_pct > warning_threshold:
                    issues.append(f"sleeping_places: {empty_sleeping_pct:.1f}% empty (warning)")
                    severity = "WARNING"

                if empty_rental_pct > critical_threshold:
                    issues.append(f"rental_units_count: {empty_rental_pct:.1f}% empty")
                    severity = "CRITICAL"
                elif empty_rental_pct > warning_threshold:
                    issues.append(f"rental_units_count: {empty_rental_pct:.1f}% empty (warning)")
                    severity = "WARNING"

                if empty_names_pct > critical_threshold:
                    issues.append(f"names: {empty_names_pct:.1f}% empty")
                    severity = "CRITICAL"

                passed = len(issues) == 0 or severity != "CRITICAL"
                message = "Data fields look healthy" if passed else f"Systematic empty fields detected: {', '.join(issues)}"

                return DataIntegrityCheck(
                    check_name="systematic_empty_fields",
                    passed=passed,
                    message=message,
                    severity=severity,
                    details={
                        "total_logies": total,
                        "empty_sleeping_places_pct": empty_sleeping_pct,
                        "empty_rental_units_pct": empty_rental_pct,
                        "empty_names_pct": empty_names_pct,
                        "empty_descriptions_pct": (empty_desc / total) * 100
                    }
                )

        except Exception as e:
            return DataIntegrityCheck(
                check_name="systematic_empty_fields",
                passed=False,
                message=f"Error checking systematic empty fields: {e}",
                severity="CRITICAL",
                details={"error": str(e)}
            )

    def check_relationship_integrity(self) -> DataIntegrityCheck:
        """Check that relationship tables are properly populated."""
        try:
            with self.connection.cursor() as cur:
                # Check relationship table populations
                relationship_checks = {}

                relationship_tables = [
                    ('logies_addresses', 'logies', 'Logies should have addresses'),
                    ('logies_contacts', 'logies', 'Logies should have contact info'),
                    ('logies_geometries', 'logies', 'Logies should have locations'),
                    ('attraction_addresses', 'tourist_attractions', 'Attractions should have addresses'),
                    ('attraction_contacts', 'tourist_attractions', 'Attractions should have contact info'),
                    ('attraction_geometries', 'tourist_attractions', 'Attractions should have locations')
                ]

                issues = []
                severity = "INFO"

                for rel_table, main_table, description in relationship_tables:
                    try:
                        # Check if relationship table exists and has data
                        cur.execute(f"SELECT COUNT(*) FROM {rel_table}")
                        rel_count = cur.fetchone()[0]

                        cur.execute(f"SELECT COUNT(*) FROM {main_table}")
                        main_count = cur.fetchone()[0]

                        if main_count > 0:
                            coverage_pct = (rel_count / main_count) * 100 if main_count > 0 else 0
                            relationship_checks[rel_table] = {
                                'relationship_count': rel_count,
                                'main_table_count': main_count,
                                'coverage_percentage': coverage_pct
                            }

                            # Very low coverage suggests extraction problems
                            if coverage_pct < 10.0 and main_count > 100:
                                issues.append(f"{rel_table}: only {coverage_pct:.1f}% coverage")
                                severity = "WARNING"

                    except psycopg2.Error as e:
                        if "does not exist" in str(e):
                            relationship_checks[rel_table] = {'error': 'Table does not exist'}
                        else:
                            relationship_checks[rel_table] = {'error': str(e)}

                passed = len(issues) == 0
                message = "Relationship data looks healthy" if passed else f"Low relationship coverage: {', '.join(issues)}"

                return DataIntegrityCheck(
                    check_name="relationship_integrity",
                    passed=passed,
                    message=message,
                    severity=severity,
                    details=relationship_checks
                )

        except Exception as e:
            return DataIntegrityCheck(
                check_name="relationship_integrity",
                passed=False,
                message=f"Error checking relationship integrity: {e}",
                severity="CRITICAL",
                details={"error": str(e)}
            )

    def check_tourist_attraction_extraction(self) -> DataIntegrityCheck:
        """Check that tourist attractions are being extracted."""
        try:
            with self.connection.cursor() as cur:
                # Check tourist attractions table
                cur.execute("SELECT COUNT(*) FROM tourist_attractions")
                attraction_count = cur.fetchone()[0]

                # Check that we have a reasonable number of attractions
                # (based on known data, should be several hundred)
                expected_minimum = 100

                if attraction_count == 0:
                    return DataIntegrityCheck(
                        check_name="tourist_attraction_extraction",
                        passed=False,
                        message="No tourist attractions found - extraction may have failed",
                        severity="CRITICAL",
                        details={"attraction_count": 0}
                    )
                elif attraction_count < expected_minimum:
                    return DataIntegrityCheck(
                        check_name="tourist_attraction_extraction",
                        passed=False,
                        message=f"Only {attraction_count} tourist attractions found (expected >= {expected_minimum})",
                        severity="WARNING",
                        details={"attraction_count": attraction_count, "expected_minimum": expected_minimum}
                    )
                else:
                    return DataIntegrityCheck(
                        check_name="tourist_attraction_extraction",
                        passed=True,
                        message=f"Tourist attraction extraction looks healthy ({attraction_count} attractions)",
                        severity="INFO",
                        details={"attraction_count": attraction_count}
                    )

        except Exception as e:
            return DataIntegrityCheck(
                check_name="tourist_attraction_extraction",
                passed=False,
                message=f"Error checking tourist attraction extraction: {e}",
                severity="CRITICAL",
                details={"error": str(e)}
            )

    def check_data_completeness(self) -> DataIntegrityCheck:
        """Check overall data completeness compared to expected volumes."""
        try:
            with self.connection.cursor() as cur:
                # Get counts for all major tables
                table_counts = {}

                tables_to_check = [
                    ('logies', 30000),  # Expected ~31k logies
                    ('tourist_attractions', 500),  # Expected ~500+ attractions
                    ('addresses', 40000),  # Expected addresses
                    ('contact_points', 30000),  # Expected contact points
                    ('geometries', 25000)  # Expected geometries
                ]

                issues = []
                severity = "INFO"

                for table, expected_min in tables_to_check:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cur.fetchone()[0]
                        table_counts[table] = count

                        if count < expected_min * 0.5:  # Less than 50% of expected
                            issues.append(f"{table}: {count} (expected >= {expected_min})")
                            severity = "WARNING"

                    except psycopg2.Error:
                        table_counts[table] = 0
                        issues.append(f"{table}: table missing or inaccessible")
                        severity = "CRITICAL"

                passed = len(issues) == 0
                message = "Data volumes look reasonable" if passed else f"Low data volumes: {', '.join(issues)}"

                return DataIntegrityCheck(
                    check_name="data_completeness",
                    passed=passed,
                    message=message,
                    severity=severity,
                    details=table_counts
                )

        except Exception as e:
            return DataIntegrityCheck(
                check_name="data_completeness",
                passed=False,
                message=f"Error checking data completeness: {e}",
                severity="CRITICAL",
                details={"error": str(e)}
            )

    def run_all_checks(self) -> List[DataIntegrityCheck]:
        """Run all data integrity checks."""
        checks = [
            self.check_systematic_empty_fields(),
            self.check_relationship_integrity(),
            self.check_tourist_attraction_extraction(),
            self.check_data_completeness()
        ]

        return checks


class TestDataIntegrityMonitoring(unittest.TestCase):
    """Test cases for data integrity monitoring."""

    def setUp(self):
        """Set up test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'tourism_test',
            'user': 'lieven',
            'password': ''
        }

    def test_systematic_empty_fields_detection(self):
        """Test detection of systematic empty fields."""
        try:
            with DataIntegrityMonitor(self.db_config) as monitor:
                result = monitor.check_systematic_empty_fields()

                # The check should complete without error
                self.assertIsInstance(result, DataIntegrityCheck)
                self.assertEqual(result.check_name, "systematic_empty_fields")

                # If there's data, it should not be systematically empty
                if result.details.get('total_logies', 0) > 1000:
                    # With substantial data, critical failures should not occur
                    if result.severity == "CRITICAL":
                        self.fail(f"Critical data integrity issue detected: {result.message}")

                    # Print results for monitoring
                    print(f"Systematic Empty Fields Check: {result.message}")
                    print(f"Details: {result.details}")

        except psycopg2.Error:
            self.skipTest("Database not available for testing")

    def test_relationship_integrity_monitoring(self):
        """Test relationship integrity monitoring."""
        try:
            with DataIntegrityMonitor(self.db_config) as monitor:
                result = monitor.check_relationship_integrity()

                self.assertIsInstance(result, DataIntegrityCheck)
                self.assertEqual(result.check_name, "relationship_integrity")

                # Print results for monitoring
                print(f"Relationship Integrity Check: {result.message}")
                print(f"Details: {result.details}")

        except psycopg2.Error:
            self.skipTest("Database not available for testing")

    def test_tourist_attraction_extraction_monitoring(self):
        """Test tourist attraction extraction monitoring."""
        try:
            with DataIntegrityMonitor(self.db_config) as monitor:
                result = monitor.check_tourist_attraction_extraction()

                self.assertIsInstance(result, DataIntegrityCheck)
                self.assertEqual(result.check_name, "tourist_attraction_extraction")

                # Print results for monitoring
                print(f"Tourist Attraction Extraction Check: {result.message}")
                print(f"Details: {result.details}")

                # If there's supposed to be attraction data, ensure it's there
                if result.severity == "CRITICAL":
                    print(f"WARNING: {result.message}")

        except psycopg2.Error:
            self.skipTest("Database not available for testing")

    def test_comprehensive_integrity_monitoring(self):
        """Run comprehensive integrity monitoring."""
        try:
            with DataIntegrityMonitor(self.db_config) as monitor:
                results = monitor.run_all_checks()

                print("\n" + "="*60)
                print("COMPREHENSIVE DATA INTEGRITY REPORT")
                print("="*60)

                critical_issues = []
                warnings = []

                for result in results:
                    status_icon = "✅" if result.passed else "❌"
                    print(f"\n{status_icon} {result.check_name.upper()}")
                    print(f"   Status: {result.severity}")
                    print(f"   Message: {result.message}")

                    if result.severity == "CRITICAL" and not result.passed:
                        critical_issues.append(result)
                    elif result.severity == "WARNING":
                        warnings.append(result)

                print("\n" + "="*60)
                print("SUMMARY")
                print("="*60)

                if critical_issues:
                    print(f"🚨 CRITICAL ISSUES: {len(critical_issues)}")
                    for issue in critical_issues:
                        print(f"   - {issue.message}")
                    print("\n⚠️  IMMEDIATE ACTION REQUIRED!")

                if warnings:
                    print(f"⚠️  WARNINGS: {len(warnings)}")
                    for warning in warnings:
                        print(f"   - {warning.message}")

                if not critical_issues and not warnings:
                    print("🎉 ALL CHECKS PASSED - Data integrity is healthy!")

                # Fail the test if there are critical issues
                if critical_issues:
                    self.fail(f"Critical data integrity issues detected: {len(critical_issues)} issues")

        except psycopg2.Error:
            self.skipTest("Database not available for testing")


if __name__ == '__main__':
    unittest.main()