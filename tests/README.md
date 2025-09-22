# Tourism Database Test Suite

Comprehensive automated test suite for the Tourism Database system, designed to prevent regression issues and ensure data integrity.

## Overview

This test suite was created in response to critical data integrity issues discovered in the TTL parser, where interleaved entity properties caused massive data loss (all `sleeping_places` and `rental_units_count` fields were systematically empty).

## Test Structure

```
tests/
├── unit/                    # Unit tests for individual components
├── integration/             # Integration tests for system components
├── regression/              # Critical regression prevention tests
├── data/                    # Test data files
├── test_runner.py          # Comprehensive test runner
├── requirements.txt        # Test dependencies
└── README.md              # This file
```

## Test Categories

### 1. Regression Tests (`regression/`)

**CRITICAL**: These tests prevent the exact bugs that caused major data loss.

- **`test_ttl_parser_regression.py`**: Tests for the interleaved entity parsing bug
- **`test_data_integrity_monitoring.py`**: Continuous monitoring for systematic data issues

#### Key Regression Tests:

1. **Interleaved Entity Parsing**: Ensures TTL parser correctly handles properties scattered throughout the file
2. **XML Schema Datatype Parsing**: Verifies numeric values like `"4"^^<http://www.w3.org/2001/XMLSchema#integer>` are parsed correctly
3. **Systematic Empty Field Detection**: Monitors for fields that are systematically empty (>95% empty indicates parser failure)
4. **Tourist Attraction Extraction**: Ensures tourist attractions are properly extracted
5. **Relationship Table Population**: Verifies relationship tables are populated

### 2. Unit Tests (`unit/`)

Tests for individual system components:

- **`test_update_system.py`**: Tests for change detection, update processing, and change tracking
- Component isolation and mocking
- Database connection handling
- Error handling and edge cases

### 3. Integration Tests (`integration/`)

Tests for system integration:

- **`test_web_dashboard.py`**: Web dashboard API endpoints and functionality
- End-to-end workflows
- System component interaction
- Real-time monitoring integration

## Running Tests

### Prerequisites

```bash
cd tests/
pip install -r requirements.txt
```

### Run All Tests

```bash
python test_runner.py
```

### Run Specific Test Categories

```bash
# Regression tests only (most critical)
python test_runner.py --regression

# Unit tests only
python test_runner.py --unit

# Integration tests only
python test_runner.py --integration
```

### Run Specific Tests

```bash
# Run specific test file
python test_runner.py --test regression/test_ttl_parser_regression.py

# Run specific test method
python test_runner.py --test regression.test_ttl_parser_regression::TestTTLParserRegression::test_interleaved_entity_parsing
```

### Verbose Output

```bash
python test_runner.py --verbosity 2
```

## Critical Test Data

### Sample Interleaved TTL (`data/sample_interleaved.ttl`)

This file contains TTL data with properties scattered across the file for the same entity - the exact pattern that caused the original bug:

```turtle
tvl:logies/12345 a logies:Logies ;
    schema:name "Test Hotel"@nl .

# Properties scattered for same entity (this was causing the bug)
tvl:logies/12345 logies:aantalSlaapplaatsen "4"^^<http://www.w3.org/2001/XMLSchema#integer> ;
    logies:aantalVerhuurEenheden "2"^^<http://www.w3.org/2001/XMLSchema#integer> .

# More scattered properties
tvl:logies/12345 schema:description "A lovely test hotel"@nl .
```

## Test Database Configuration

Tests use the `tourism_test` database by default. Configure in test files:

```python
db_config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tourism_test',
    'user': 'lieven',
    'password': ''
}
```

## Data Integrity Monitoring

The regression tests include continuous monitoring for:

1. **Systematic Empty Fields** (>95% empty = critical)
2. **Low Relationship Coverage** (<10% = warning)
3. **Missing Tourist Attractions** (0 = critical)
4. **Unexpected Data Volumes** (<50% expected = warning)

## Critical Thresholds

| Check | Warning Threshold | Critical Threshold |
|-------|------------------|-------------------|
| Empty sleeping_places | >80% | >95% |
| Empty rental_units_count | >80% | >95% |
| Missing relationships | <10% coverage | N/A |
| Tourist attractions | <100 total | 0 total |

## Post-Import Verification

**IMPORTANT**: Run regression tests after every data import:

```bash
# Quick regression check
python test_runner.py --regression

# Full integrity monitoring
python -m unittest regression.test_data_integrity_monitoring.TestDataIntegrityMonitoring.test_comprehensive_integrity_monitoring
```

## Continuous Integration

For CI/CD pipelines:

```bash
# Exit with error code if tests fail
python test_runner.py
echo "Exit code: $?"
```

## Test Results Interpretation

### Green (PASS)
- All data integrity checks passed
- System is safe for production use

### Yellow (WARNING)
- Some data quality issues detected
- Review warnings but may proceed with caution

### Red (CRITICAL)
- Major data integrity issues detected
- **DO NOT DEPLOY** - investigate immediately

## Example Output

```
==============================================================
COMPREHENSIVE DATA INTEGRITY REPORT
==============================================================

✅ SYSTEMATIC_EMPTY_FIELDS
   Status: INFO
   Message: Data fields look healthy

✅ RELATIONSHIP_INTEGRITY
   Status: INFO
   Message: Relationship data looks healthy

✅ TOURIST_ATTRACTION_EXTRACTION
   Status: INFO
   Message: Tourist attraction extraction looks healthy (539 attractions)

✅ DATA_COMPLETENESS
   Status: INFO
   Message: Data volumes look reasonable

==============================================================
SUMMARY
==============================================================
🎉 ALL CHECKS PASSED - Data integrity is healthy!
```

## Bug Prevention

This test suite specifically prevents:

1. **Interleaved Entity Parsing Bug**: Properties scattered across TTL file not collected
2. **XML Schema Parsing Bug**: Numeric datatypes not parsed correctly
3. **Missing Entity Types**: Tourist attractions not extracted
4. **Relationship Loss**: Relationship tables not populated
5. **Systematic Data Loss**: Core business fields systematically empty

## Contributing

When adding new features:

1. Add unit tests for new components
2. Add integration tests for new endpoints
3. Add regression tests for any bug fixes
4. Update data integrity monitoring for new data types

## Emergency Response

If critical tests fail:

1. **Stop all deployments immediately**
2. Run individual regression tests to isolate issue
3. Check the `test_data_integrity_monitoring` output for specific problems
4. Compare with known good data volumes
5. Review recent changes to TTL parser or update system

## Contact

For questions about the test suite or interpreting results, refer to the main project documentation or system administrator.