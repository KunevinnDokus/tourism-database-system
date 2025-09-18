# Tourism Database System

A comprehensive PostgreSQL database system for processing and managing Flemish tourism data from RDF/TTL sources. This system successfully imports and structures accommodation data, addresses, contact points, geometries, and identifiers from the official Flemish Tourism "Logies Basis Application Profile" dataset.

## Overview

This project processes the complete Flemish tourism dataset (553MB TTL file) containing accommodation information, addresses, contact details, and geographical data. The system has been tested and validated to successfully import **339,037 records** across 5 core entity types with complete address data restoration.

## Data Model

The database schema consists of 5 core tables that represent the tourism data structure:

### Core Tables

#### 1. logies (Accommodations)
Primary accommodation entities with business information:
- `id` (UUID, Primary Key)
- `name` (TEXT) - Accommodation name
- `description` (TEXT) - Detailed description
- `accommodation_type` (TEXT) - Type of accommodation
- `rental_units` (INTEGER) - Number of rental units
- `created_at`, `updated_at` (TIMESTAMP)

#### 2. addresses (Address Information)
Complete address data linked to accommodations:
- `id` (UUID, Primary Key)
- `street_name` (TEXT) - Street name
- `house_number` (TEXT) - House number
- `postal_code` (TEXT) - Postal code
- `municipality` (TEXT, NOT NULL) - Municipality name
- `country` (TEXT) - Country
- `full_address` (TEXT) - Complete formatted address
- `logies_id` (UUID, Foreign Key to logies)

#### 3. contact_points (Contact Information)
Contact details for accommodations:
- `id` (UUID, Primary Key)
- `email` (TEXT) - Email address
- `telephone` (TEXT) - Phone number
- `fax` (TEXT) - Fax number
- `website` (TEXT) - Website URL
- `contact_type` (TEXT) - Type of contact
- `logies_id` (UUID, Foreign Key to logies)

#### 4. geometries (Geographic Data)
Spatial coordinates and location information:
- `id` (UUID, Primary Key)
- `latitude` (DECIMAL) - Latitude coordinate
- `longitude` (DECIMAL) - Longitude coordinate
- `geometry_type` (TEXT) - Type of geometry
- `coordinate_system` (TEXT) - Coordinate reference system
- `logies_id` (UUID, Foreign Key to logies)

#### 5. identifiers (External Identifiers)
External system identifiers and references:
- `id` (UUID, Primary Key)
- `identifier_value` (TEXT) - The identifier value
- `identifier_type` (TEXT) - Type of identifier
- `notation` (TEXT) - Notation format
- `related_entity_id` (UUID) - Related entity UUID
- `related_entity_type` (TEXT) - Type of related entity

## TTL Source File Structure

The source data (`toeristische-attracties.ttl`, 553MB) follows the RDF/Turtle format with these key entity types:

### Entity Types in TTL
- **Registratie** - Main accommodation registration entities
- **Address** - Address information using LOCN vocabulary
- **ContactPoint** - Contact details using schema.org
- **Point/Geometry** - Geographic coordinates using LOCN
- **Identifier** - External identifiers using ADMS

### Sample TTL Structure
```turtle
<https://data.vlaanderen.be/id/logies/12345> a logies:Logies ;
    rdfs:label "Hotel Example"@nl ;
    schema:description "Description text"@nl ;
    locn:address <https://data.vlaanderen.be/id/adres/67890> ;
    schema:contactPoint <https://data.vlaanderen.be/id/contactpunt/111> ;
    adms:identifier <https://data.vlaanderen.be/id/identificator/222> .

<https://data.vlaanderen.be/id/adres/67890> a locn:Address ;
    locn:thoroughfare "Hoofdstraat" ;
    adms:addressId "123" ;
    locn:postCode "1000" ;
    locn:adminUnitL2 "Brussels" .
```

### Key Vocabularies Used
- **logies:** - Flemish Tourism vocabulary
- **locn:** - Core Location Vocabulary
- **schema:** - Schema.org vocabulary
- **adms:** - Asset Description Metadata Schema
- **rdfs:** - RDF Schema vocabulary

## Production Setup Procedure

### Prerequisites
1. **PostgreSQL 15+** installed and running
2. **Python 3.8+** with required packages:
   ```bash
   pip install psycopg2-binary rdflib
   ```
3. **Database access** with CREATE privileges

### Step 1: Database Preparation
```bash
# Create production database
createdb tourism_flanders_corrected

# Create database schema
psql -d tourism_flanders_corrected -f corrected_tourism_schema.sql
```

### Step 2: Data Import
```bash
# Run the enhanced data import script
python3 corrected_ttl_to_postgres_ENHANCED.py \
    --ttl-file toeristische-attracties.ttl \
    --db-name tourism_flanders_corrected \
    --db-user your_username \
    --db-password your_password
```

### Step 3: Validation
```sql
-- Verify data import success
SELECT
    'logies' as table_name, COUNT(*) as record_count
FROM logies
UNION ALL
SELECT 'addresses', COUNT(*) FROM addresses
UNION ALL
SELECT 'contact_points', COUNT(*) FROM contact_points
UNION ALL
SELECT 'geometries', COUNT(*) FROM geometries
UNION ALL
SELECT 'identifiers', COUNT(*) FROM identifiers;

-- Check address data completeness
SELECT
    COUNT(*) as total_addresses,
    COUNT(street_name) as addresses_with_streets,
    COUNT(municipality) as addresses_with_municipality
FROM addresses;
```

Expected results:
- **logies**: ~15,000 records
- **addresses**: ~52,000 records (100% with municipality, ~99% with street names)
- **contact_points**: ~40,000 records
- **geometries**: ~200,000 records
- **identifiers**: ~32,000 records

## Incremental Update System

The Tourism Database includes a comprehensive incremental update system designed to automatically detect and apply changes from new TTL data sources while maintaining full audit trails and transaction safety.

### System Architecture

The update system consists of 4 integrated phases:

#### Phase 1: Change Tracking Infrastructure
- **Audit Tables**: Comprehensive changelog tables for all entity types
- **Trigger System**: Automatic change capture for INSERT/UPDATE/DELETE operations
- **Run Management**: Update run tracking with timing and statistics
- **Session Management**: Run ID context for associating changes with update operations

#### Phase 2: Data Source & Download Management
- **TTL File Handling**: Download, validation, and integrity checking
- **Metadata Tracking**: File hash comparison and change detection
- **Permanent Storage**: Downloads saved to `downloads/` directory with timestamps
- **File Organization**: Automatic timestamped naming (`toeristische-attracties_YYYYMMDD-HHMMSS.ttl`)
- **Download Management**: List, cleanup, and analyze downloaded files
- **URL Validation**: Source availability checking with retry logic

#### Phase 3: Change Detection Engine
- **Database Comparison**: Entity-level diff operations between master and new data
- **Change Classification**: Automatic categorization as INSERT/UPDATE/DELETE
- **Validation Framework**: Data integrity checking and conflict detection
- **Temporary Database**: Isolated environment for new data processing

#### Phase 4: Update Processing & Integration
- **Transaction Safety**: Full rollback capability on errors
- **Batch Processing**: Configurable batch sizes for performance optimization
- **Dependency Ordering**: Referential integrity maintenance through proper table ordering
- **Dry Run Mode**: Safe validation without applying changes

### Update System Components

#### Core Modules

**`update_system/change_tracker.py`**
- Manages audit logging and update run lifecycle
- Tracks all database changes with before/after values
- Provides statistics and reporting capabilities

**`update_system/data_source_manager.py`**
- Handles TTL file downloads and validation
- Compares file metadata to detect changes
- Manages permanent downloads storage with timestamped files
- Provides downloads management utilities (list, cleanup, summary)

**`update_system/change_detector.py`**
- Creates temporary databases from TTL files
- Performs entity-level comparison between databases
- Generates detailed change reports with operation types

**`update_system/update_processor.py`**
- Applies incremental changes with transaction safety
- Supports dry run validation mode
- Maintains referential integrity through dependency ordering
- Provides comprehensive error handling and rollback

#### Schema Files

**`sql/changelog_schema.sql`**
- Change tracking tables (update_runs, *_changelog)
- Audit views and summary reporting
- Index optimization for performance

**`sql/triggers.sql`**
- Automatic audit triggers for all core tables
- Run ID management functions
- Utility functions for trigger management

### Usage Examples

#### Basic Update Operation
```python
from update_system.data_source_manager import DataSourceManager
from update_system.change_detector import ChangeDetector
from update_system.update_processor import UpdateProcessor
from update_system import DEFAULT_DB_CONFIG

# Configure database connection
config = DEFAULT_DB_CONFIG.copy()
config.update({
    'user': 'your_username',
    'database': 'tourism_production',
    'password': 'your_password'
})

# Download and validate new TTL data (saves to downloads/ directory)
with DataSourceManager(config) as dsm:
    file_path, file_hash, file_size = dsm.download_latest_ttl(save_to_downloads=True)

    # Check if file has changed
    if dsm.compare_file_metadata(file_hash, file_size)['has_changes']:

        # Detect changes
        with ChangeDetector(config) as detector:
            temp_db = detector.create_temp_database_from_ttl(file_path)
            changes = detector.compare_databases('tourism_production', temp_db)

            # Apply changes
            with UpdateProcessor(config) as processor:
                # Dry run first
                dry_result = processor.apply_changes(changes, dry_run=True)

                if dry_result.success:
                    # Apply for real
                    result = processor.apply_changes(changes, dry_run=False)
                    print(f"Applied {result.records_processed} changes successfully")
```

#### Validation-Only Mode
```python
# Run change detection without applying updates
with ChangeDetector(config) as detector:
    temp_db = detector.create_temp_database_from_ttl('new_data.ttl')
    changes = detector.compare_databases('tourism_production', temp_db)

    # Validate changes
    validation = detector.validate_comparison_result(changes)

    if validation['is_valid']:
        print(f"Detected {changes.total_changes} valid changes")
        for table, summary in changes.summary.items():
            print(f"  {table}: {summary}")
    else:
        print("Validation errors:", validation['errors'])
```

#### Monitoring and Statistics
```python
with UpdateProcessor(config) as processor:
    # Get overall statistics
    stats = processor.get_processing_statistics()
    print(f"Recent runs: {stats['recent_runs']}")
    print(f"30-day changes: {stats['total_changes_30_days']}")

    # Get specific run details
    run_stats = processor.get_processing_statistics(run_id='specific-run-id')
    print(f"Run status: {run_stats['status']}")
    print(f"Records processed: {run_stats['total_changes']}")
```

### Command Line Interface

The update system includes a comprehensive CLI for managing all operations:

#### Update Commands
```bash
# Full update workflow
python3 tourism_update_cli.py update

# Update with custom configuration
python3 tourism_update_cli.py update --config config.json

# Update with custom database
python3 tourism_update_cli.py update --db-name my_db --db-user myuser

# Validation-only (no changes applied)
python3 tourism_update_cli.py validate --source-url https://example.com/data.ttl
```

#### Downloads Management
```bash
# List all downloaded files
python3 tourism_update_cli.py downloads list

# Show latest download info
python3 tourism_update_cli.py downloads latest

# Get downloads summary
python3 tourism_update_cli.py downloads summary

# Clean up files older than 30 days
python3 tourism_update_cli.py downloads cleanup --days-to-keep 30
```

#### System Operations
```bash
# Check system status
python3 tourism_update_cli.py status

# Create database backup
python3 tourism_update_cli.py backup

# Generate sample configuration
python3 tourism_update_cli.py create-config --output my_config.json
```

#### Download File Organization

Downloaded TTL files are automatically organized in the `downloads/` directory:
- **File naming**: `toeristische-attracties_YYYYMMDD-HHMMSS.ttl`
- **Automatic timestamps**: Files include download date and time
- **Historical tracking**: All downloads preserved for comparison
- **Size management**: Each file is approximately 550MB

### Update System Configuration

#### Database Setup
```bash
# Install change tracking schema
psql -d your_database -f sql/changelog_schema.sql
psql -d your_database -f sql/triggers.sql
```

#### Environment Configuration
```python
# Default configuration in update_system/__init__.py
DEFAULT_DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tourism_flanders_corrected',
    'user': 'postgres',
    'password': ''
}

TOURISM_DATA_SOURCE = {
    'current_file_url': 'https://data.vlaanderen.be/sparql-endpoint/tourism/data.ttl',
    'download_timeout': 300,
    'max_retries': 3
}
```

### Testing Framework

The update system includes comprehensive tests for all components:

```bash
# Run individual phase tests
python3 tests/test_change_tracker.py       # Phase 1 tests
python3 tests/test_data_source_manager.py  # Phase 2 tests
python3 tests/test_change_detector.py      # Phase 3 tests
python3 tests/test_update_processor.py     # Phase 4 tests

# Test data located in tests/data/
# - test_baseline.ttl: Baseline dataset for comparison
# - test_updates_simple.ttl: Simple change scenarios
```

### Performance Characteristics

#### Change Detection Performance
- **Database Comparison**: ~30-60 seconds for full dataset
- **TTL Import**: ~10-20 minutes for temporary database creation
- **Memory Usage**: ~4-8GB during peak processing

#### Update Processing Performance
- **Batch Size**: Configurable (default: 100 records per batch)
- **Transaction Speed**: ~1000-5000 changes per minute
- **Rollback Time**: <5 seconds for complete transaction rollback

### Monitoring and Maintenance

#### Change Tracking Views
```sql
-- View recent changes across all tables
SELECT * FROM recent_changes_view LIMIT 100;

-- Summarize changes by update run
SELECT * FROM change_summary_by_run ORDER BY started_at DESC LIMIT 10;

-- Get entity change history
SELECT * FROM entity_change_history
WHERE entity_id = 'specific-entity-id'
ORDER BY changed_at DESC;
```

#### Maintenance Tasks
```sql
-- Clean up old audit records (older than 1 year)
DELETE FROM logies_changelog WHERE changed_at < NOW() - INTERVAL '1 year';
DELETE FROM addresses_changelog WHERE changed_at < NOW() - INTERVAL '1 year';
-- Repeat for other changelog tables

-- Archive completed update runs
DELETE FROM update_runs
WHERE status = 'COMPLETED' AND completed_at < NOW() - INTERVAL '6 months';
```

### Error Handling and Recovery

#### Common Scenarios
- **Network Issues**: Automatic retry with exponential backoff
- **Database Conflicts**: Full transaction rollback with detailed error reporting
- **Schema Mismatches**: Validation before processing with clear error messages
- **Constraint Violations**: Individual record skipping with continued processing

#### Recovery Procedures
```sql
-- Check for failed update runs
SELECT * FROM update_runs WHERE status = 'FAILED' ORDER BY started_at DESC;

-- Review specific run errors
SELECT * FROM change_summary_by_run WHERE status = 'FAILED';

-- Manual cleanup if needed
UPDATE update_runs SET status = 'CANCELLED' WHERE run_id = 'failed-run-id';
```

## Scripts and Components

### Core Files

#### 1. `corrected_tourism_schema.sql`
Complete PostgreSQL schema definition with:
- Table structure definitions
- Primary key and foreign key constraints
- Indexes for performance optimization
- Check constraints for data validation

#### 2. `corrected_ttl_to_postgres_ENHANCED.py`
Enhanced Python parser with key features:

**Key Components:**
- **`EnhancedTourismDataImporter`** - Main importer class
- **`detect_entity_type()`** - Correctly classifies RDF entities by type
- **`capture_relationships()`** - Maps entity relationships during parsing
- **`apply_entity_relationships()`** - Links related entities after import

**Entity Processing Methods:**
- `process_logies()` - Handles accommodation entities
- `process_address()` - Processes address information
- `process_contact_point()` - Manages contact details
- `process_geometry()` - Handles geographic data
- `process_identifier()` - Processes external identifiers

**Critical Bug Fixes:**
- Fixed entity classification priority to prevent misclassification
- Implemented proper relationship mapping between entities
- Added comprehensive entity type detection
- Enhanced error handling and validation

#### 3. `toeristische-attracties.ttl`
Source RDF/TTL data file (553MB) containing complete Flemish tourism dataset.

## Usage Examples

### Basic Import
```bash
# Import to default database
python3 corrected_ttl_to_postgres_ENHANCED.py \
    --ttl-file toeristische-attracties.ttl \
    --db-name tourism_db
```

### Custom Database Connection
```bash
# Import with custom connection parameters
python3 corrected_ttl_to_postgres_ENHANCED.py \
    --ttl-file toeristische-attracties.ttl \
    --db-name production_tourism \
    --db-user tourism_user \
    --db-password secure_password \
    --db-host localhost \
    --db-port 5432
```

### Test Environment Setup
```bash
# Create test database with sample data
createdb tourism_test
psql -d tourism_test -f corrected_tourism_schema.sql

# Import subset for testing
head -n 10000 toeristische-attracties.ttl > sample_data.ttl
python3 corrected_ttl_to_postgres_ENHANCED.py \
    --ttl-file sample_data.ttl \
    --db-name tourism_test
```

## Performance and Statistics

### Import Performance
- **Processing Speed**: ~50,000 triples per minute
- **Total Import Time**: ~45-60 minutes for full dataset
- **Memory Usage**: ~2-4GB during peak processing

### Data Statistics (Production)
- **Total Records**: 339,037 across all tables
- **Address Completeness**: 99.9% street-level data restored
- **Relationship Integrity**: 100% entity relationships mapped
- **Data Quality**: All constraint validations passing

## Troubleshooting

### Common Issues

#### 1. Database Connection Errors
```bash
# Check PostgreSQL is running
pg_isready

# Verify database exists
psql -l | grep tourism
```

#### 2. Import Failures
- **Constraint Violations**: Check for existing data conflicts
- **Memory Issues**: Increase system memory or process in chunks
- **Schema Mismatches**: Ensure schema is up to date

#### 3. Data Validation Issues
```sql
-- Check for missing relationships
SELECT COUNT(*) FROM addresses WHERE logies_id IS NULL;

-- Validate identifier relationships
SELECT COUNT(*) FROM identifiers
WHERE related_entity_id IS NULL AND related_entity_type IS NOT NULL;
```

## System Requirements

### Minimum Requirements
- **CPU**: 2+ cores
- **RAM**: 8GB minimum, 16GB recommended
- **Storage**: 10GB free space
- **PostgreSQL**: Version 15+
- **Python**: 3.8+

### Recommended Production Setup
- **CPU**: 4+ cores
- **RAM**: 32GB
- **Storage**: SSD with 50GB+ free space
- **PostgreSQL**: Latest stable version
- **Network**: High-speed connection for TTL download

## Maintenance

### Regular Tasks
1. **Database Vacuum**: Run weekly for performance
   ```sql
   VACUUM ANALYZE logies, addresses, contact_points, geometries, identifiers;
   ```

2. **Index Maintenance**: Monitor query performance
   ```sql
   SELECT schemaname, tablename, indexname, idx_tup_read, idx_tup_fetch
   FROM pg_stat_user_indexes;
   ```

3. **Data Freshness**: Check source TTL file for updates
   - Source URL: [Flemish Tourism Data Portal](https://data.vlaanderen.be)
   - Update Frequency: Typically monthly

### Backup Strategy
```bash
# Create full database backup
pg_dump tourism_flanders_corrected > tourism_backup_$(date +%Y%m%d).sql

# Restore from backup
psql -d tourism_flanders_corrected_new < tourism_backup_20250918.sql
```

---

*Last Updated: September 2025*
*Database Version: Enhanced Production v2.0*
*Total Records: 339,037*