"""
Tourism Database Update System

A comprehensive system for tracking changes to the Flemish tourism dataset over time,
providing historical data tracking and incremental updates.

Modules:
    change_tracker: Database change tracking and audit logging
    data_source_manager: TTL file download and validation
    change_detector: Database comparison and change detection
    diff_generator: Differential TTL file generation
    update_processor: Incremental update processing
    update_orchestrator: Main orchestration controller
"""

__version__ = "1.0.0"
__author__ = "Tourism Database Team"

# Data source configuration
TOURISM_DATA_SOURCE = {
    "base_url": "https://linked.toerismevlaanderen.be",
    "dataset_page": "https://linked.toerismevlaanderen.be/datasets",
    "current_file_url": "https://linked.toerismevlaanderen.be/files/02a71541-9434-11f0-b486-e14b0db176db/download?name=toeristische-attracties.ttl",
    "file_name": "toeristische-attracties.ttl"
}

# Database configuration defaults
DEFAULT_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "",
    "production_db": "tourism_flanders_corrected",
    "temp_db_prefix": "tourism_temp_",
    "test_db": "tourism_test"
}

# Update system configuration
UPDATE_CONFIG = {
    "download_timeout": 3600,  # 1 hour
    "max_retries": 3,
    "batch_size": 1000,
    "temp_file_retention_days": 7,
    "changelog_retention_days": 365
}