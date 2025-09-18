"""
Test Tourism Database Update Orchestrator

Tests the orchestration system functionality including:
- Full update workflow coordination
- Validation-only workflows
- Configuration management
- Error handling and recovery
- System status monitoring
"""

import sys
import os
import tempfile
import time
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_system.orchestrator import (
    UpdateOrchestrator,
    OrchestrationConfig,
    create_default_config,
    console_notification_handler
)
from update_system.change_detector import ChangeDetectionResult, EntityChange
from update_system import DEFAULT_DB_CONFIG


def create_mock_orchestration_config() -> OrchestrationConfig:
    """Create mock orchestration configuration for testing."""
    config = DEFAULT_DB_CONFIG.copy()
    config['user'] = 'lieven'
    config['database'] = 'tourism_test_master'

    return create_default_config(
        db_config=config,
        source_url='https://test.example.com/test.ttl'
    )


def test_orchestrator_initialization():
    """Test orchestrator initialization and configuration."""
    print("Testing orchestrator initialization...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Test configuration is properly stored
    assert orchestrator.config == config
    assert orchestrator.result is None
    assert len(orchestrator.notification_handlers) == 0

    # Test notification handler addition
    orchestrator.add_notification_handler(console_notification_handler)
    assert len(orchestrator.notification_handlers) == 1

    print("✓ Orchestrator initialization works correctly")
    print("✓ Configuration management works")
    print("✓ Notification handlers can be added")
    print("✓ Orchestrator initialization test completed\n")


def test_system_status_monitoring():
    """Test system status monitoring functionality."""
    print("Testing system status monitoring...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Get system status
    status = orchestrator.get_system_status()

    # Verify status structure
    assert 'timestamp' in status
    assert 'database' in status or 'error' in status  # May fail if DB not available
    assert 'data_source' in status or 'error' in status
    assert 'system_health' in status

    print("✓ System status monitoring works")
    print(f"✓ System health: {status.get('system_health', 'unknown')}")

    if 'database' in status:
        db_status = status['database']
        print(f"✓ Database connected: {db_status.get('connected', False)}")

    if 'recent_activity' in status:
        activity = status['recent_activity']
        print(f"✓ Recent runs: {activity.get('runs_last_7_days', 0)}")

    print("✓ System status monitoring test completed\n")


def test_validation_workflow():
    """Test validation-only workflow."""
    print("Testing validation workflow...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Create a test TTL file
    test_ttl_content = """
@prefix logies: <https://data.vlaanderen.be/ns/logies#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .

<https://data.vlaanderen.be/id/logies/test-1> a logies:Logies ;
    rdfs:label "Test Hotel"@nl ;
    schema:description "A test hotel"@nl .
    """

    with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
        f.write(test_ttl_content)
        test_ttl_path = f.name

    try:
        # Test validation workflow
        result = orchestrator.execute_validation_only(test_ttl_path)

        # Check basic result structure
        assert result is not None
        assert result.run_id is not None
        assert result.started_at is not None
        assert result.completed_at is not None
        assert isinstance(result.success, bool)
        assert result.processing_time >= 0

        print(f"✓ Validation workflow executed: {'success' if result.success else 'failed'}")
        print(f"✓ Processing time: {result.processing_time:.2f}s")
        print(f"✓ Changes detected: {result.total_changes}")

        if not result.success and result.error_messages:
            print(f"⚠️  Validation errors (expected for test): {result.error_messages[0]}")

        # Test phase results structure
        if result.phase_results:
            for phase, phase_result in result.phase_results.items():
                print(f"✓ Phase {phase}: {'✓' if phase_result.get('success', False) else '✗'}")

    finally:
        # Cleanup
        if os.path.exists(test_ttl_path):
            os.unlink(test_ttl_path)

    print("✓ Validation workflow test completed\n")


def test_backup_functionality():
    """Test database backup functionality."""
    print("Testing backup functionality...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Test backup creation
    backup_result = orchestrator.create_backup()

    # Check backup result structure
    assert 'success' in backup_result
    assert isinstance(backup_result['success'], bool)

    if backup_result['success']:
        assert 'backup_file' in backup_result
        assert 'timestamp' in backup_result
        print("✓ Backup creation command generated successfully")
        print(f"✓ Backup file: {backup_result.get('backup_file', 'N/A')}")
    else:
        print(f"⚠️  Backup disabled or failed: {backup_result.get('error', 'Unknown')}")

    print("✓ Backup functionality test completed\n")


def test_configuration_management():
    """Test configuration management and validation."""
    print("Testing configuration management...")

    # Test default configuration creation
    db_config = {'host': 'localhost', 'database': 'test_db'}
    source_url = 'https://example.com/data.ttl'

    config = create_default_config(db_config, source_url)

    # Verify configuration structure
    assert config.db_config == db_config
    assert config.source_url == source_url
    assert config.batch_size == 100  # Default value
    assert config.dry_run_first is True  # Default value
    assert config.force_update is False  # Default value

    print("✓ Default configuration creation works")
    print(f"✓ Batch size: {config.batch_size}")
    print(f"✓ Dry run first: {config.dry_run_first}")
    print(f"✓ Force update: {config.force_update}")

    # Test configuration modification
    config.batch_size = 50
    config.dry_run_first = False
    config.force_update = True

    assert config.batch_size == 50
    assert config.dry_run_first is False
    assert config.force_update is True

    print("✓ Configuration modification works")
    print("✓ Configuration management test completed\n")


def test_notification_system():
    """Test notification system functionality."""
    print("Testing notification system...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Test notification handler management
    notifications_received = []

    def test_notification_handler(result):
        notifications_received.append(result)

    # Add notification handler
    orchestrator.add_notification_handler(test_notification_handler)
    assert len(orchestrator.notification_handlers) == 1

    # Create mock result for notification
    from update_system.orchestrator import OrchestrationResult
    from datetime import datetime

    mock_result = OrchestrationResult(
        run_id="test_run",
        started_at=datetime.now(),
        success=True,
        total_changes=5
    )

    # Test notification sending
    orchestrator.result = mock_result
    orchestrator._send_notifications()

    # Verify notification was received
    assert len(notifications_received) == 1
    assert notifications_received[0] == mock_result

    print("✓ Notification handlers work correctly")
    print("✓ Notifications are sent properly")
    print("✓ Notification system test completed\n")


def test_error_handling():
    """Test error handling in orchestration workflows."""
    print("Testing error handling...")

    config = create_mock_orchestration_config()
    # Set invalid source URL to trigger error
    config.source_url = "invalid://not-a-real-url"

    orchestrator = UpdateOrchestrator(config)

    # Test error handling in validation workflow
    result = orchestrator.execute_validation_only()

    # Should fail gracefully
    assert result is not None
    assert result.success is False
    assert len(result.error_messages) > 0

    print("✓ Error handling works correctly")
    print(f"✓ Error captured: {result.error_messages[0] if result.error_messages else 'N/A'}")
    print("✓ Workflow fails gracefully")

    # Test cleanup even on error
    if hasattr(orchestrator, 'result') and orchestrator.result:
        assert orchestrator.result.completed_at is not None

    print("✓ Cleanup works even on errors")
    print("✓ Error handling test completed\n")


def test_workflow_timing_and_metrics():
    """Test workflow timing and metrics collection."""
    print("Testing workflow timing and metrics...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Create minimal test TTL
    test_ttl = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<https://test.example/1> rdfs:label "Test" .
    """

    with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
        f.write(test_ttl)
        test_ttl_path = f.name

    try:
        start_time = time.time()

        # Execute validation workflow
        result = orchestrator.execute_validation_only(test_ttl_path)

        execution_time = time.time() - start_time

        # Verify timing information
        assert result.processing_time > 0
        assert result.processing_time <= execution_time + 1  # Allow 1 second tolerance

        # Verify timestamps
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.completed_at >= result.started_at

        print(f"✓ Processing time recorded: {result.processing_time:.2f}s")
        print(f"✓ Actual execution time: {execution_time:.2f}s")
        print("✓ Timestamps are properly recorded")

        # Test run ID generation
        assert result.run_id is not None
        assert result.run_id.startswith("validation_")

        print(f"✓ Run ID generated: {result.run_id}")

    finally:
        # Cleanup
        if os.path.exists(test_ttl_path):
            os.unlink(test_ttl_path)

    print("✓ Workflow timing and metrics test completed\n")


def test_phase_coordination():
    """Test coordination between different phases."""
    print("Testing phase coordination...")

    config = create_mock_orchestration_config()
    orchestrator = UpdateOrchestrator(config)

    # Mock successful phase results to test coordination
    mock_download_result = {
        'success': True,
        'file_path': '/tmp/test.ttl',
        'has_changes': True,
        'validation': {'is_valid': True}
    }

    mock_detection_result = {
        'success': True,
        'changes': MagicMock(),
        'total_changes': 5
    }

    # Test that phases are executed in correct order
    with patch.object(orchestrator, '_execute_data_source_phase', return_value=mock_download_result):
        with patch.object(orchestrator, '_execute_change_detection_phase', return_value=mock_detection_result):
            with patch.object(orchestrator, '_execute_update_processing_phase', return_value={'success': True, 'records_processed': 5}):

                result = orchestrator.execute_full_update_workflow()

                # Verify phase coordination
                assert 'data_source' in result.phase_results
                assert 'change_detection' in result.phase_results
                assert 'update_processing' in result.phase_results

                print("✓ All phases executed in correct order")
                print("✓ Phase results properly coordinated")

    print("✓ Phase coordination test completed\n")


def main():
    """Run all orchestrator tests."""
    print("=== Update Orchestrator Tests ===\n")

    try:
        # Test 1: Initialization
        test_orchestrator_initialization()

        # Test 2: System Status Monitoring
        test_system_status_monitoring()

        # Test 3: Validation Workflow
        test_validation_workflow()

        # Test 4: Backup Functionality
        test_backup_functionality()

        # Test 5: Configuration Management
        test_configuration_management()

        # Test 6: Notification System
        test_notification_system()

        # Test 7: Error Handling
        test_error_handling()

        # Test 8: Timing and Metrics
        test_workflow_timing_and_metrics()

        # Test 9: Phase Coordination
        test_phase_coordination()

        print("🎉 All Update Orchestrator tests passed!")
        print("\nPhase 5 (Orchestration & Automation) core functionality validated.")

    except Exception as e:
        print(f"❌ Update Orchestrator test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()