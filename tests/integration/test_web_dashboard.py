"""
Integration Tests for Web Dashboard
===================================

Tests for web dashboard API endpoints and functionality.
"""

import unittest
import json
import sys
import os
from unittest.mock import patch, Mock, MagicMock

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Mock the dependencies before importing the app
sys.modules['psycopg2'] = MagicMock()
sys.modules['flask_socketio'] = MagicMock()
sys.modules['update_system.advanced_monitor'] = MagicMock()
sys.modules['update_system.performance_optimizer'] = MagicMock()
sys.modules['update_system.orchestrator'] = MagicMock()
sys.modules['update_system'] = MagicMock()

from web_dashboard.app import app


class TestWebDashboardAPI(unittest.TestCase):
    """Test web dashboard API endpoints."""

    def setUp(self):
        """Set up test client."""
        app.config['TESTING'] = True
        self.client = app.test_client()

        # Mock the global instances
        self.mock_monitor = Mock()
        self.mock_optimizer = Mock()
        self.mock_orchestrator = Mock()

    def test_dashboard_route(self):
        """Test main dashboard route."""
        with patch('web_dashboard.app.render_template') as mock_render:
            mock_render.return_value = 'Dashboard HTML'

            response = self.client.get('/')
            self.assertEqual(response.status_code, 200)
            mock_render.assert_called_once_with('dashboard.html')

    @patch('web_dashboard.app.monitor')
    def test_health_api(self, mock_monitor):
        """Test health API endpoint."""
        mock_monitor.get_system_health_summary.return_value = {
            'status': 'healthy',
            'cpu_usage': 25.5,
            'memory_usage': 60.2
        }

        response = self.client.get('/api/health')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['status'], 'healthy')
        self.assertEqual(data['cpu_usage'], 25.5)

    @patch('web_dashboard.app.monitor')
    def test_health_api_no_monitor(self, mock_monitor):
        """Test health API when monitor is not initialized."""
        mock_monitor = None

        with patch('web_dashboard.app.monitor', None):
            response = self.client.get('/api/health')
            self.assertEqual(response.status_code, 500)

            data = json.loads(response.data)
            self.assertIn('error', data)

    @patch('web_dashboard.app.monitor')
    def test_metrics_api(self, mock_monitor):
        """Test metrics API endpoint."""
        mock_metrics = Mock()
        mock_metrics.timestamp.isoformat.return_value = '2023-01-01T00:00:00'
        mock_metrics.cpu_percent = 15.5
        mock_metrics.memory_percent = 45.2
        mock_metrics.database_connections = 5
        mock_metrics.active_update_runs = 1

        mock_monitor.get_current_metrics.return_value = mock_metrics

        response = self.client.get('/api/metrics')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['cpu_percent'], 15.5)
        self.assertEqual(data['memory_percent'], 45.2)
        self.assertEqual(data['database_connections'], 5)

    @patch('web_dashboard.app.monitor')
    def test_metrics_history_api(self, mock_monitor):
        """Test metrics history API endpoint."""
        mock_metric1 = Mock()
        mock_metric1.timestamp.isoformat.return_value = '2023-01-01T00:00:00'
        mock_metric1.cpu_percent = 10.0
        mock_metric1.memory_percent = 40.0

        mock_metric2 = Mock()
        mock_metric2.timestamp.isoformat.return_value = '2023-01-01T01:00:00'
        mock_metric2.cpu_percent = 15.0
        mock_metric2.memory_percent = 45.0

        mock_monitor.get_metrics_history.return_value = [mock_metric1, mock_metric2]

        response = self.client.get('/api/metrics/history?hours=2')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['cpu_percent'], 10.0)
        self.assertEqual(data[1]['cpu_percent'], 15.0)

    @patch('web_dashboard.app.monitor')
    def test_alerts_api(self, mock_monitor):
        """Test alerts API endpoint."""
        mock_alert = Mock()
        mock_alert.alert_id = 'alert-123'
        mock_alert.severity = 'WARNING'
        mock_alert.component = 'CPU'
        mock_alert.message = 'High CPU usage'
        mock_alert.value = 85.5
        mock_alert.threshold = 80.0
        mock_alert.timestamp.isoformat.return_value = '2023-01-01T00:00:00'

        mock_monitor.get_active_alerts.return_value = [mock_alert]

        response = self.client.get('/api/alerts')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['alert_id'], 'alert-123')
        self.assertEqual(data[0]['severity'], 'WARNING')

    @patch('web_dashboard.app.optimizer')
    def test_performance_api(self, mock_optimizer):
        """Test performance API endpoint."""
        mock_optimizer.get_performance_summary.return_value = {
            'database_size_gb': 2.5,
            'query_performance': 'good',
            'optimization_recommendations': 3
        }

        response = self.client.get('/api/performance')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['database_size_gb'], 2.5)
        self.assertEqual(data['optimization_recommendations'], 3)

    @patch('web_dashboard.app.optimizer')
    def test_optimize_api(self, mock_optimizer):
        """Test optimization API endpoint."""
        mock_optimizer.optimize_database.return_value = {
            'optimizations_applied': 2,
            'performance_improvement': '15%',
            'recommendations': ['Index on name field', 'Vacuum tables']
        }

        response = self.client.post('/api/performance/optimize')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['optimizations_applied'], 2)
        self.assertIn('Index on name field', data['recommendations'])

    @patch('web_dashboard.app.orchestrator')
    def test_update_status_api(self, mock_orchestrator):
        """Test update status API endpoint."""
        mock_orchestrator.get_system_status.return_value = {
            'status': 'ready',
            'last_update': '2023-01-01T00:00:00',
            'pending_updates': 0
        }

        response = self.client.get('/api/updates/status')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['status'], 'ready')
        self.assertEqual(data['pending_updates'], 0)

    @patch('web_dashboard.app.psycopg2')
    def test_update_history_api(self, mock_psycopg2):
        """Test update history API endpoint."""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ('run-123', '2023-01-01T00:00:00', '2023-01-01T01:00:00', 'COMPLETED', 100, None),
            ('run-456', '2023-01-01T02:00:00', '2023-01-01T03:00:00', 'COMPLETED', 50, None)
        ]

        mock_conn = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2.connect.return_value.__enter__.return_value = mock_conn

        response = self.client.get('/api/updates/history')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['run_id'], 'run-123')
        self.assertEqual(data[0]['status'], 'COMPLETED')

    @patch('web_dashboard.app.psycopg2')
    def test_database_stats_api(self, mock_psycopg2):
        """Test database statistics API endpoint."""
        mock_cursor = Mock()

        # Mock table sizes query
        mock_cursor.fetchall.side_effect = [
            # Table sizes
            [('public', 'logies', '100 MB', 104857600),
             ('public', 'tourist_attractions', '50 MB', 52428800)],
            # Row counts
            [('public', 'logies', 1000, 100, 10, 990),
             ('public', 'tourist_attractions', 500, 50, 5, 495)],
        ]

        # Mock entity counts query
        mock_cursor.fetchone.return_value = (
            31347, 539, 45000, 35000, 30000, 60000, 55000, 58000, 8000, 7500, 7800
        )

        mock_conn = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2.connect.return_value.__enter__.return_value = mock_conn

        response = self.client.get('/api/database/stats')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertIn('table_sizes', data)
        self.assertIn('row_counts', data)
        self.assertIn('entity_counts', data)
        self.assertEqual(data['entity_counts']['logies'], 31347)
        self.assertEqual(data['entity_counts']['tourist_attractions'], 539)

    @patch('web_dashboard.app.psycopg2')
    def test_tourist_attractions_stats_api(self, mock_psycopg2):
        """Test tourist attractions statistics API endpoint."""
        mock_cursor = Mock()

        # Mock attraction stats query
        mock_cursor.fetchall.side_effect = [
            # Categories
            [('Museum', 150), ('Park', 100), ('Monument', 75)],
        ]

        # Mock main stats and relationships queries
        mock_cursor.fetchone.side_effect = [
            # Main stats
            (539, 400, 450, 500),
            # Relationships
            (800, 600, 750, 200)
        ]

        mock_conn = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2.connect.return_value.__enter__.return_value = mock_conn

        response = self.client.get('/api/tourist_attractions/stats')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['total_attractions'], 539)
        self.assertEqual(data['with_alternative_names'], 400)
        self.assertEqual(len(data['top_categories']), 3)
        self.assertEqual(data['top_categories'][0]['category'], 'Museum')
        self.assertEqual(data['relationships']['address_links'], 800)

    @patch('web_dashboard.app.orchestrator')
    @patch('web_dashboard.app.threading')
    @patch('web_dashboard.app.socketio')
    def test_run_update_api(self, mock_socketio, mock_threading, mock_orchestrator):
        """Test manual update run API endpoint."""
        # Mock update result
        mock_result = Mock()
        mock_result.success = True
        mock_result.run_id = 'run-789'
        mock_result.total_changes = 150

        mock_orchestrator.execute_full_update_workflow.return_value = mock_result

        # Mock thread creation
        mock_thread = Mock()
        mock_threading.Thread.return_value = mock_thread

        response = self.client.post('/api/updates/run')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertEqual(data['message'], 'Update started')
        self.assertEqual(data['status'], 'running')

        # Verify thread was started
        mock_thread.start.assert_called_once()


class TestWebDashboardErrorHandling(unittest.TestCase):
    """Test error handling in web dashboard."""

    def setUp(self):
        """Set up test client."""
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_api_error_handling(self):
        """Test that API endpoints handle errors gracefully."""
        # Test with monitor not initialized
        with patch('web_dashboard.app.monitor', None):
            response = self.client.get('/api/health')
            self.assertEqual(response.status_code, 500)

            data = json.loads(response.data)
            self.assertIn('error', data)
            self.assertEqual(data['error'], 'Monitor not initialized')

    @patch('web_dashboard.app.psycopg2')
    def test_database_error_handling(self, mock_psycopg2):
        """Test database error handling."""
        # Mock database connection error
        mock_psycopg2.connect.side_effect = Exception("Database connection failed")

        response = self.client.get('/api/database/stats')
        self.assertEqual(response.status_code, 500)

        data = json.loads(response.data)
        self.assertIn('error', data)


class TestWebDashboardIntegration(unittest.TestCase):
    """Test integration scenarios for web dashboard."""

    def setUp(self):
        """Set up test environment."""
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_real_time_update_flow(self):
        """Test real-time update notification flow."""
        with patch('web_dashboard.app.socketio') as mock_socketio:
            with patch('web_dashboard.app.orchestrator') as mock_orchestrator:
                with patch('web_dashboard.app.threading') as mock_threading:

                    # Mock successful update
                    mock_result = Mock()
                    mock_result.success = True
                    mock_result.run_id = 'test-run'
                    mock_result.total_changes = 100

                    mock_orchestrator.execute_full_update_workflow.return_value = mock_result

                    # Trigger update
                    response = self.client.post('/api/updates/run')
                    self.assertEqual(response.status_code, 200)

                    # Verify thread was created to run update in background
                    mock_threading.Thread.assert_called_once()

    def test_monitoring_data_consistency(self):
        """Test that monitoring data is consistent across endpoints."""
        with patch('web_dashboard.app.monitor') as mock_monitor:
            # Mock consistent metrics
            mock_metrics = Mock()
            mock_metrics.timestamp.isoformat.return_value = '2023-01-01T00:00:00'
            mock_metrics.cpu_percent = 25.0
            mock_metrics.memory_percent = 60.0
            mock_metrics.database_connections = 5

            mock_monitor.get_current_metrics.return_value = mock_metrics

            # Mock health summary with consistent data
            mock_monitor.get_system_health_summary.return_value = {
                'cpu_usage': 25.0,
                'memory_usage': 60.0,
                'database_connections': 5,
                'status': 'healthy'
            }

            # Get data from both endpoints
            metrics_response = self.client.get('/api/metrics')
            health_response = self.client.get('/api/health')

            metrics_data = json.loads(metrics_response.data)
            health_data = json.loads(health_response.data)

            # Verify consistency
            self.assertEqual(metrics_data['cpu_percent'], health_data['cpu_usage'])
            self.assertEqual(metrics_data['memory_percent'], health_data['memory_usage'])
            self.assertEqual(metrics_data['database_connections'], health_data['database_connections'])


if __name__ == '__main__':
    unittest.main()