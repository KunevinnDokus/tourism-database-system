"""
Tourism Database Monitoring Dashboard

Web-based dashboard for real-time monitoring and management
of the Tourism Database Update System.
"""

import os
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from flask import Flask, render_template, jsonify, request, redirect, url_for, flash
    from flask_socketio import SocketIO, emit
    import psycopg2
except ImportError:
    print("Required packages not installed. Run: pip install flask flask-socketio psycopg2-binary")
    sys.exit(1)

from update_system.advanced_monitor import AdvancedMonitor, create_default_monitor_config
from update_system.performance_optimizer import PerformanceOptimizer
from update_system.orchestrator import UpdateOrchestrator, create_default_config
from update_system import DEFAULT_DB_CONFIG

app = Flask(__name__)
app.config['SECRET_KEY'] = 'tourism-db-dashboard-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global instances
monitor = None
optimizer = None
orchestrator = None


def initialize_services():
    """Initialize monitoring and optimization services."""
    global monitor, optimizer, orchestrator

    try:
        # Initialize monitor
        monitor_config = create_default_monitor_config()
        monitor = AdvancedMonitor(DEFAULT_DB_CONFIG, monitor_config)
        monitor.start_monitoring(interval_seconds=30)

        # Initialize optimizer
        optimizer = PerformanceOptimizer(DEFAULT_DB_CONFIG)
        optimizer.start_background_tasks()

        # Initialize orchestrator
        orchestrator_config = create_default_config(
            DEFAULT_DB_CONFIG,
            "https://linked.toerismevlaanderen.be/files/02a71541-9434-11f0-b486-e14b0db176db/download?name=toeristische-attracties.ttl"
        )
        orchestrator = UpdateOrchestrator(orchestrator_config)

        print("Dashboard services initialized successfully")

    except Exception as e:
        print(f"Failed to initialize services: {e}")


@app.route('/')
def dashboard():
    """Main dashboard page."""
    return render_template('dashboard.html')


@app.route('/api/health')
def api_health():
    """Get system health summary."""
    try:
        if not monitor:
            return jsonify({'error': 'Monitor not initialized'}), 500

        health = monitor.get_system_health_summary()
        return jsonify(health)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics')
def api_metrics():
    """Get current system metrics."""
    try:
        if not monitor:
            return jsonify({'error': 'Monitor not initialized'}), 500

        current_metrics = monitor.get_current_metrics()
        if not current_metrics:
            return jsonify({'error': 'No metrics available'}), 404

        return jsonify({
            'timestamp': current_metrics.timestamp.isoformat(),
            'cpu_percent': current_metrics.cpu_percent,
            'memory_percent': current_metrics.memory_percent,
            'memory_available_gb': current_metrics.memory_available_gb,
            'disk_usage_percent': current_metrics.disk_usage_percent,
            'disk_free_gb': current_metrics.disk_free_gb,
            'database_connections': current_metrics.database_connections,
            'database_size_gb': current_metrics.database_size_gb,
            'active_update_runs': current_metrics.active_update_runs,
            'error_rate_24h': current_metrics.error_rate_24h
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/history')
def api_metrics_history():
    """Get metrics history."""
    try:
        hours = request.args.get('hours', 1, type=int)

        if not monitor:
            return jsonify({'error': 'Monitor not initialized'}), 500

        history = monitor.get_metrics_history(hours)

        return jsonify([
            {
                'timestamp': m.timestamp.isoformat(),
                'cpu_percent': m.cpu_percent,
                'memory_percent': m.memory_percent,
                'disk_usage_percent': m.disk_usage_percent,
                'database_connections': m.database_connections,
                'error_rate_24h': m.error_rate_24h
            }
            for m in history
        ])

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts')
def api_alerts():
    """Get active alerts."""
    try:
        if not monitor:
            return jsonify({'error': 'Monitor not initialized'}), 500

        alerts = monitor.get_active_alerts()

        return jsonify([
            {
                'alert_id': alert.alert_id,
                'severity': alert.severity,
                'component': alert.component,
                'message': alert.message,
                'value': alert.value,
                'threshold': alert.threshold,
                'timestamp': alert.timestamp.isoformat()
            }
            for alert in alerts
        ])

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/performance')
def api_performance():
    """Get performance optimization summary."""
    try:
        if not optimizer:
            return jsonify({'error': 'Optimizer not initialized'}), 500

        summary = optimizer.get_performance_summary()
        return jsonify(summary)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/performance/optimize', methods=['POST'])
def api_optimize():
    """Run database optimization analysis."""
    try:
        if not optimizer:
            return jsonify({'error': 'Optimizer not initialized'}), 500

        result = optimizer.optimize_database()
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/updates/status')
def api_update_status():
    """Get update system status."""
    try:
        if not orchestrator:
            return jsonify({'error': 'Orchestrator not initialized'}), 500

        status = orchestrator.get_system_status()
        return jsonify(status)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/updates/history')
def api_update_history():
    """Get update run history."""
    try:
        limit = request.args.get('limit', 20, type=int)

        with psycopg2.connect(**DEFAULT_DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_id, started_at, completed_at, status,
                           total_changes, error_message
                    FROM update_runs
                    ORDER BY started_at DESC
                    LIMIT %s
                """, (limit,))

                runs = []
                for row in cur.fetchall():
                    runs.append({
                        'run_id': row[0],
                        'started_at': row[1].isoformat() if row[1] else None,
                        'completed_at': row[2].isoformat() if row[2] else None,
                        'status': row[3],
                        'total_changes': row[4],
                        'error_message': row[5]
                    })

                return jsonify(runs)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/updates/run', methods=['POST'])
def api_run_update():
    """Trigger manual update run."""
    try:
        if not orchestrator:
            return jsonify({'error': 'Orchestrator not initialized'}), 500

        # Run in background and return immediately
        import threading

        def run_update():
            try:
                result = orchestrator.execute_full_update_workflow()
                socketio.emit('update_completed', {
                    'success': result.success,
                    'run_id': result.run_id,
                    'total_changes': result.total_changes
                })
            except Exception as e:
                socketio.emit('update_error', {'error': str(e)})

        thread = threading.Thread(target=run_update, daemon=True)
        thread.start()

        return jsonify({'message': 'Update started', 'status': 'running'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/database/stats')
def api_database_stats():
    """Get database statistics."""
    try:
        with psycopg2.connect(**DEFAULT_DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Table sizes
                cur.execute("""
                    SELECT schemaname, tablename,
                           pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                           pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                    FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY size_bytes DESC
                    LIMIT 10
                """)

                table_sizes = [
                    {
                        'table': f"{row[0]}.{row[1]}",
                        'size': row[2],
                        'size_bytes': row[3]
                    }
                    for row in cur.fetchall()
                ]

                # Row counts
                cur.execute("""
                    SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public'
                    ORDER BY n_live_tup DESC
                    LIMIT 10
                """)

                row_counts = [
                    {
                        'table': f"{row[0]}.{row[1]}",
                        'inserts': row[2],
                        'updates': row[3],
                        'deletes': row[4],
                        'live_rows': row[5]
                    }
                    for row in cur.fetchall()
                ]

                return jsonify({
                    'table_sizes': table_sizes,
                    'row_counts': row_counts
                })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    print('Client connected')
    emit('connected', {'message': 'Connected to Tourism DB Dashboard'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    print('Client disconnected')


def emit_real_time_updates():
    """Emit real-time updates to connected clients."""
    import threading
    import time

    def update_loop():
        while True:
            try:
                if monitor:
                    health = monitor.get_system_health_summary()
                    socketio.emit('health_update', health)

                time.sleep(30)  # Update every 30 seconds

            except Exception as e:
                print(f"Error in real-time update loop: {e}")
                time.sleep(60)

    thread = threading.Thread(target=update_loop, daemon=True)
    thread.start()


if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)

    # Initialize services
    initialize_services()

    # Start real-time updates
    emit_real_time_updates()

    print("Starting Tourism Database Dashboard...")
    print("Access the dashboard at: http://localhost:5000")

    # Run the app
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)