"""
Advanced Monitoring and Alerting System

Enterprise-grade monitoring for the Tourism Database Update System.
Provides real-time health monitoring, performance metrics, and intelligent alerting.
"""

import os
import json
import time
import psutil
import logging
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot."""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_available_gb: float
    disk_usage_percent: float
    disk_free_gb: float
    database_connections: int
    database_size_gb: float
    active_update_runs: int
    avg_update_duration_minutes: float
    last_successful_update: Optional[datetime]
    error_rate_24h: float


@dataclass
class AlertThresholds:
    """Configurable alert thresholds."""
    cpu_critical: float = 90.0
    cpu_warning: float = 75.0
    memory_critical: float = 90.0
    memory_warning: float = 80.0
    disk_critical: float = 95.0
    disk_warning: float = 85.0
    database_connections_warning: int = 80
    database_connections_critical: int = 95
    error_rate_warning: float = 5.0
    error_rate_critical: float = 10.0
    update_failure_hours: int = 24


@dataclass
class Alert:
    """System alert with metadata."""
    alert_id: str
    severity: str  # 'critical', 'warning', 'info'
    component: str
    message: str
    value: float
    threshold: float
    timestamp: datetime
    resolved: bool = False
    resolution_timestamp: Optional[datetime] = None


class NotificationHandler:
    """Handles various notification channels."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.email_config = config.get('email', {})
        self.webhook_config = config.get('webhook', {})
        self.slack_config = config.get('slack', {})

    def send_email_alert(self, alert: Alert) -> bool:
        """Send email notification for alert."""
        if not self.email_config.get('enabled', False):
            return False

        try:
            smtp_server = self.email_config['smtp_server']
            smtp_port = self.email_config.get('smtp_port', 587)
            username = self.email_config['username']
            password = self.email_config['password']
            to_addresses = self.email_config['to_addresses']

            msg = MIMEMultipart()
            msg['From'] = username
            msg['To'] = ', '.join(to_addresses)
            msg['Subject'] = f"[{alert.severity.upper()}] Tourism DB: {alert.component}"

            body = f"""
Tourism Database Alert

Severity: {alert.severity.upper()}
Component: {alert.component}
Message: {alert.message}
Current Value: {alert.value}
Threshold: {alert.threshold}
Time: {alert.timestamp.isoformat()}
Alert ID: {alert.alert_id}

This is an automated alert from the Tourism Database Monitoring System.
            """

            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)

            logger.info(f"Email alert sent for {alert.alert_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def send_webhook_alert(self, alert: Alert) -> bool:
        """Send webhook notification."""
        if not self.webhook_config.get('enabled', False):
            return False

        try:
            import requests

            webhook_url = self.webhook_config['url']
            payload = {
                'alert_id': alert.alert_id,
                'severity': alert.severity,
                'component': alert.component,
                'message': alert.message,
                'value': alert.value,
                'threshold': alert.threshold,
                'timestamp': alert.timestamp.isoformat(),
                'source': 'tourism_database_monitor'
            }

            headers = self.webhook_config.get('headers', {})

            response = requests.post(
                webhook_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()

            logger.info(f"Webhook alert sent for {alert.alert_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
            return False

    def send_alert(self, alert: Alert) -> bool:
        """Send alert via all configured channels."""
        success = True

        if self.email_config.get('enabled', False):
            success &= self.send_email_alert(alert)

        if self.webhook_config.get('enabled', False):
            success &= self.send_webhook_alert(alert)

        return success


class AdvancedMonitor:
    """Advanced system monitoring with intelligent alerting."""

    def __init__(self, db_config: Dict[str, Any], monitor_config: Dict[str, Any]):
        self.db_config = db_config
        self.monitor_config = monitor_config
        self.thresholds = AlertThresholds(**monitor_config.get('thresholds', {}))
        self.notification_handler = NotificationHandler(monitor_config.get('notifications', {}))

        # State management
        self.active_alerts: Dict[str, Alert] = {}
        self.metrics_history: List[PerformanceMetrics] = []
        self.monitoring = False
        self.monitor_thread = None

        # Alert cooldowns to prevent spam
        self.alert_cooldowns: Dict[str, datetime] = {}
        self.cooldown_minutes = monitor_config.get('alert_cooldown_minutes', 15)

        # Metrics retention
        self.max_history_hours = monitor_config.get('metrics_retention_hours', 24)

    def start_monitoring(self, interval_seconds: int = 60):
        """Start continuous monitoring in background thread."""
        if self.monitoring:
            logger.warning("Monitoring already running")
            return

        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"Started monitoring with {interval_seconds}s interval")

    def stop_monitoring(self):
        """Stop monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        logger.info("Monitoring stopped")

    def _monitor_loop(self, interval_seconds: int):
        """Main monitoring loop."""
        while self.monitoring:
            try:
                metrics = self.collect_metrics()
                if metrics:
                    self.metrics_history.append(metrics)
                    self._cleanup_old_metrics()
                    self._evaluate_alerts(metrics)

                time.sleep(interval_seconds)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(interval_seconds)

    def collect_metrics(self) -> Optional[PerformanceMetrics]:
        """Collect current system performance metrics."""
        try:
            # System metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Database metrics
            db_metrics = self._get_database_metrics()

            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                memory_available_gb=memory.available / (1024**3),
                disk_usage_percent=disk.percent,
                disk_free_gb=disk.free / (1024**3),
                database_connections=db_metrics.get('connections', 0),
                database_size_gb=db_metrics.get('size_gb', 0),
                active_update_runs=db_metrics.get('active_runs', 0),
                avg_update_duration_minutes=db_metrics.get('avg_duration_minutes', 0),
                last_successful_update=db_metrics.get('last_successful_update'),
                error_rate_24h=db_metrics.get('error_rate_24h', 0)
            )

            return metrics

        except Exception as e:
            logger.error(f"Failed to collect metrics: {e}")
            return None

    def _get_database_metrics(self) -> Dict[str, Any]:
        """Get database-specific metrics."""
        try:
            import psycopg2

            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    metrics = {}

                    # Connection count
                    cur.execute("""
                        SELECT COUNT(*) FROM pg_stat_activity
                        WHERE datname = %s AND state = 'active'
                    """, (self.db_config['database'],))
                    metrics['connections'] = cur.fetchone()[0]

                    # Database size
                    cur.execute("""
                        SELECT pg_size_pretty(pg_database_size(%s))::text,
                               pg_database_size(%s)::bigint
                    """, (self.db_config['database'], self.db_config['database']))
                    size_text, size_bytes = cur.fetchone()
                    metrics['size_gb'] = size_bytes / (1024**3)

                    # Active update runs
                    cur.execute("""
                        SELECT COUNT(*) FROM update_runs
                        WHERE status = 'RUNNING'
                    """)
                    metrics['active_runs'] = cur.fetchone()[0]

                    # Average duration of recent runs
                    cur.execute("""
                        SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))/60)
                        FROM update_runs
                        WHERE completed_at IS NOT NULL
                        AND started_at > NOW() - INTERVAL '7 days'
                    """)
                    result = cur.fetchone()[0]
                    metrics['avg_duration_minutes'] = float(result) if result else 0

                    # Last successful update
                    cur.execute("""
                        SELECT MAX(completed_at) FROM update_runs
                        WHERE status = 'COMPLETED'
                    """)
                    result = cur.fetchone()[0]
                    metrics['last_successful_update'] = result

                    # Error rate (24h)
                    cur.execute("""
                        SELECT
                            COUNT(*) FILTER (WHERE status = 'FAILED') * 100.0 /
                            NULLIF(COUNT(*), 0) as error_rate
                        FROM update_runs
                        WHERE started_at > NOW() - INTERVAL '24 hours'
                    """)
                    result = cur.fetchone()[0]
                    metrics['error_rate_24h'] = float(result) if result else 0

                    return metrics

        except Exception as e:
            logger.error(f"Failed to get database metrics: {e}")
            return {}

    def _evaluate_alerts(self, metrics: PerformanceMetrics):
        """Evaluate metrics against thresholds and generate alerts."""
        current_time = datetime.now()

        # CPU alerts
        self._check_threshold_alert(
            'cpu_usage', 'CPU Usage', metrics.cpu_percent,
            self.thresholds.cpu_warning, self.thresholds.cpu_critical,
            current_time, '%'
        )

        # Memory alerts
        self._check_threshold_alert(
            'memory_usage', 'Memory Usage', metrics.memory_percent,
            self.thresholds.memory_warning, self.thresholds.memory_critical,
            current_time, '%'
        )

        # Disk alerts
        self._check_threshold_alert(
            'disk_usage', 'Disk Usage', metrics.disk_usage_percent,
            self.thresholds.disk_warning, self.thresholds.disk_critical,
            current_time, '%'
        )

        # Database connection alerts
        self._check_threshold_alert(
            'db_connections', 'Database Connections', metrics.database_connections,
            self.thresholds.database_connections_warning,
            self.thresholds.database_connections_critical,
            current_time, 'connections'
        )

        # Error rate alerts
        self._check_threshold_alert(
            'error_rate', 'Error Rate (24h)', metrics.error_rate_24h,
            self.thresholds.error_rate_warning, self.thresholds.error_rate_critical,
            current_time, '%'
        )

        # Update failure alert
        if (metrics.last_successful_update and
            current_time - metrics.last_successful_update >
            timedelta(hours=self.thresholds.update_failure_hours)):

            hours_since = (current_time - metrics.last_successful_update).total_seconds() / 3600
            self._create_alert(
                'update_failure', 'critical', 'Update System',
                f"No successful update in {hours_since:.1f} hours",
                hours_since, self.thresholds.update_failure_hours, current_time
            )

    def _check_threshold_alert(self, alert_type: str, component: str, value: float,
                              warning_threshold: float, critical_threshold: float,
                              timestamp: datetime, unit: str):
        """Check value against thresholds and create alerts."""
        severity = None
        threshold = None

        if value >= critical_threshold:
            severity = 'critical'
            threshold = critical_threshold
        elif value >= warning_threshold:
            severity = 'warning'
            threshold = warning_threshold

        if severity:
            message = f"{component} is {value:.1f}{unit} (threshold: {threshold:.1f}{unit})"
            self._create_alert(alert_type, severity, component, message, value, threshold, timestamp)
        else:
            # Check if we need to resolve an existing alert
            self._resolve_alert(alert_type)

    def _create_alert(self, alert_type: str, severity: str, component: str,
                     message: str, value: float, threshold: float, timestamp: datetime):
        """Create new alert if not in cooldown."""
        alert_key = f"{alert_type}_{severity}"

        # Check cooldown
        if alert_key in self.alert_cooldowns:
            if timestamp - self.alert_cooldowns[alert_key] < timedelta(minutes=self.cooldown_minutes):
                return

        # Create alert
        alert = Alert(
            alert_id=f"{alert_key}_{int(timestamp.timestamp())}",
            severity=severity,
            component=component,
            message=message,
            value=value,
            threshold=threshold,
            timestamp=timestamp
        )

        self.active_alerts[alert_key] = alert
        self.alert_cooldowns[alert_key] = timestamp

        # Send notification
        self.notification_handler.send_alert(alert)

        logger.warning(f"Alert created: {alert.alert_id} - {message}")

    def _resolve_alert(self, alert_type: str):
        """Resolve alert if it exists."""
        for severity in ['warning', 'critical']:
            alert_key = f"{alert_type}_{severity}"
            if alert_key in self.active_alerts:
                alert = self.active_alerts[alert_key]
                alert.resolved = True
                alert.resolution_timestamp = datetime.now()
                del self.active_alerts[alert_key]
                logger.info(f"Alert resolved: {alert.alert_id}")

    def _cleanup_old_metrics(self):
        """Remove old metrics to prevent memory growth."""
        cutoff_time = datetime.now() - timedelta(hours=self.max_history_hours)
        self.metrics_history = [
            m for m in self.metrics_history
            if m.timestamp > cutoff_time
        ]

    def get_current_metrics(self) -> Optional[PerformanceMetrics]:
        """Get most recent metrics."""
        return self.metrics_history[-1] if self.metrics_history else None

    def get_metrics_history(self, hours: int = 1) -> List[PerformanceMetrics]:
        """Get metrics history for specified hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [m for m in self.metrics_history if m.timestamp > cutoff_time]

    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return list(self.active_alerts.values())

    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive system health summary."""
        current_metrics = self.get_current_metrics()
        if not current_metrics:
            return {'status': 'unknown', 'message': 'No metrics available'}

        active_alerts = self.get_active_alerts()
        critical_alerts = [a for a in active_alerts if a.severity == 'critical']
        warning_alerts = [a for a in active_alerts if a.severity == 'warning']

        # Determine overall health
        if critical_alerts:
            status = 'critical'
            message = f"{len(critical_alerts)} critical issue(s) detected"
        elif warning_alerts:
            status = 'warning'
            message = f"{len(warning_alerts)} warning(s) active"
        else:
            status = 'healthy'
            message = 'All systems operating normally'

        return {
            'status': status,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'metrics': asdict(current_metrics),
            'active_alerts': len(active_alerts),
            'critical_alerts': len(critical_alerts),
            'warning_alerts': len(warning_alerts),
            'alerts': [asdict(alert) for alert in active_alerts]
        }


def create_default_monitor_config() -> Dict[str, Any]:
    """Create default monitoring configuration."""
    return {
        'thresholds': {
            'cpu_critical': 90.0,
            'cpu_warning': 75.0,
            'memory_critical': 90.0,
            'memory_warning': 80.0,
            'disk_critical': 95.0,
            'disk_warning': 85.0,
            'database_connections_warning': 80,
            'database_connections_critical': 95,
            'error_rate_warning': 5.0,
            'error_rate_critical': 10.0,
            'update_failure_hours': 24
        },
        'notifications': {
            'email': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'username': '',
                'password': '',
                'to_addresses': []
            },
            'webhook': {
                'enabled': False,
                'url': '',
                'headers': {}
            }
        },
        'alert_cooldown_minutes': 15,
        'metrics_retention_hours': 24
    }