"""
Tourism Database Update System Monitoring

Provides comprehensive monitoring, alerting, and health checking capabilities
for the tourism database update system.
"""

import os
import json
import psutil
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .change_tracker import ChangeTracker
from .data_source_manager import DataSourceManager

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckResult:
    """Result of a system health check."""
    component: str
    healthy: bool
    message: str
    details: Dict[str, Any] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.details is None:
            self.details = {}


@dataclass
class SystemMetrics:
    """System performance metrics."""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_usage_percent: float
    database_connections: int
    database_size_mb: float
    recent_errors: int
    recent_changes: int
    last_successful_update: Optional[datetime] = None


@dataclass
class AlertConfig:
    """Configuration for system alerts."""
    name: str
    enabled: bool = True
    threshold_type: str = "greater_than"  # greater_than, less_than, equals
    threshold_value: float = 0
    check_interval_minutes: int = 5
    alert_cooldown_minutes: int = 60
    severity: str = "warning"  # info, warning, error, critical


class SystemMonitor:
    """Comprehensive system monitoring for the tourism database update system."""

    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize the system monitor.

        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.health_checks: List[Callable[[], HealthCheckResult]] = []
        self.alert_configs: Dict[str, AlertConfig] = {}
        self.alert_handlers: List[Callable[[str, AlertConfig, Any], None]] = []
        self.last_alert_times: Dict[str, datetime] = {}
        self.metrics_history: List[SystemMetrics] = []

        # Register default health checks
        self._register_default_health_checks()
        self._register_default_alerts()

    def _register_default_health_checks(self):
        """Register default health check functions."""
        self.health_checks.extend([
            self._check_database_connectivity,
            self._check_database_size,
            self._check_data_source_availability,
            self._check_system_resources,
            self._check_recent_update_status,
            self._check_error_rates,
            self._check_disk_space,
            self._check_log_files
        ])

    def _register_default_alerts(self):
        """Register default alert configurations."""
        self.alert_configs.update({
            "high_cpu": AlertConfig(
                name="High CPU Usage",
                threshold_type="greater_than",
                threshold_value=80.0,
                check_interval_minutes=5,
                severity="warning"
            ),
            "high_memory": AlertConfig(
                name="High Memory Usage",
                threshold_type="greater_than",
                threshold_value=90.0,
                check_interval_minutes=5,
                severity="warning"
            ),
            "low_disk": AlertConfig(
                name="Low Disk Space",
                threshold_type="greater_than",
                threshold_value=90.0,
                check_interval_minutes=15,
                severity="error"
            ),
            "database_connection_failed": AlertConfig(
                name="Database Connection Failed",
                threshold_type="equals",
                threshold_value=0,  # 0 = failed, 1 = success
                check_interval_minutes=2,
                severity="critical"
            ),
            "data_source_unavailable": AlertConfig(
                name="Data Source Unavailable",
                threshold_type="equals",
                threshold_value=0,  # 0 = unavailable, 1 = available
                check_interval_minutes=30,
                severity="error"
            ),
            "no_recent_updates": AlertConfig(
                name="No Recent Updates",
                threshold_type="greater_than",
                threshold_value=7,  # days since last successful update
                check_interval_minutes=60,
                severity="warning"
            )
        })

    def add_health_check(self, check_func: Callable[[], HealthCheckResult]):
        """Add a custom health check function."""
        self.health_checks.append(check_func)

    def add_alert_config(self, config: AlertConfig):
        """Add an alert configuration."""
        self.alert_configs[config.name.lower().replace(" ", "_")] = config

    def add_alert_handler(self, handler: Callable[[str, AlertConfig, Any], None]):
        """Add an alert handler function."""
        self.alert_handlers.append(handler)

    def run_health_checks(self) -> List[HealthCheckResult]:
        """Run all registered health checks."""
        results = []

        for check_func in self.health_checks:
            try:
                result = check_func()
                results.append(result)
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                results.append(HealthCheckResult(
                    component="health_check_error",
                    healthy=False,
                    message=f"Health check failed: {e}"
                ))

        return results

    def collect_system_metrics(self) -> SystemMetrics:
        """Collect current system performance metrics."""
        try:
            # System metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Database metrics
            db_connections = 0
            db_size_mb = 0.0
            recent_errors = 0
            recent_changes = 0
            last_successful_update = None

            try:
                with ChangeTracker(self.db_config) as tracker:
                    # Get database size
                    db_size_mb = self._get_database_size()

                    # Get recent activity
                    recent_runs = tracker.get_recent_runs(days=1)
                    db_connections = len(recent_runs)  # Approximate

                    # Count errors and changes
                    for run in recent_runs:
                        if run.get('status') == 'FAILED':
                            recent_errors += 1
                        elif run.get('status') == 'COMPLETED':
                            if last_successful_update is None or run.get('started_at') > last_successful_update:
                                last_successful_update = run.get('started_at')

                    # Get change summary
                    change_summary = tracker.get_change_summary(days=1)
                    recent_changes = change_summary.get('total_changes', 0)

            except Exception as e:
                logger.warning(f"Database metrics collection failed: {e}")

            metrics = SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                disk_usage_percent=disk.percent,
                database_connections=db_connections,
                database_size_mb=db_size_mb,
                recent_errors=recent_errors,
                recent_changes=recent_changes,
                last_successful_update=last_successful_update
            )

            # Store in history (keep last 1440 entries = 24 hours if collected every minute)
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > 1440:
                self.metrics_history = self.metrics_history[-1440:]

            return metrics

        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
            return SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=0.0,
                memory_percent=0.0,
                disk_usage_percent=0.0,
                database_connections=0,
                database_size_mb=0.0,
                recent_errors=1,
                recent_changes=0
            )

    def check_alerts(self, metrics: SystemMetrics):
        """Check all alerts against current metrics and trigger if needed."""
        metric_values = {
            "cpu_percent": metrics.cpu_percent,
            "memory_percent": metrics.memory_percent,
            "disk_usage_percent": metrics.disk_usage_percent,
            "recent_errors": metrics.recent_errors,
            "database_connections": 1 if metrics.database_connections > 0 else 0,
            "data_source_available": 1,  # Would need to check separately
            "days_since_last_update": self._calculate_days_since_last_update(metrics.last_successful_update)
        }

        for alert_key, config in self.alert_configs.items():
            if not config.enabled:
                continue

            # Check cooldown
            if self._is_alert_in_cooldown(alert_key, config):
                continue

            # Get metric value for this alert
            metric_name = self._get_metric_name_for_alert(alert_key)
            if metric_name not in metric_values:
                continue

            current_value = metric_values[metric_name]

            # Check threshold
            should_alert = self._check_threshold(current_value, config)

            if should_alert:
                self._trigger_alert(alert_key, config, current_value)

    def _check_database_connectivity(self) -> HealthCheckResult:
        """Check database connectivity."""
        try:
            with ChangeTracker(self.db_config) as tracker:
                # Simple connectivity test
                return HealthCheckResult(
                    component="database",
                    healthy=True,
                    message="Database connection successful",
                    details={"database": self.db_config.get('database', 'Unknown')}
                )
        except Exception as e:
            return HealthCheckResult(
                component="database",
                healthy=False,
                message=f"Database connection failed: {e}"
            )

    def _check_database_size(self) -> HealthCheckResult:
        """Check database size and growth."""
        try:
            size_mb = self._get_database_size()
            return HealthCheckResult(
                component="database_size",
                healthy=size_mb < 10000,  # Alert if over 10GB
                message=f"Database size: {size_mb:.1f} MB",
                details={"size_mb": size_mb}
            )
        except Exception as e:
            return HealthCheckResult(
                component="database_size",
                healthy=False,
                message=f"Failed to check database size: {e}"
            )

    def _check_data_source_availability(self) -> HealthCheckResult:
        """Check data source availability."""
        try:
            from . import TOURISM_DATA_SOURCE
            url = TOURISM_DATA_SOURCE['current_file_url']
            availability = DataSourceManager.check_url_availability(url, timeout=10)

            return HealthCheckResult(
                component="data_source",
                healthy=availability['available'],
                message=f"Data source {'available' if availability['available'] else 'unavailable'}",
                details=availability
            )
        except Exception as e:
            return HealthCheckResult(
                component="data_source",
                healthy=False,
                message=f"Failed to check data source: {e}"
            )

    def _check_system_resources(self) -> HealthCheckResult:
        """Check system resource usage."""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            healthy = (
                cpu_percent < 90 and
                memory.percent < 95 and
                disk.percent < 95
            )

            return HealthCheckResult(
                component="system_resources",
                healthy=healthy,
                message=f"CPU: {cpu_percent:.1f}%, Memory: {memory.percent:.1f}%, Disk: {disk.percent:.1f}%",
                details={
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "disk_percent": disk.percent
                }
            )
        except Exception as e:
            return HealthCheckResult(
                component="system_resources",
                healthy=False,
                message=f"Failed to check system resources: {e}"
            )

    def _check_recent_update_status(self) -> HealthCheckResult:
        """Check status of recent updates."""
        try:
            with ChangeTracker(self.db_config) as tracker:
                recent_runs = tracker.get_recent_runs(days=7)

                if not recent_runs:
                    return HealthCheckResult(
                        component="recent_updates",
                        healthy=False,
                        message="No recent update runs found"
                    )

                failed_runs = [r for r in recent_runs if r.get('status') == 'FAILED']
                success_rate = (len(recent_runs) - len(failed_runs)) / len(recent_runs)

                return HealthCheckResult(
                    component="recent_updates",
                    healthy=success_rate > 0.8,  # 80% success rate threshold
                    message=f"Update success rate: {success_rate:.1%} ({len(recent_runs)} runs)",
                    details={
                        "total_runs": len(recent_runs),
                        "failed_runs": len(failed_runs),
                        "success_rate": success_rate
                    }
                )

        except Exception as e:
            return HealthCheckResult(
                component="recent_updates",
                healthy=False,
                message=f"Failed to check recent updates: {e}"
            )

    def _check_error_rates(self) -> HealthCheckResult:
        """Check error rates in recent operations."""
        try:
            # Check log files for errors
            log_errors = self._count_recent_log_errors()

            return HealthCheckResult(
                component="error_rates",
                healthy=log_errors < 10,  # Less than 10 errors in recent logs
                message=f"Recent errors: {log_errors}",
                details={"error_count": log_errors}
            )
        except Exception as e:
            return HealthCheckResult(
                component="error_rates",
                healthy=False,
                message=f"Failed to check error rates: {e}"
            )

    def _check_disk_space(self) -> HealthCheckResult:
        """Check available disk space."""
        try:
            disk = psutil.disk_usage('/')
            free_gb = disk.free / (1024**3)

            return HealthCheckResult(
                component="disk_space",
                healthy=free_gb > 5.0,  # At least 5GB free
                message=f"Free disk space: {free_gb:.1f} GB",
                details={
                    "free_gb": free_gb,
                    "used_percent": disk.percent
                }
            )
        except Exception as e:
            return HealthCheckResult(
                component="disk_space",
                healthy=False,
                message=f"Failed to check disk space: {e}"
            )

    def _check_log_files(self) -> HealthCheckResult:
        """Check log file sizes and recent activity."""
        try:
            log_dir = Path('logs')
            if not log_dir.exists():
                return HealthCheckResult(
                    component="log_files",
                    healthy=True,
                    message="No log directory found"
                )

            log_files = list(log_dir.glob('*.log'))
            total_size_mb = sum(f.stat().st_size for f in log_files) / (1024**2)

            return HealthCheckResult(
                component="log_files",
                healthy=total_size_mb < 100,  # Less than 100MB of logs
                message=f"Log files: {len(log_files)} files, {total_size_mb:.1f} MB total",
                details={
                    "file_count": len(log_files),
                    "total_size_mb": total_size_mb
                }
            )
        except Exception as e:
            return HealthCheckResult(
                component="log_files",
                healthy=False,
                message=f"Failed to check log files: {e}"
            )

    def _get_database_size(self) -> float:
        """Get database size in MB."""
        try:
            with ChangeTracker(self.db_config) as tracker:
                with tracker.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT pg_size_pretty(pg_database_size(current_database())),
                               pg_database_size(current_database()) / (1024*1024) as size_mb
                    """)
                    result = cursor.fetchone()
                    return float(result[1]) if result else 0.0
        except Exception as e:
            logger.warning(f"Failed to get database size: {e}")
            return 0.0

    def _count_recent_log_errors(self) -> int:
        """Count errors in recent log files."""
        error_count = 0
        log_dir = Path('logs')

        if not log_dir.exists():
            return 0

        # Check today's log files
        today = datetime.now().strftime('%Y%m%d')
        log_files = log_dir.glob(f'*{today}*.log')

        for log_file in log_files:
            try:
                with open(log_file, 'r') as f:
                    for line in f:
                        if ' ERROR ' in line or ' CRITICAL ' in line:
                            error_count += 1
            except Exception as e:
                logger.warning(f"Failed to read log file {log_file}: {e}")

        return error_count

    def _calculate_days_since_last_update(self, last_update: Optional[datetime]) -> float:
        """Calculate days since last successful update."""
        if last_update is None:
            return 999.0  # Large number to trigger alert

        return (datetime.now() - last_update).total_seconds() / 86400

    def _get_metric_name_for_alert(self, alert_key: str) -> str:
        """Map alert key to metric name."""
        mapping = {
            "high_cpu": "cpu_percent",
            "high_memory": "memory_percent",
            "low_disk": "disk_usage_percent",
            "database_connection_failed": "database_connections",
            "data_source_unavailable": "data_source_available",
            "no_recent_updates": "days_since_last_update"
        }
        return mapping.get(alert_key, "unknown")

    def _check_threshold(self, value: float, config: AlertConfig) -> bool:
        """Check if value exceeds threshold."""
        if config.threshold_type == "greater_than":
            return value > config.threshold_value
        elif config.threshold_type == "less_than":
            return value < config.threshold_value
        elif config.threshold_type == "equals":
            return value == config.threshold_value
        return False

    def _is_alert_in_cooldown(self, alert_key: str, config: AlertConfig) -> bool:
        """Check if alert is in cooldown period."""
        if alert_key not in self.last_alert_times:
            return False

        last_alert = self.last_alert_times[alert_key]
        cooldown_delta = timedelta(minutes=config.alert_cooldown_minutes)
        return datetime.now() - last_alert < cooldown_delta

    def _trigger_alert(self, alert_key: str, config: AlertConfig, current_value: Any):
        """Trigger an alert."""
        self.last_alert_times[alert_key] = datetime.now()

        logger.warning(f"ALERT: {config.name} - Current value: {current_value}, Threshold: {config.threshold_value}")

        # Send to all alert handlers
        for handler in self.alert_handlers:
            try:
                handler(alert_key, config, current_value)
            except Exception as e:
                logger.error(f"Alert handler failed: {e}")

    def get_system_overview(self) -> Dict[str, Any]:
        """Get comprehensive system overview."""
        health_results = self.run_health_checks()
        current_metrics = self.collect_system_metrics()

        healthy_components = sum(1 for r in health_results if r.healthy)
        total_components = len(health_results)

        return {
            "timestamp": datetime.now().isoformat(),
            "overall_health": "healthy" if healthy_components == total_components else "degraded",
            "health_score": healthy_components / total_components if total_components > 0 else 0,
            "system_metrics": asdict(current_metrics),
            "health_checks": [
                {
                    "component": r.component,
                    "healthy": r.healthy,
                    "message": r.message,
                    "details": r.details
                }
                for r in health_results
            ],
            "active_alerts": len([k for k, v in self.alert_configs.items() if v.enabled])
        }


# Alert handlers

def console_alert_handler(alert_key: str, config: AlertConfig, current_value: Any):
    """Console alert handler."""
    severity_icon = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "critical": "🚨"
    }

    icon = severity_icon.get(config.severity, "⚠️")
    print(f"{icon} ALERT: {config.name}")
    print(f"   Current: {current_value}, Threshold: {config.threshold_value}")
    print(f"   Severity: {config.severity.upper()}")


def email_alert_handler(alert_key: str, config: AlertConfig, current_value: Any, email_config: Dict[str, str]):
    """Email alert handler."""
    try:
        subject = f"Tourism DB Alert: {config.name} ({config.severity.upper()})"

        body = f"""
Alert: {config.name}
Severity: {config.severity.upper()}
Current Value: {current_value}
Threshold: {config.threshold_value}
Timestamp: {datetime.now().isoformat()}

Please investigate and take appropriate action.
        """

        msg = MIMEMultipart()
        msg['From'] = email_config['from_email']
        msg['To'] = email_config['to_email']
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(email_config['smtp_server'], email_config.get('smtp_port', 587))
        server.starttls()
        server.login(email_config['smtp_username'], email_config['smtp_password'])

        text = msg.as_string()
        server.sendmail(email_config['from_email'], email_config['to_email'], text)
        server.quit()

        logger.info(f"Email alert sent for {config.name}")

    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")


def log_alert_handler(alert_key: str, config: AlertConfig, current_value: Any):
    """Log-based alert handler."""
    log_level = {
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }.get(config.severity, logging.WARNING)

    logger.log(
        log_level,
        f"ALERT: {config.name} - Current: {current_value}, Threshold: {config.threshold_value}"
    )