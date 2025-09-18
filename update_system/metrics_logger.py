"""
Comprehensive Logging and Metrics System

Enterprise-grade logging, metrics collection, and observability
for the Tourism Database Update System.
"""

import os
import json
import time
import logging
import logging.handlers
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import queue
from collections import defaultdict, deque
import uuid

# Configure structured logging
class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter for logs."""

    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # Add extra fields if available
        if hasattr(record, 'request_id'):
            log_entry['request_id'] = record.request_id
        if hasattr(record, 'user_id'):
            log_entry['user_id'] = record.user_id
        if hasattr(record, 'duration_ms'):
            log_entry['duration_ms'] = record.duration_ms
        if hasattr(record, 'operation'):
            log_entry['operation'] = record.operation

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


@dataclass
class MetricPoint:
    """Individual metric data point."""
    name: str
    value: float
    timestamp: datetime
    tags: Dict[str, str]
    metric_type: str  # 'counter', 'gauge', 'histogram', 'timer'


@dataclass
class PerformanceEvent:
    """Performance tracking event."""
    event_id: str
    operation: str
    started_at: datetime
    completed_at: Optional[datetime]
    duration_ms: Optional[float]
    success: bool
    metadata: Dict[str, Any]
    error_message: Optional[str] = None


class MetricsCollector:
    """Collects and aggregates system metrics."""

    def __init__(self, retention_hours: int = 24):
        self.retention_hours = retention_hours
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.aggregates: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.lock = threading.RLock()

        # Performance tracking
        self.active_operations: Dict[str, PerformanceEvent] = {}
        self.completed_operations: deque = deque(maxlen=1000)

        # Counters
        self.counters: Dict[str, int] = defaultdict(int)

        # Background cleanup
        self.cleanup_thread = None
        self.running = False

    def start_background_tasks(self):
        """Start background metric processing."""
        if self.running:
            return

        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def stop_background_tasks(self):
        """Stop background processing."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)

    def record_metric(self, name: str, value: float, tags: Dict[str, str] = None,
                     metric_type: str = 'gauge'):
        """Record a metric value."""
        with self.lock:
            metric = MetricPoint(
                name=name,
                value=value,
                timestamp=datetime.now(),
                tags=tags or {},
                metric_type=metric_type
            )

            self.metrics[name].append(metric)

            # Update aggregates
            self._update_aggregates(name, value)

    def increment_counter(self, name: str, value: int = 1, tags: Dict[str, str] = None):
        """Increment a counter metric."""
        with self.lock:
            self.counters[name] += value
            self.record_metric(name, self.counters[name], tags, 'counter')

    def record_timer(self, name: str, duration_ms: float, tags: Dict[str, str] = None):
        """Record a timing metric."""
        self.record_metric(name, duration_ms, tags, 'timer')

    def start_operation(self, operation: str, metadata: Dict[str, Any] = None) -> str:
        """Start tracking a performance operation."""
        event_id = str(uuid.uuid4())

        event = PerformanceEvent(
            event_id=event_id,
            operation=operation,
            started_at=datetime.now(),
            completed_at=None,
            duration_ms=None,
            success=False,
            metadata=metadata or {}
        )

        with self.lock:
            self.active_operations[event_id] = event

        return event_id

    def complete_operation(self, event_id: str, success: bool = True,
                          error_message: str = None, metadata: Dict[str, Any] = None):
        """Complete a performance operation."""
        with self.lock:
            if event_id not in self.active_operations:
                return

            event = self.active_operations[event_id]
            event.completed_at = datetime.now()
            event.duration_ms = (event.completed_at - event.started_at).total_seconds() * 1000
            event.success = success
            event.error_message = error_message

            if metadata:
                event.metadata.update(metadata)

            # Move to completed operations
            self.completed_operations.append(event)
            del self.active_operations[event_id]

            # Record metrics
            self.record_timer(f"{event.operation}_duration", event.duration_ms)
            self.increment_counter(f"{event.operation}_total")

            if success:
                self.increment_counter(f"{event.operation}_success")
            else:
                self.increment_counter(f"{event.operation}_errors")

    def get_metric_stats(self, name: str, hours: int = 1) -> Dict[str, Any]:
        """Get statistics for a specific metric."""
        with self.lock:
            if name not in self.metrics:
                return {}

            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_metrics = [
                m for m in self.metrics[name]
                if m.timestamp > cutoff_time
            ]

            if not recent_metrics:
                return {}

            values = [m.value for m in recent_metrics]

            return {
                'count': len(values),
                'min': min(values),
                'max': max(values),
                'avg': sum(values) / len(values),
                'latest': values[-1] if values else 0,
                'metric_type': recent_metrics[-1].metric_type if recent_metrics else 'unknown'
            }

    def get_operation_stats(self, operation: str = None, hours: int = 1) -> Dict[str, Any]:
        """Get performance statistics for operations."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        with self.lock:
            # Filter operations
            if operation:
                operations = [
                    op for op in self.completed_operations
                    if op.operation == operation and op.completed_at and op.completed_at > cutoff_time
                ]
            else:
                operations = [
                    op for op in self.completed_operations
                    if op.completed_at and op.completed_at > cutoff_time
                ]

            if not operations:
                return {}

            # Calculate statistics
            durations = [op.duration_ms for op in operations if op.duration_ms]
            successful_ops = [op for op in operations if op.success]
            failed_ops = [op for op in operations if not op.success]

            stats = {
                'total_operations': len(operations),
                'successful_operations': len(successful_ops),
                'failed_operations': len(failed_ops),
                'success_rate': len(successful_ops) / len(operations) * 100 if operations else 0,
                'active_operations': len(self.active_operations)
            }

            if durations:
                stats.update({
                    'avg_duration_ms': sum(durations) / len(durations),
                    'min_duration_ms': min(durations),
                    'max_duration_ms': max(durations),
                    'p95_duration_ms': sorted(durations)[int(len(durations) * 0.95)] if len(durations) > 1 else durations[0],
                    'p99_duration_ms': sorted(durations)[int(len(durations) * 0.99)] if len(durations) > 1 else durations[0]
                })

            return stats

    def get_all_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all collected metrics."""
        with self.lock:
            summary = {
                'metrics_count': len(self.metrics),
                'active_operations': len(self.active_operations),
                'completed_operations': len(self.completed_operations),
                'counters': dict(self.counters),
                'metric_names': list(self.metrics.keys())
            }

            # Add recent activity
            recent_operations = self.get_operation_stats(hours=1)
            summary['recent_activity'] = recent_operations

            return summary

    def _update_aggregates(self, name: str, value: float):
        """Update metric aggregates."""
        if name not in self.aggregates:
            self.aggregates[name] = {
                'count': 0,
                'sum': 0,
                'min': float('inf'),
                'max': float('-inf')
            }

        agg = self.aggregates[name]
        agg['count'] += 1
        agg['sum'] += value
        agg['min'] = min(agg['min'], value)
        agg['max'] = max(agg['max'], value)
        agg['avg'] = agg['sum'] / agg['count']

    def _cleanup_loop(self):
        """Background cleanup of old metrics."""
        while self.running:
            try:
                self._cleanup_old_metrics()
                time.sleep(300)  # Clean up every 5 minutes
            except Exception as e:
                logging.error(f"Error in metrics cleanup: {e}")
                time.sleep(60)

    def _cleanup_old_metrics(self):
        """Remove old metric data points."""
        cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)

        with self.lock:
            for name, metric_deque in self.metrics.items():
                # Remove old metrics (deque automatically limits size)
                while metric_deque and metric_deque[0].timestamp < cutoff_time:
                    metric_deque.popleft()


class EnhancedLogger:
    """Enhanced logging system with structured logging and metrics integration."""

    def __init__(self, name: str, log_dir: str = "logs", metrics_collector: MetricsCollector = None):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.metrics = metrics_collector or MetricsCollector()

        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Clear existing handlers
        self.logger.handlers.clear()

        # Add structured file handler
        self._setup_file_handlers()

        # Add console handler for development
        self._setup_console_handler()

        # Request tracking
        self._request_context = threading.local()

    def _setup_file_handlers(self):
        """Setup file logging handlers."""
        # Main application log
        main_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / "tourism_db.log",
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=10,
            encoding='utf-8'
        )
        main_handler.setLevel(logging.INFO)
        main_handler.setFormatter(StructuredFormatter())

        # Error log
        error_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / "tourism_db_errors.log",
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=5,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(StructuredFormatter())

        # Performance log
        perf_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / "tourism_db_performance.log",
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=5,
            encoding='utf-8'
        )
        perf_handler.setLevel(logging.INFO)
        perf_handler.addFilter(lambda record: hasattr(record, 'duration_ms'))
        perf_handler.setFormatter(StructuredFormatter())

        self.logger.addHandler(main_handler)
        self.logger.addHandler(error_handler)
        self.logger.addHandler(perf_handler)

    def _setup_console_handler(self):
        """Setup console logging for development."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def set_request_context(self, request_id: str, user_id: str = None, operation: str = None):
        """Set context for current request/operation."""
        self._request_context.request_id = request_id
        self._request_context.user_id = user_id
        self._request_context.operation = operation

    def clear_request_context(self):
        """Clear request context."""
        for attr in ['request_id', 'user_id', 'operation']:
            if hasattr(self._request_context, attr):
                delattr(self._request_context, attr)

    def _add_context(self, extra: Dict[str, Any] = None) -> Dict[str, Any]:
        """Add request context to log extra data."""
        context = extra or {}

        if hasattr(self._request_context, 'request_id'):
            context['request_id'] = self._request_context.request_id
        if hasattr(self._request_context, 'user_id'):
            context['user_id'] = self._request_context.user_id
        if hasattr(self._request_context, 'operation'):
            context['operation'] = self._request_context.operation

        return context

    def info(self, message: str, extra: Dict[str, Any] = None):
        """Log info message with context."""
        self.logger.info(message, extra=self._add_context(extra))
        self.metrics.increment_counter('log_messages_info')

    def warning(self, message: str, extra: Dict[str, Any] = None):
        """Log warning message with context."""
        self.logger.warning(message, extra=self._add_context(extra))
        self.metrics.increment_counter('log_messages_warning')

    def error(self, message: str, exc_info=None, extra: Dict[str, Any] = None):
        """Log error message with context."""
        self.logger.error(message, exc_info=exc_info, extra=self._add_context(extra))
        self.metrics.increment_counter('log_messages_error')

    def debug(self, message: str, extra: Dict[str, Any] = None):
        """Log debug message with context."""
        self.logger.debug(message, extra=self._add_context(extra))
        self.metrics.increment_counter('log_messages_debug')

    def log_performance(self, operation: str, duration_ms: float, success: bool = True,
                       metadata: Dict[str, Any] = None):
        """Log performance metrics."""
        extra = self._add_context({
            'operation': operation,
            'duration_ms': duration_ms,
            'success': success
        })

        if metadata:
            extra.update(metadata)

        level = logging.INFO if success else logging.WARNING
        message = f"Operation {operation} {'completed' if success else 'failed'} in {duration_ms:.2f}ms"

        self.logger.log(level, message, extra=extra)
        self.metrics.record_timer(f"{operation}_duration", duration_ms)

    def log_database_operation(self, operation: str, table: str, rows_affected: int,
                              duration_ms: float, success: bool = True):
        """Log database operation with specific context."""
        extra = {
            'operation': operation,
            'table': table,
            'rows_affected': rows_affected,
            'duration_ms': duration_ms,
            'success': success
        }

        message = f"Database {operation} on {table}: {rows_affected} rows in {duration_ms:.2f}ms"

        if success:
            self.info(message, extra)
        else:
            self.error(message, extra)

        # Record metrics
        self.metrics.record_timer(f"db_{operation}_duration", duration_ms)
        self.metrics.record_metric(f"db_{operation}_rows", rows_affected)

    def log_api_request(self, method: str, endpoint: str, status_code: int,
                       duration_ms: float, user_id: str = None):
        """Log API request with standard format."""
        extra = {
            'method': method,
            'endpoint': endpoint,
            'status_code': status_code,
            'duration_ms': duration_ms,
            'user_id': user_id
        }

        message = f"{method} {endpoint} - {status_code} ({duration_ms:.2f}ms)"

        if status_code < 400:
            self.info(message, extra)
        elif status_code < 500:
            self.warning(message, extra)
        else:
            self.error(message, extra)

        # Record metrics
        self.metrics.record_timer('api_request_duration', duration_ms)
        self.metrics.increment_counter(f'api_requests_total')
        self.metrics.increment_counter(f'api_requests_{status_code // 100}xx')


class PerformanceTimer:
    """Context manager for timing operations."""

    def __init__(self, logger: EnhancedLogger, operation: str, metadata: Dict[str, Any] = None):
        self.logger = logger
        self.operation = operation
        self.metadata = metadata or {}
        self.start_time = None
        self.event_id = None

    def __enter__(self):
        self.start_time = time.time()
        self.event_id = self.logger.metrics.start_operation(self.operation, self.metadata)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        success = exc_type is None

        self.logger.metrics.complete_operation(
            self.event_id,
            success=success,
            error_message=str(exc_val) if exc_val else None
        )

        self.logger.log_performance(
            self.operation,
            duration_ms,
            success=success,
            metadata=self.metadata
        )


def create_system_logger(name: str = "tourism_db") -> EnhancedLogger:
    """Create a system-wide logger instance."""
    metrics_collector = MetricsCollector()
    metrics_collector.start_background_tasks()

    logger = EnhancedLogger(name, metrics_collector=metrics_collector)
    return logger


# Global logger instance
system_logger = create_system_logger()


def get_logger(name: str = None) -> EnhancedLogger:
    """Get the system logger."""
    if name:
        return EnhancedLogger(name, metrics_collector=system_logger.metrics)
    return system_logger


def timed_operation(operation: str, metadata: Dict[str, Any] = None):
    """Decorator for timing operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger()
            with PerformanceTimer(logger, operation, metadata):
                return func(*args, **kwargs)
        return wrapper
    return decorator


# Example usage and testing
if __name__ == "__main__":
    # Test the logging system
    logger = get_logger("test")

    # Test basic logging
    logger.info("System starting up")
    logger.warning("This is a warning")

    # Test performance logging
    with PerformanceTimer(logger, "test_operation", {"user": "test"}):
        time.sleep(0.1)  # Simulate work

    # Test metrics
    logger.metrics.record_metric("cpu_usage", 75.5)
    logger.metrics.increment_counter("requests_total")

    # Print metrics summary
    print(json.dumps(logger.metrics.get_all_metrics_summary(), indent=2))