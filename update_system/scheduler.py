"""
Tourism Database Update Scheduler

Provides automated scheduling and cron-like functionality for the update system.
Supports various scheduling patterns and automated execution.
"""

import time
import threading
import schedule
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

from .orchestrator import UpdateOrchestrator, OrchestrationConfig, OrchestrationResult

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Types of scheduling patterns."""
    ONCE = "once"
    INTERVAL = "interval"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CRON = "cron"


@dataclass
class ScheduleConfig:
    """Configuration for scheduled update operations."""
    name: str
    schedule_type: ScheduleType
    enabled: bool = True

    # Interval scheduling (for INTERVAL type)
    interval_minutes: Optional[int] = None
    interval_hours: Optional[int] = None

    # Daily scheduling (for DAILY type)
    daily_time: Optional[str] = None  # Format: "HH:MM"

    # Weekly scheduling (for WEEKLY type)
    weekly_day: Optional[str] = None  # Format: "monday", "tuesday", etc.
    weekly_time: Optional[str] = None  # Format: "HH:MM"

    # Monthly scheduling (for MONTHLY type)
    monthly_day: Optional[int] = None  # Day of month (1-31)
    monthly_time: Optional[str] = None  # Format: "HH:MM"

    # Once scheduling (for ONCE type)
    once_at: Optional[datetime] = None

    # Cron expression (for CRON type)
    cron_expression: Optional[str] = None

    # Execution options
    run_validation_first: bool = True
    max_consecutive_failures: int = 3
    failure_notification: bool = True
    success_notification: bool = False


@dataclass
class ScheduledJobResult:
    """Result of a scheduled job execution."""
    job_name: str
    started_at: datetime
    completed_at: datetime
    success: bool
    orchestration_result: Optional[OrchestrationResult] = None
    error_message: Optional[str] = None
    validation_only: bool = False


class UpdateScheduler:
    """Scheduler for automated tourism database updates."""

    def __init__(self, orchestration_config: OrchestrationConfig):
        """
        Initialize the scheduler.

        Args:
            orchestration_config: Base configuration for orchestration
        """
        self.orchestration_config = orchestration_config
        self.scheduled_jobs: Dict[str, ScheduleConfig] = {}
        self.job_history: List[ScheduledJobResult] = []
        self.failure_counts: Dict[str, int] = {}
        self.running = False
        self.scheduler_thread = None
        self.notification_handlers = []

    def add_notification_handler(self, handler: Callable[[ScheduledJobResult], None]):
        """Add notification handler for job results."""
        self.notification_handlers.append(handler)

    def add_scheduled_job(self, config: ScheduleConfig):
        """
        Add a scheduled job.

        Args:
            config: Schedule configuration
        """
        if not self._validate_schedule_config(config):
            raise ValueError(f"Invalid schedule configuration for job: {config.name}")

        self.scheduled_jobs[config.name] = config
        self.failure_counts[config.name] = 0

        if self.running:
            self._setup_job_schedule(config)

        logger.info(f"Added scheduled job: {config.name} ({config.schedule_type.value})")

    def remove_scheduled_job(self, job_name: str):
        """Remove a scheduled job."""
        if job_name in self.scheduled_jobs:
            del self.scheduled_jobs[job_name]
            del self.failure_counts[job_name]
            schedule.clear(job_name)
            logger.info(f"Removed scheduled job: {job_name}")

    def enable_job(self, job_name: str):
        """Enable a scheduled job."""
        if job_name in self.scheduled_jobs:
            self.scheduled_jobs[job_name].enabled = True
            if self.running:
                self._setup_job_schedule(self.scheduled_jobs[job_name])
            logger.info(f"Enabled job: {job_name}")

    def disable_job(self, job_name: str):
        """Disable a scheduled job."""
        if job_name in self.scheduled_jobs:
            self.scheduled_jobs[job_name].enabled = False
            schedule.clear(job_name)
            logger.info(f"Disabled job: {job_name}")

    def start_scheduler(self):
        """Start the scheduler in a background thread."""
        if self.running:
            logger.warning("Scheduler is already running")
            return

        self.running = True

        # Set up all job schedules
        for config in self.scheduled_jobs.values():
            if config.enabled:
                self._setup_job_schedule(config)

        # Start scheduler thread
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()

        logger.info("Update scheduler started")

    def stop_scheduler(self):
        """Stop the scheduler."""
        if not self.running:
            logger.warning("Scheduler is not running")
            return

        self.running = False
        schedule.clear()

        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)

        logger.info("Update scheduler stopped")

    def _run_scheduler(self):
        """Main scheduler loop."""
        logger.info("Scheduler thread started")

        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(60)

        logger.info("Scheduler thread stopped")

    def _setup_job_schedule(self, config: ScheduleConfig):
        """Set up schedule for a specific job."""
        if not config.enabled:
            return

        job_func = lambda: self._execute_scheduled_job(config.name)

        if config.schedule_type == ScheduleType.INTERVAL:
            if config.interval_minutes:
                schedule.every(config.interval_minutes).minutes.do(job_func).tag(config.name)
            elif config.interval_hours:
                schedule.every(config.interval_hours).hours.do(job_func).tag(config.name)

        elif config.schedule_type == ScheduleType.DAILY:
            if config.daily_time:
                schedule.every().day.at(config.daily_time).do(job_func).tag(config.name)

        elif config.schedule_type == ScheduleType.WEEKLY:
            if config.weekly_day and config.weekly_time:
                getattr(schedule.every(), config.weekly_day.lower()).at(config.weekly_time).do(job_func).tag(config.name)

        elif config.schedule_type == ScheduleType.ONCE:
            if config.once_at:
                # For one-time jobs, we'll check the time manually
                schedule.every().minute.do(
                    lambda: self._check_once_job(config.name, config.once_at)
                ).tag(config.name)

        # Note: MONTHLY and CRON types would need more complex implementation
        # For now, they're not fully supported

        logger.info(f"Scheduled job '{config.name}' set up successfully")

    def _check_once_job(self, job_name: str, target_time: datetime):
        """Check if a one-time job should run."""
        if datetime.now() >= target_time:
            self._execute_scheduled_job(job_name)
            # Remove the job after execution
            schedule.clear(job_name)
            self.scheduled_jobs[job_name].enabled = False

    def _execute_scheduled_job(self, job_name: str):
        """Execute a scheduled job."""
        if job_name not in self.scheduled_jobs:
            logger.error(f"Scheduled job not found: {job_name}")
            return

        config = self.scheduled_jobs[job_name]
        if not config.enabled:
            return

        logger.info(f"Executing scheduled job: {job_name}")

        start_time = datetime.now()
        job_result = ScheduledJobResult(
            job_name=job_name,
            started_at=start_time,
            completed_at=start_time,  # Will be updated
            success=False
        )

        try:
            # Create orchestrator
            orchestrator = UpdateOrchestrator(self.orchestration_config)

            # Execute based on configuration
            if config.run_validation_first:
                # Run validation first
                logger.info(f"Running validation for job: {job_name}")
                validation_result = orchestrator.execute_validation_only()

                if validation_result.success:
                    # If validation passes, run full update
                    logger.info(f"Validation passed, running full update for job: {job_name}")
                    orchestration_result = orchestrator.execute_full_update_workflow()
                    job_result.orchestration_result = orchestration_result
                    job_result.success = orchestration_result.success
                else:
                    # If validation fails, don't run update
                    logger.warning(f"Validation failed for job: {job_name}")
                    job_result.orchestration_result = validation_result
                    job_result.success = False
                    job_result.validation_only = True
            else:
                # Run full update directly
                orchestration_result = orchestrator.execute_full_update_workflow()
                job_result.orchestration_result = orchestration_result
                job_result.success = orchestration_result.success

            # Update failure count
            if job_result.success:
                self.failure_counts[job_name] = 0
            else:
                self.failure_counts[job_name] += 1

            # Check if job should be disabled due to consecutive failures
            if self.failure_counts[job_name] >= config.max_consecutive_failures:
                logger.error(f"Job {job_name} disabled due to {config.max_consecutive_failures} consecutive failures")
                self.disable_job(job_name)

        except Exception as e:
            logger.error(f"Scheduled job {job_name} failed with exception: {e}")
            job_result.success = False
            job_result.error_message = str(e)
            self.failure_counts[job_name] += 1

        finally:
            job_result.completed_at = datetime.now()
            self.job_history.append(job_result)

            # Keep only last 100 results
            if len(self.job_history) > 100:
                self.job_history = self.job_history[-100:]

            # Send notifications
            self._send_job_notifications(job_result, config)

        logger.info(f"Scheduled job {job_name} completed: {'success' if job_result.success else 'failed'}")

    def _send_job_notifications(self, job_result: ScheduledJobResult, config: ScheduleConfig):
        """Send notifications for job completion."""
        should_notify = (
            (config.success_notification and job_result.success) or
            (config.failure_notification and not job_result.success)
        )

        if should_notify:
            for handler in self.notification_handlers:
                try:
                    handler(job_result)
                except Exception as e:
                    logger.warning(f"Notification handler failed: {e}")

    def _validate_schedule_config(self, config: ScheduleConfig) -> bool:
        """Validate schedule configuration."""
        if config.schedule_type == ScheduleType.INTERVAL:
            return bool(config.interval_minutes or config.interval_hours)

        elif config.schedule_type == ScheduleType.DAILY:
            return bool(config.daily_time)

        elif config.schedule_type == ScheduleType.WEEKLY:
            return bool(config.weekly_day and config.weekly_time)

        elif config.schedule_type == ScheduleType.MONTHLY:
            return bool(config.monthly_day and config.monthly_time)

        elif config.schedule_type == ScheduleType.ONCE:
            return bool(config.once_at)

        elif config.schedule_type == ScheduleType.CRON:
            return bool(config.cron_expression)

        return False

    def get_job_status(self, job_name: str) -> Dict[str, Any]:
        """Get status of a specific job."""
        if job_name not in self.scheduled_jobs:
            return {'error': 'Job not found'}

        config = self.scheduled_jobs[job_name]

        # Get recent job results
        job_results = [r for r in self.job_history if r.job_name == job_name]
        recent_results = sorted(job_results, key=lambda x: x.started_at, reverse=True)[:10]

        # Get next run time
        next_run = None
        job = schedule.jobs
        for job in schedule.jobs:
            if job_name in job.tags:
                next_run = job.next_run
                break

        return {
            'name': job_name,
            'enabled': config.enabled,
            'schedule_type': config.schedule_type.value,
            'failure_count': self.failure_counts.get(job_name, 0),
            'max_consecutive_failures': config.max_consecutive_failures,
            'next_run': next_run.isoformat() if next_run else None,
            'recent_results': [
                {
                    'started_at': r.started_at.isoformat(),
                    'success': r.success,
                    'validation_only': r.validation_only,
                    'total_changes': r.orchestration_result.total_changes if r.orchestration_result else 0
                }
                for r in recent_results
            ]
        }

    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get overall scheduler status."""
        active_jobs = sum(1 for config in self.scheduled_jobs.values() if config.enabled)
        total_jobs = len(self.scheduled_jobs)

        recent_results = sorted(self.job_history, key=lambda x: x.started_at, reverse=True)[:20]
        successful_recent = sum(1 for r in recent_results if r.success)

        return {
            'running': self.running,
            'total_jobs': total_jobs,
            'active_jobs': active_jobs,
            'disabled_jobs': total_jobs - active_jobs,
            'recent_success_rate': successful_recent / len(recent_results) if recent_results else 0,
            'total_executions': len(self.job_history),
            'last_execution': recent_results[0].started_at.isoformat() if recent_results else None
        }


# Pre-defined schedule configurations

def create_daily_update_schedule(time_str: str = "02:00") -> ScheduleConfig:
    """Create a daily update schedule."""
    return ScheduleConfig(
        name="daily_update",
        schedule_type=ScheduleType.DAILY,
        daily_time=time_str,
        run_validation_first=True,
        max_consecutive_failures=3,
        failure_notification=True,
        success_notification=False
    )


def create_weekly_update_schedule(day: str = "sunday", time_str: str = "03:00") -> ScheduleConfig:
    """Create a weekly update schedule."""
    return ScheduleConfig(
        name="weekly_update",
        schedule_type=ScheduleType.WEEKLY,
        weekly_day=day,
        weekly_time=time_str,
        run_validation_first=True,
        max_consecutive_failures=2,
        failure_notification=True,
        success_notification=True
    )


def create_hourly_validation_schedule() -> ScheduleConfig:
    """Create an hourly validation schedule."""
    return ScheduleConfig(
        name="hourly_validation",
        schedule_type=ScheduleType.INTERVAL,
        interval_hours=1,
        run_validation_first=True,  # This will be validation-only since it's frequent
        max_consecutive_failures=5,
        failure_notification=False,  # Too frequent for notifications
        success_notification=False
    )


# Notification handlers for scheduled jobs

def scheduled_job_console_handler(result: ScheduledJobResult):
    """Console notification handler for scheduled jobs."""
    status = "✅" if result.success else "❌"
    duration = (result.completed_at - result.started_at).total_seconds()

    changes = 0
    if result.orchestration_result:
        changes = result.orchestration_result.total_changes

    validation_note = " (validation only)" if result.validation_only else ""

    print(f"{status} {result.job_name}: {changes} changes in {duration:.1f}s{validation_note}")


def scheduled_job_email_handler(result: ScheduledJobResult, email_config: Dict[str, Any]):
    """Email notification handler for scheduled jobs."""
    # Placeholder for email implementation
    subject = f"Tourism DB Update: {result.job_name} - {'Success' if result.success else 'Failed'}"
    logger.info(f"Email notification: {subject}")


def scheduled_job_log_handler(result: ScheduledJobResult):
    """Log-based notification handler for scheduled jobs."""
    log_level = logging.INFO if result.success else logging.ERROR

    changes = result.orchestration_result.total_changes if result.orchestration_result else 0
    duration = (result.completed_at - result.started_at).total_seconds()

    logger.log(
        log_level,
        f"Scheduled job '{result.job_name}' completed: "
        f"success={result.success}, changes={changes}, duration={duration:.1f}s"
    )