"""Unit tests for v2 API schema validation."""

from datetime import datetime, timedelta
import pytest
from pydantic import ValidationError

from src.api.schemas import CreateJobRequest
from src.models.enums import JobPriority, ScheduleType


class TestCreateJobRequestV2:
    """Test v2-specific schema validation."""

    def test_interval_schedule_valid(self):
        """INTERVAL schedule with valid integer expression should pass."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.INTERVAL,
            schedule_expression="300",
        )
        assert req.schedule_expression == "300"

    def test_interval_schedule_invalid_string(self):
        """Non-numeric INTERVAL expression should fail validation."""
        with pytest.raises(ValidationError, match="positive integer"):
            CreateJobRequest(
                task_id="log_message",
                schedule_type=ScheduleType.INTERVAL,
                schedule_expression="every_5_minutes",
            )

    def test_interval_schedule_zero_fails(self):
        """Zero-second interval should fail validation."""
        with pytest.raises(ValidationError):
            CreateJobRequest(
                task_id="log_message",
                schedule_type=ScheduleType.INTERVAL,
                schedule_expression="0",
            )

    def test_priority_default_is_normal(self):
        """Default priority should be NORMAL."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
        )
        assert req.priority == JobPriority.NORMAL

    def test_priority_critical_accepted(self):
        """CRITICAL priority should be accepted."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            priority=JobPriority.CRITICAL,
        )
        assert req.priority == JobPriority.CRITICAL

    def test_tags_limit_enforced(self):
        """More than 20 tags should fail validation."""
        with pytest.raises(ValidationError):
            CreateJobRequest(
                task_id="log_message",
                schedule_type=ScheduleType.IMMEDIATE,
                tags=[f"tag_{i}" for i in range(25)],
            )

    def test_tags_accepted(self):
        """Valid tags list should be accepted."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            tags=["prod", "billing", "daily"],
        )
        assert len(req.tags) == 3

    def test_timeout_default_is_30(self):
        """Default execution timeout should be 30 seconds."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
        )
        assert req.execution_timeout_seconds == 30

    def test_timeout_valid_range(self):
        """Timeout within valid range should pass."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            execution_timeout_seconds=120,
        )
        assert req.execution_timeout_seconds == 120

    def test_notification_url_accepted(self):
        """Notification URL should be stored."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            notification_url="https://hooks.example.com/webhook",
        )
        assert req.notification_url == "https://hooks.example.com/webhook"

    def test_max_executions_accepted(self):
        """Positive max_executions should pass."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            max_executions=10,
        )
        assert req.max_executions == 10

    def test_depends_on_accepted(self):
        """depends_on list should be stored."""
        dep_id = "550e8400-e29b-41d4-a716-446655440000"
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            depends_on=[dep_id],
        )
        assert len(req.depends_on) == 1

    def test_job_name_accepted(self):
        """job_name should be stored for uniqueness checking."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.IMMEDIATE,
            job_name="daily-report",
        )
        assert req.job_name == "daily-report"

    def test_timezone_accepted(self):
        """Custom timezone should be stored."""
        req = CreateJobRequest(
            task_id="log_message",
            schedule_type=ScheduleType.CRON,
            schedule_expression="0 9 * * 1-5",
            timezone="America/New_York",
        )
        assert req.timezone == "America/New_York"
