"""Built-in email task (simulation - logs instead of sending)."""

import logging
from typing import Any

from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

TASK_ID = "send_email"


def send_email(parameters: dict) -> Any:
    """Simulate sending an email by logging the details.

    This is a mock implementation. In production, replace with actual
    SMTP/SES/SendGrid integration.

    Parameters:
        to (str): Recipient email address. Required.
        subject (str): Email subject. Required.
        body (str): Email body. Required.
        from_addr (str): Sender address. Defaults to noreply@jobscheduler.local.
        cc (list): Optional CC addresses.
        bcc (list): Optional BCC addresses.

    Returns:
        dict: Simulation result with metadata.

    Raises:
        ValueError: If required parameters are missing.
    """
    to = parameters.get("to")
    subject = parameters.get("subject")
    body = parameters.get("body")

    if not to:
        raise ValueError("Parameter 'to' is required for send_email task.")
    if not subject:
        raise ValueError("Parameter 'subject' is required for send_email task.")
    if not body:
        raise ValueError("Parameter 'body' is required for send_email task.")

    from_addr = parameters.get("from_addr", "noreply@jobscheduler.local")
    cc = parameters.get("cc", [])
    bcc = parameters.get("bcc", [])

    # Validate email format (basic check)
    if "@" not in to:
        raise ValueError(f"Invalid recipient email address: '{to}'")

    logger.info(
        "[send_email task] SIMULATED EMAIL SEND:\n"
        "  From: %s\n"
        "  To: %s\n"
        "  CC: %s\n"
        "  BCC: %s\n"
        "  Subject: %s\n"
        "  Body: %s",
        from_addr,
        to,
        cc,
        bcc,
        subject,
        body[:200] + "..." if len(body) > 200 else body,
    )

    return {
        "simulated": True,
        "to": to,
        "from": from_addr,
        "subject": subject,
        "cc": cc,
        "bcc": bcc,
        "message": "Email simulation logged. Configure SMTP/SES for production.",
    }


# Register with the global task registry
TASK_REGISTRY.register(TASK_ID, send_email)
