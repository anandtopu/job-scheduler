"""Built-in HTTP request task."""

import logging
from typing import Any

import httpx

from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

TASK_ID = "http_request"

VALID_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
DEFAULT_TIMEOUT = 30.0  # seconds
MAX_BODY_LOG_LENGTH = 500


def http_request(parameters: dict) -> Any:
    """Make an HTTP request to the specified URL.

    Parameters:
        url (str): The URL to request. Required.
        method (str): HTTP method. Defaults to GET.
        headers (dict): Optional HTTP headers.
        body (dict): Optional request body (sent as JSON).
        timeout (float): Request timeout in seconds. Defaults to 30.

    Returns:
        dict: Response details including status_code, headers, and body.

    Raises:
        ValueError: If url is missing or method is invalid.
        httpx.HTTPError: If the request fails.
        httpx.TimeoutException: If the request times out.
    """
    url = parameters.get("url")
    if not url:
        raise ValueError("Parameter 'url' is required for http_request task.")

    method = parameters.get("method", "GET").upper()
    if method not in VALID_METHODS:
        raise ValueError(
            f"Invalid HTTP method '{method}'. Must be one of: {sorted(VALID_METHODS)}"
        )

    headers = parameters.get("headers", {})
    body = parameters.get("body")
    timeout = float(parameters.get("timeout", DEFAULT_TIMEOUT))

    logger.info("[http_request task] %s %s", method, url)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.request(
                method=method,
                url=url,
                headers=headers,
                json=body if body else None,
            )

        response_body = response.text
        if len(response_body) > MAX_BODY_LOG_LENGTH:
            log_body = response_body[:MAX_BODY_LOG_LENGTH] + "...[truncated]"
        else:
            log_body = response_body

        logger.info(
            "[http_request task] Response: status=%d body_preview=%s",
            response.status_code,
            log_body,
        )

        # Raise for 4xx/5xx errors
        response.raise_for_status()

        return {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": response.text,
            "url": str(response.url),
            "elapsed_ms": response.elapsed.total_seconds() * 1000,
        }

    except httpx.TimeoutException as exc:
        logger.error("[http_request task] Timeout after %.1fs: %s", timeout, exc)
        raise
    except httpx.HTTPStatusError as exc:
        logger.error(
            "[http_request task] HTTP error %d: %s",
            exc.response.status_code,
            exc,
        )
        raise
    except httpx.HTTPError as exc:
        logger.error("[http_request task] HTTP error: %s", exc)
        raise


# Register with the global task registry
TASK_REGISTRY.register(TASK_ID, http_request)
