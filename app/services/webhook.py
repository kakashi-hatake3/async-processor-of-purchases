import asyncio
import logging
from typing import Any, Dict, Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


async def send_webhook(
    webhook_url: str,
    payload: Dict[str, Any],
    max_retries: Optional[int] = None,
    timeout: Optional[float] = None,
    backoff_factor: Optional[float] = None,
) -> bool:
    """
    Sends a webhook POST notification with retry mechanism.
    Default: 3 attempts with exponential backoff.
    """
    retries = max_retries if max_retries is not None else settings.WEBHOOK_MAX_RETRIES
    req_timeout = timeout if timeout is not None else settings.WEBHOOK_TIMEOUT
    factor = backoff_factor if backoff_factor is not None else settings.WEBHOOK_BACKOFF_FACTOR

    async with httpx.AsyncClient(timeout=req_timeout) as client:
        for attempt in range(1, retries + 1):
            try:
                logger.info(
                    "Webhook: sending notification to %s (attempt %d/%d)...",
                    webhook_url,
                    attempt,
                    retries,
                )
                response = await client.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                if response.is_success:
                    logger.info(
                        "Webhook: successfully delivered to %s (status %d)",
                        webhook_url,
                        response.status_code,
                    )
                    return True
                raise httpx.HTTPError(f"Server returned error HTTP {response.status_code}")
            except (httpx.HTTPError, Exception) as exc:
                logger.warning(
                    "Webhook: attempt %d/%d failed for %s: %s",
                    attempt,
                    retries,
                    webhook_url,
                    exc,
                )
                if attempt < retries:
                    delay = factor * (2 ** (attempt - 1))
                    logger.info("Webhook: waiting %.2fs before retry...", delay)
                    await asyncio.sleep(delay)

    logger.error("Webhook: all %d attempts failed for %s", retries, webhook_url)
    return False
