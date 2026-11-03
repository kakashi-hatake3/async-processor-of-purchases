import asyncio
import logging
import random
from typing import Optional
import uuid

from app.config import settings
from app.models.payment import PaymentStatus

logger = logging.getLogger(__name__)


async def simulate_gateway_processing(
    payment_id: uuid.UUID,
    min_delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    success_rate: Optional[float] = None,
) -> PaymentStatus:
    """
    Simulates external payment gateway processing.
    Emulates 2-5 seconds delay and 90% success / 10% failure rate.
    """
    _min = min_delay if min_delay is not None else settings.GATEWAY_MIN_DELAY
    _max = max_delay if max_delay is not None else settings.GATEWAY_MAX_DELAY
    _rate = success_rate if success_rate is not None else settings.GATEWAY_SUCCESS_RATE

    delay = random.uniform(_min, _max)
    logger.info("Gateway: processing payment %s (emulating %.2fs latency)...", payment_id, delay)
    await asyncio.sleep(delay)

    is_success = random.random() < _rate
    status = PaymentStatus.SUCCEEDED if is_success else PaymentStatus.FAILED

    logger.info("Gateway: payment %s finished with status %s", payment_id, status.value)
    return status
