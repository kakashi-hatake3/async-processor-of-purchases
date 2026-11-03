import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Dict, Optional
import uuid

from faststream.rabbit import RabbitMessage
from sqlalchemy import select

from app.broker.rabbitmq import broker, dlq_queue, dlx_exchange, payments_exchange, payments_queue
from app.config import settings
from app.database import AsyncSessionLocal
from app.models.payment import Payment, PaymentStatus
from app.services.gateway import simulate_gateway_processing
from app.services.webhook import send_webhook

logger = logging.getLogger(__name__)

MAX_CONSUMER_RETRIES = 3


async def process_payment_business_logic(
    payment_id: uuid.UUID,
    session=None,
    gateway_min_delay: Optional[float] = None,
    gateway_max_delay: Optional[float] = None,
    gateway_success_rate: Optional[float] = None,
    webhook_retries: Optional[int] = None,
) -> Payment:
    """
    Executes the single consumer business logic:
    1. Fetches payment and checks if already processed.
    2. Emulates external gateway processing.
    3. Updates payment status in DB.
    4. Dispatches webhook notification with retries.
    """
    close_session_at_end = False
    if session is None:
        session = AsyncSessionLocal()
        close_session_at_end = True

    try:
        stmt = select(Payment).where(Payment.id == payment_id)
        result = await session.execute(stmt)
        payment = result.scalar_one_or_none()

        if not payment:
            raise ValueError(f"Payment with ID {payment_id} not found in database")

        if payment.status != PaymentStatus.PENDING:
            logger.info(
                "Payment %s already processed (status: %s), skipping gateway processing.",
                payment.id,
                payment.status.value,
            )
            return payment

        # 1. Gateway emulation
        status = await simulate_gateway_processing(
            payment_id=payment.id,
            min_delay=gateway_min_delay,
            max_delay=gateway_max_delay,
            success_rate=gateway_success_rate,
        )

        # 2. Update payment status in DB
        payment.status = status
        payment.processed_at = datetime.now(timezone.utc)
        await session.commit()
        await session.refresh(payment)
        logger.info("Payment %s updated in DB to status %s", payment.id, payment.status.value)

        # 3. Send Webhook notification
        webhook_payload = {
            "event": "payment.updated",
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "description": payment.description,
            "metadata": payment.metadata_info or {},
            "processed_at": payment.processed_at.isoformat(),
        }

        await send_webhook(
            webhook_url=payment.webhook_url,
            payload=webhook_payload,
            max_retries=webhook_retries,
        )

        return payment
    finally:
        if close_session_at_end:
            await session.close()


@broker.subscriber(queue=payments_queue, exchange=payments_exchange)
async def handle_new_payment(payload: Dict[str, Any], message: RabbitMessage) -> None:
    """
    Consumer handler for payments.new queue.
    Includes retry logic (up to 3 attempts with exponential backoff)
    and routes failed messages to Dead Letter Queue (DLQ).
    """
    headers = message.headers or {}
    retry_count = int(headers.get("x-retry-count", 0))
    raw_payment_id = payload.get("payment_id")

    logger.info(
        "Consumer: received payment event for payment_id=%s (attempt %d/%d)",
        raw_payment_id,
        retry_count + 1,
        MAX_CONSUMER_RETRIES,
    )

    try:
        if not raw_payment_id:
            raise ValueError("Missing 'payment_id' in event payload")

        payment_id = uuid.UUID(str(raw_payment_id))
        await process_payment_business_logic(payment_id=payment_id)
        await message.ack()
        logger.info("Consumer: successfully processed payment %s", payment_id)

    except Exception as exc:
        logger.error(
            "Consumer error processing payment %s (attempt %d/%d): %s",
            raw_payment_id,
            retry_count + 1,
            MAX_CONSUMER_RETRIES,
            exc,
        )

        if retry_count + 1 < MAX_CONSUMER_RETRIES:
            next_retry = retry_count + 1
            backoff_delay = 2 ** retry_count
            logger.info("Consumer: waiting %.2fs before retrying message for %s...", backoff_delay, raw_payment_id)
            await asyncio.sleep(backoff_delay)

            # Re-publish with incremented retry count
            new_headers = dict(headers)
            new_headers["x-retry-count"] = next_retry
            new_headers["x-last-error"] = str(exc)

            await broker.publish(
                message=payload,
                exchange=payments_exchange,
                routing_key="payments.new",
                headers=new_headers,
            )
            await message.ack()
        else:
            # Maximum retries exceeded: Route to Dead Letter Queue
            logger.critical(
                "Consumer: payment %s exceeded max retries (%d). Routing message to DLQ.",
                raw_payment_id,
                MAX_CONSUMER_RETRIES,
            )
            dlq_headers = dict(headers)
            dlq_headers["x-retry-count"] = retry_count + 1
            dlq_headers["x-final-error"] = str(exc)
            dlq_headers["x-failed-at"] = datetime.now(timezone.utc).isoformat()

            try:
                await broker.publish(
                    message=payload,
                    exchange=dlx_exchange,
                    routing_key="payments.dlq",
                    headers=dlq_headers,
                )
            except Exception as dlq_err:
                logger.exception("Failed to manually publish to DLQ, rejecting to native DLX: %s", dlq_err)
                await message.reject(requeue=False)
                return

            await message.ack()
