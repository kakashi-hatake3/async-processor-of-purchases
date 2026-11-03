import asyncio
from datetime import datetime, timezone
import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.rabbitmq import broker as default_broker, payments_exchange
from app.config import settings
from app.database import AsyncSessionLocal
from app.models.outbox import OutboxMessage, OutboxStatus

logger = logging.getLogger(__name__)


async def ensure_broker_connected(broker_instance) -> bool:
    """Ensures broker connection is active before publishing."""
    if hasattr(broker_instance, "_connection") and not broker_instance._connection:
        try:
            await broker_instance.connect()
            from app.broker.rabbitmq import declare_topology
            await declare_topology(broker_instance)
            logger.info("Outbox: connected to broker and declared topology")
            return True
        except Exception as exc:
            logger.warning("Outbox: unable to connect to broker: %s", exc)
            return False
    return True


async def publish_pending_messages(
    session: AsyncSession,
    broker_instance=None,
    batch_size: Optional[int] = None,
) -> int:
    """
    Selects pending outbox messages using FOR UPDATE SKIP LOCKED
    and publishes them to RabbitMQ.
    Returns the count of successfully processed messages.
    """
    if broker_instance is None:
        broker_instance = default_broker

    if hasattr(broker_instance, "_connection") and not broker_instance._connection:
        connected = await ensure_broker_connected(broker_instance)
        if not connected:
            return 0

    limit = batch_size or settings.OUTBOX_BATCH_SIZE

    stmt = (
        select(OutboxMessage)
        .where(
            (OutboxMessage.status == OutboxStatus.PENDING)
            | (
                (OutboxMessage.status == OutboxStatus.FAILED)
                & (OutboxMessage.error_message.ilike("%connect broker%"))
            )
        )
        .order_by(OutboxMessage.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    result = await session.execute(stmt)
    messages = result.scalars().all()

    if not messages:
        return 0

    processed_count = 0
    for msg in messages:
        try:
            await broker_instance.publish(
                message=msg.payload,
                exchange=payments_exchange,
                routing_key="payments.new",
            )
            msg.status = OutboxStatus.PROCESSED
            msg.processed_at = datetime.now(timezone.utc)
            msg.error_message = None
            processed_count += 1
            logger.info("Outbox message %s (%s) successfully published", msg.id, msg.event_type)
        except Exception as exc:
            error_str = str(exc)
            if "connect broker" in error_str.lower() or "connection refused" in error_str.lower():
                logger.warning("Outbox message %s delayed due to broker connection: %s", msg.id, exc)
                if hasattr(broker_instance, "_connection"):
                    broker_instance._connection = None
                break
            else:
                msg.retry_count += 1
                msg.error_message = error_str
                if msg.retry_count >= 5:
                    msg.status = OutboxStatus.FAILED
                    logger.error("Outbox message %s marked as FAILED after %s retries: %s", msg.id, msg.retry_count, exc)
                else:
                    logger.warning("Failed to publish outbox message %s (attempt %s): %s", msg.id, msg.retry_count, exc)

    await session.commit()
    return processed_count


class OutboxPublisher:
    """
    Background worker that continuously polls the outbox table
    and publishes pending events to the message broker.
    """

    def __init__(self, poll_interval: Optional[float] = None, broker_instance=None):
        self.poll_interval = poll_interval or settings.OUTBOX_POLL_INTERVAL
        self.broker = broker_instance or default_broker
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self._trigger_event = asyncio.Event()

    def trigger(self) -> None:
        """Wakes up the publisher to process immediately without waiting for poll interval."""
        self._trigger_event.set()

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("OutboxPublisher started with poll_interval=%.2fs", self.poll_interval)

    async def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
        self._trigger_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("OutboxPublisher stopped")

    async def _run_loop(self) -> None:
        while self._is_running:
            try:
                async with AsyncSessionLocal() as session:
                    await publish_pending_messages(session, self.broker)
            except Exception as exc:
                logger.exception("Unexpected error in OutboxPublisher loop: %s", exc)

            try:
                await asyncio.wait_for(self._trigger_event.wait(), timeout=self.poll_interval)
                self._trigger_event.clear()
            except asyncio.TimeoutError:
                pass


outbox_publisher = OutboxPublisher()
