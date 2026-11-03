from decimal import Decimal
from unittest.mock import AsyncMock
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage, OutboxStatus
from app.models.payment import Currency, Payment
from app.schemas.payment import PaymentCreate
from app.services.outbox import publish_pending_messages
from app.services.payment import create_payment_with_outbox


@pytest.mark.asyncio
async def test_transactional_outbox_creation(db_session: AsyncSession):
    data = PaymentCreate(
        amount=Decimal("499.99"),
        currency=Currency.RUB,
        description="Book purchase",
        metadata={"item": "book"},
        webhook_url="https://shop.local/callback",
    )

    payment, created = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="tx-outbox-test-1",
    )
    assert created is True
    assert payment.id is not None

    # Check outbox record
    stmt = select(OutboxMessage).where(OutboxMessage.event_type == "payments.new")
    result = await db_session.execute(stmt)
    outbox_entries = result.scalars().all()

    assert len(outbox_entries) == 1
    entry = outbox_entries[0]
    assert entry.status == OutboxStatus.PENDING
    assert entry.payload["payment_id"] == str(payment.id)
    assert entry.payload["amount"] == "499.99"
    assert entry.payload["currency"] == "RUB"


@pytest.mark.asyncio
async def test_outbox_publish_success(db_session: AsyncSession, mock_broker: AsyncMock):
    data = PaymentCreate(
        amount=Decimal("120.00"),
        currency=Currency.USD,
        description="Outbox publish test",
        metadata={},
        webhook_url="https://test.local/hook",
    )
    payment, _ = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="tx-publish-test",
    )

    processed_count = await publish_pending_messages(
        session=db_session,
        broker_instance=mock_broker,
    )
    assert processed_count == 1
    mock_broker.publish.assert_called_once()

    # Verify message in DB is now PROCESSED
    stmt = select(OutboxMessage).where(OutboxMessage.payload["payment_id"].as_string() == str(payment.id))
    res = await db_session.execute(stmt)
    entry = res.scalar_one()
    assert entry.status == OutboxStatus.PROCESSED
    assert entry.processed_at is not None
    assert entry.error_message is None


@pytest.mark.asyncio
async def test_outbox_publish_failure_and_retry(db_session: AsyncSession):
    failing_broker = AsyncMock()
    failing_broker.publish.side_effect = ConnectionError("RabbitMQ broker unreachable")

    data = PaymentCreate(
        amount=Decimal("50.00"),
        currency=Currency.EUR,
        description="Failure test",
        metadata={},
        webhook_url="https://test.local/hook",
    )
    payment, _ = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="tx-failure-test",
    )

    processed_count = await publish_pending_messages(
        session=db_session,
        broker_instance=failing_broker,
    )
    assert processed_count == 0

    # Verify retry count incremented and status remains PENDING
    stmt = select(OutboxMessage).where(OutboxMessage.payload["payment_id"].as_string() == str(payment.id))
    res = await db_session.execute(stmt)
    entry = res.scalar_one()
    assert entry.status == OutboxStatus.PENDING
    assert entry.retry_count == 1
    assert "RabbitMQ broker unreachable" in entry.error_message


@pytest.mark.asyncio
async def test_outbox_publish_marks_failed_after_max_retries(db_session: AsyncSession):
    failing_broker = AsyncMock()
    failing_broker.publish.side_effect = RuntimeError("Fatal broker error")

    data = PaymentCreate(
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="Max retries test",
        metadata={},
        webhook_url="https://test.local/hook",
    )
    payment, _ = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="tx-max-retries-test",
    )

    # Set retry count to 4 manually
    stmt = select(OutboxMessage).where(OutboxMessage.payload["payment_id"].as_string() == str(payment.id))
    res = await db_session.execute(stmt)
    entry = res.scalar_one()
    entry.retry_count = 4
    await db_session.commit()

    # Now publishing failure should mark it FAILED
    await publish_pending_messages(session=db_session, broker_instance=failing_broker)

    await db_session.refresh(entry)
    assert entry.status == OutboxStatus.FAILED
    assert entry.retry_count == 5


@pytest.mark.asyncio
async def test_outbox_auto_reconnects_disconnected_broker(db_session: AsyncSession):
    broker_with_reconnect = AsyncMock()
    broker_with_reconnect._connection = None

    async def fake_connect():
        broker_with_reconnect._connection = True

    broker_with_reconnect.connect = AsyncMock(side_effect=fake_connect)
    broker_with_reconnect.publish = AsyncMock(return_value=None)
    broker_with_reconnect.declare_exchange = AsyncMock(return_value=None)
    broker_with_reconnect.declare_queue = AsyncMock(return_value=AsyncMock())

    data = PaymentCreate(
        amount=Decimal("75.00"),
        currency=Currency.USD,
        description="Auto reconnect test",
        metadata={},
        webhook_url="https://test.local/hook",
    )
    payment, _ = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="tx-reconnect-test",
    )

    processed_count = await publish_pending_messages(
        session=db_session,
        broker_instance=broker_with_reconnect,
    )

    assert processed_count == 1
    broker_with_reconnect.connect.assert_called_once()
    broker_with_reconnect.publish.assert_called_once()
