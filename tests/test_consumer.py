from decimal import Decimal
from unittest.mock import AsyncMock, patch
import uuid
import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.consumer import (
    handle_new_payment,
    process_payment_business_logic,
)
from app.broker.rabbitmq import dlx_exchange, payments_exchange
from app.models.payment import Currency, Payment, PaymentStatus
from app.schemas.payment import PaymentCreate
from app.services.gateway import simulate_gateway_processing
from app.services.payment import create_payment_with_outbox
from app.services.webhook import send_webhook


@pytest.mark.asyncio
async def test_gateway_simulation():
    # Test 100% success
    res_succ = await simulate_gateway_processing(
        payment_id=uuid.uuid4(),
        min_delay=0.0,
        max_delay=0.0,
        success_rate=1.0,
    )
    assert res_succ == PaymentStatus.SUCCEEDED

    # Test 0% success (failure)
    res_fail = await simulate_gateway_processing(
        payment_id=uuid.uuid4(),
        min_delay=0.0,
        max_delay=0.0,
        success_rate=0.0,
    )
    assert res_fail == PaymentStatus.FAILED


@pytest.mark.asyncio
async def test_webhook_success():
    payload = {"payment_id": str(uuid.uuid4()), "status": "succeeded"}
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_response = httpx.Response(200, json={"ok": True})
        mock_post.return_value = mock_response

        success = await send_webhook(
            webhook_url="https://client.site/webhook",
            payload=payload,
            max_retries=3,
            backoff_factor=0.01,
        )
        assert success is True
        assert mock_post.call_count == 1


@pytest.mark.asyncio
async def test_webhook_retries_and_succeeds():
    payload = {"payment_id": str(uuid.uuid4()), "status": "succeeded"}
    with patch("httpx.AsyncClient.post") as mock_post:
        # Fails twice, succeeds on 3rd attempt
        mock_post.side_effect = [
            httpx.ConnectError("Connection refused"),
            httpx.Response(500, request=httpx.Request("POST", "https://client.site/webhook")),
            httpx.Response(200, json={"received": True}),
        ]

        success = await send_webhook(
            webhook_url="https://client.site/webhook",
            payload=payload,
            max_retries=3,
            backoff_factor=0.01,
        )
        assert success is True
        assert mock_post.call_count == 3


@pytest.mark.asyncio
async def test_webhook_exhausts_retries():
    payload = {"payment_id": str(uuid.uuid4()), "status": "failed"}
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_post.side_effect = httpx.ConnectTimeout("Request timed out")

        success = await send_webhook(
            webhook_url="https://client.site/webhook",
            payload=payload,
            max_retries=3,
            backoff_factor=0.01,
        )
        assert success is False
        assert mock_post.call_count == 3


@pytest.mark.asyncio
async def test_consumer_business_logic(db_session: AsyncSession):
    data = PaymentCreate(
        amount=Decimal("999.00"),
        currency=Currency.RUB,
        description="Premium subscription",
        metadata={"plan": "pro"},
        webhook_url="https://client.site/webhook",
    )
    payment, _ = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="consumer-biz-logic-1",
    )
    assert payment.status == PaymentStatus.PENDING
    assert payment.processed_at is None

    with patch("app.broker.consumer.send_webhook", new_callable=AsyncMock) as mock_send_webhook:
        processed_payment = await process_payment_business_logic(
            payment_id=payment.id,
            session=db_session,
            gateway_min_delay=0.0,
            gateway_max_delay=0.0,
            gateway_success_rate=1.0,
        )

        assert processed_payment.status == PaymentStatus.SUCCEEDED
        assert processed_payment.processed_at is not None
        mock_send_webhook.assert_called_once()
        call_kwargs = mock_send_webhook.call_args[1]
        assert call_kwargs["webhook_url"] == "https://client.site/webhook"
        assert call_kwargs["payload"]["status"] == "succeeded"
        assert call_kwargs["payload"]["payment_id"] == str(payment.id)


@pytest.mark.asyncio
async def test_consumer_skips_already_processed_payment(db_session: AsyncSession):
    data = PaymentCreate(
        amount=Decimal("100.00"),
        currency=Currency.USD,
        description="Single purchase",
        metadata={},
        webhook_url="https://client.site/webhook",
    )
    payment, _ = await create_payment_with_outbox(
        session=db_session,
        data=data,
        idempotency_key="consumer-skip-processed",
    )

    # Process first time
    with patch("app.broker.consumer.send_webhook", new_callable=AsyncMock):
        await process_payment_business_logic(
            payment_id=payment.id,
            session=db_session,
            gateway_min_delay=0.0,
            gateway_max_delay=0.0,
            gateway_success_rate=1.0,
        )

    # Second call should skip processing
    with patch("app.broker.consumer.simulate_gateway_processing") as mock_gw:
        result = await process_payment_business_logic(
            payment_id=payment.id,
            session=db_session,
        )
        assert result.status == PaymentStatus.SUCCEEDED
        mock_gw.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_retry_on_failure(mock_broker: AsyncMock):
    fake_message = AsyncMock()
    fake_message.headers = {"x-retry-count": 0}
    fake_message.ack = AsyncMock()

    payload = {"payment_id": str(uuid.uuid4())}

    with patch("app.broker.consumer.process_payment_business_logic", side_effect=RuntimeError("DB temporarily down")):
        with patch("app.broker.consumer.broker", mock_broker):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await handle_new_payment(payload=payload, message=fake_message)

    # Message must be acknowledged from current queue and republished with incremented retry count
    fake_message.ack.assert_called_once()
    mock_broker.publish.assert_called_once()
    pub_kwargs = mock_broker.publish.call_args[1]
    assert pub_kwargs["exchange"] == payments_exchange
    assert pub_kwargs["routing_key"] == "payments.new"
    assert pub_kwargs["headers"]["x-retry-count"] == 1


@pytest.mark.asyncio
async def test_consumer_routes_to_dlq_after_max_retries(mock_broker: AsyncMock):
    fake_message = AsyncMock()
    fake_message.headers = {"x-retry-count": 2}  # 3rd attempt
    fake_message.ack = AsyncMock()

    payload = {"payment_id": str(uuid.uuid4())}

    with patch("app.broker.consumer.process_payment_business_logic", side_effect=RuntimeError("Unrecoverable error")):
        with patch("app.broker.consumer.broker", mock_broker):
            await handle_new_payment(payload=payload, message=fake_message)

    # Should publish to DLQ (exchange payments.dlx, routing_key payments.dlq)
    fake_message.ack.assert_called_once()
    mock_broker.publish.assert_called_once()
    pub_kwargs = mock_broker.publish.call_args[1]
    assert pub_kwargs["exchange"] == dlx_exchange
    assert pub_kwargs["routing_key"] == "payments.dlq"
    assert pub_kwargs["headers"]["x-retry-count"] == 3
    assert "Unrecoverable error" in pub_kwargs["headers"]["x-final-error"]
