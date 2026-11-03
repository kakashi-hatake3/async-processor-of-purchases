import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage
from app.models.payment import Payment


@pytest.mark.asyncio
async def test_idempotent_duplicate_request(
    client: AsyncClient,
    auth_headers: dict,
    db_session: AsyncSession,
):
    headers = {**auth_headers, "Idempotency-Key": "idempotent-key-duplicate"}
    payload = {
        "amount": "1500.00",
        "currency": "USD",
        "description": "Monthly subscription",
        "metadata": {"user": "tester"},
        "webhook_url": "https://service.org/hook",
    }

    # First request
    resp1 = await client.post("/api/v1/payments", json=payload, headers=headers)
    assert resp1.status_code == 202
    data1 = resp1.json()

    # Second identical request
    resp2 = await client.post("/api/v1/payments", json=payload, headers=headers)
    assert resp2.status_code == 202
    data2 = resp2.json()

    # Same payment_id must be returned
    assert data1["payment_id"] == data2["payment_id"]

    # Verify database has only 1 payment and 1 outbox record
    payments_stmt = select(Payment).where(Payment.idempotency_key == "idempotent-key-duplicate")
    payments_res = await db_session.execute(payments_stmt)
    all_payments = payments_res.scalars().all()
    assert len(all_payments) == 1

    outbox_stmt = select(OutboxMessage).where(OutboxMessage.payload["payment_id"].as_string() == data1["payment_id"])
    outbox_res = await db_session.execute(outbox_stmt)
    all_outbox = outbox_res.scalars().all()
    assert len(all_outbox) == 1


@pytest.mark.asyncio
async def test_idempotency_conflict_detection(client: AsyncClient, auth_headers: dict):
    headers = {**auth_headers, "Idempotency-Key": "conflict-test-key"}
    initial_payload = {
        "amount": "300.00",
        "currency": "EUR",
        "description": "Initial invoice",
        "metadata": {},
        "webhook_url": "https://service.org/hook",
    }

    resp1 = await client.post("/api/v1/payments", json=initial_payload, headers=headers)
    assert resp1.status_code == 202

    # Attempt with different amount
    conflict_amount = dict(initial_payload, amount="500.00")
    resp2 = await client.post("/api/v1/payments", json=conflict_amount, headers=headers)
    assert resp2.status_code == 409
    assert "differs from existing payment" in resp2.json()["detail"]

    # Attempt with different currency
    conflict_currency = dict(initial_payload, currency="USD")
    resp3 = await client.post("/api/v1/payments", json=conflict_currency, headers=headers)
    assert resp3.status_code == 409

    # Attempt with different description
    conflict_desc = dict(initial_payload, description="Altered invoice")
    resp4 = await client.post("/api/v1/payments", json=conflict_desc, headers=headers)
    assert resp4.status_code == 409
