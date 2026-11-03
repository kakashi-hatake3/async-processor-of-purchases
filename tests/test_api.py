import uuid
from decimal import Decimal
import pytest
from httpx import AsyncClient

from app.models.payment import Currency, PaymentStatus


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "async-processor-of-purchases"


@pytest.mark.asyncio
async def test_api_key_required(client: AsyncClient):
    random_id = str(uuid.uuid4())
    # Request without X-API-Key
    response = await client.get(f"/api/v1/payments/{random_id}")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API key"

    # Request with invalid X-API-Key
    response = await client.get(
        f"/api/v1/payments/{random_id}",
        headers={"X-API-Key": "wrong-key"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_payment_missing_idempotency_key(client: AsyncClient, auth_headers: dict):
    payload = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Test order",
        "webhook_url": "https://example.com/webhook",
    }
    response = await client.post(
        "/api/v1/payments",
        json=payload,
        headers=auth_headers,
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_create_payment_validation_errors(client: AsyncClient, auth_headers: dict):
    headers = {**auth_headers, "Idempotency-Key": "test-key-1"}

    # Negative amount
    bad_amount = {
        "amount": "-50.00",
        "currency": "RUB",
        "description": "Negative",
        "webhook_url": "https://example.com/webhook",
    }
    resp = await client.post("/api/v1/payments", json=bad_amount, headers=headers)
    assert resp.status_code == 422

    # Unsupported currency
    bad_currency = {
        "amount": "100.00",
        "currency": "GBP",
        "description": "Invalid currency",
        "webhook_url": "https://example.com/webhook",
    }
    resp = await client.post("/api/v1/payments", json=bad_currency, headers=headers)
    assert resp.status_code == 422

    # Invalid webhook URL
    bad_url = {
        "amount": "100.00",
        "currency": "USD",
        "description": "Invalid url",
        "webhook_url": "not-a-valid-url",
    }
    resp = await client.post("/api/v1/payments", json=bad_url, headers=headers)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_and_get_payment_flow(client: AsyncClient, auth_headers: dict):
    headers = {**auth_headers, "Idempotency-Key": "unique-order-101"}
    payload = {
        "amount": "2500.00",
        "currency": "RUB",
        "description": "Order #101 payment",
        "metadata": {"order_id": 101, "customer": "Alice"},
        "webhook_url": "https://client.example.com/webhook",
    }

    # 1. Create payment (202 Accepted)
    response = await client.post("/api/v1/payments", json=payload, headers=headers)
    assert response.status_code == 202
    created_data = response.json()
    assert "payment_id" in created_data
    assert created_data["status"] == PaymentStatus.PENDING.value
    assert "created_at" in created_data

    payment_id = created_data["payment_id"]

    # 2. Get payment info (200 OK)
    get_resp = await client.get(f"/api/v1/payments/{payment_id}", headers=auth_headers)
    assert get_resp.status_code == 200
    info = get_resp.json()
    assert info["payment_id"] == payment_id
    assert Decimal(str(info["amount"])) == Decimal("2500.00")
    assert info["currency"] == "RUB"
    assert info["description"] == "Order #101 payment"
    assert info["metadata"] == {"order_id": 101, "customer": "Alice"}
    assert info["status"] == "pending"
    assert info["idempotency_key"] == "unique-order-101"
    assert info["webhook_url"] == "https://client.example.com/webhook"
    assert info["processed_at"] is None


@pytest.mark.asyncio
async def test_get_payment_not_found(client: AsyncClient, auth_headers: dict):
    non_existent = str(uuid.uuid4())
    response = await client.get(f"/api/v1/payments/{non_existent}", headers=auth_headers)
    assert response.status_code == 404
    assert f"Payment {non_existent} not found" in response.json()["detail"]
