from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.models.payment import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(..., gt=0, decimal_places=2, max_digits=12, description="Сумма платежа")
    currency: Currency = Field(..., description="Валюта платежа (RUB, USD, EUR)")
    description: str = Field(..., min_length=1, max_length=255, description="Описание платежа")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Дополнительные метаданные")
    webhook_url: HttpUrl = Field(..., description="URL для отправки вебхука с результатом")

    @field_validator("webhook_url", mode="before")
    @classmethod
    def validate_webhook_url(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip()
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "amount": "1500.50",
                "currency": "RUB",
                "description": "Оплата заказа #12345",
                "metadata": {"order_id": 12345, "user_id": 42},
                "webhook_url": "https://example.com/api/webhook"
            }
        }
    )


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID = Field(..., description="Уникальный идентификатор платежа")
    status: PaymentStatus = Field(..., description="Текущий статус платежа")
    created_at: datetime = Field(..., description="Дата и время создания")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "payment_id": "4b68e98e-4a69-4e78-bcce-78a08c02824b",
                "status": "pending",
                "created_at": "2026-11-04T12:00:00Z"
            }
        }
    )


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID = Field(..., description="Уникальный идентификатор платежа")
    amount: Decimal = Field(..., description="Сумма платежа")
    currency: Currency = Field(..., description="Валюта платежа")
    description: str = Field(..., description="Описание платежа")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Метаданные")
    status: PaymentStatus = Field(..., description="Статус платежа (pending, succeeded, failed)")
    idempotency_key: str = Field(..., description="Ключ идемпотентности")
    webhook_url: str = Field(..., description="Webhook URL")
    created_at: datetime = Field(..., description="Дата создания")
    updated_at: datetime = Field(..., description="Дата последнего обновления")
    processed_at: Optional[datetime] = Field(None, description="Дата завершения обработки")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "payment_id": "4b68e98e-4a69-4e78-bcce-78a08c02824b",
                "amount": "1500.50",
                "currency": "RUB",
                "description": "Оплата заказа #12345",
                "metadata": {"order_id": 12345},
                "status": "succeeded",
                "idempotency_key": "order-12345-attempt-1",
                "webhook_url": "https://example.com/api/webhook",
                "created_at": "2026-11-04T12:00:00Z",
                "updated_at": "2026-11-04T12:00:03Z",
                "processed_at": "2026-11-04T12:00:03Z"
            }
        }
    )

    @classmethod
    def from_orm_payment(cls, payment: Any) -> "PaymentResponse":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_info if hasattr(payment, "metadata_info") else getattr(payment, "metadata", {}),
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            updated_at=payment.updated_at,
            processed_at=payment.processed_at,
        )
