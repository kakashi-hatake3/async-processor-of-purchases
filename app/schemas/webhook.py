from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field

from app.models.payment import Currency, PaymentStatus


class WebhookPayload(BaseModel):
    event: str = Field(default="payment.updated", description="Тип события вебхука")
    payment_id: uuid.UUID = Field(..., description="ID платежа")
    status: PaymentStatus = Field(..., description="Итоговый статус платежа (succeeded, failed)")
    amount: Decimal = Field(..., description="Сумма платежа")
    currency: Currency = Field(..., description="Валюта платежа")
    description: str = Field(..., description="Описание платежа")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Метаданные")
    processed_at: datetime = Field(..., description="Дата и время обработки")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "event": "payment.updated",
                "payment_id": "4b68e98e-4a69-4e78-bcce-78a08c02824b",
                "status": "succeeded",
                "amount": "1500.50",
                "currency": "RUB",
                "description": "Оплата заказа #12345",
                "metadata": {"order_id": 12345},
                "processed_at": "2026-11-04T12:00:03Z"
            }
        }
    )
