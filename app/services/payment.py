import logging
from typing import Optional, Tuple
import uuid

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage, OutboxStatus
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreate

logger = logging.getLogger(__name__)


def _compare_payloads(payment: Payment, data: PaymentCreate) -> bool:
    """Verifies that the new request data matches the existing payment."""
    if payment.amount != data.amount:
        return False
    if payment.currency != data.currency:
        return False
    if payment.description != data.description:
        return False
    if str(payment.webhook_url) != str(data.webhook_url):
        return False
    existing_meta = payment.metadata_info or {}
    new_meta = data.metadata or {}
    if existing_meta != new_meta:
        return False
    return True


async def get_payment_by_idempotency_key(
    session: AsyncSession,
    idempotency_key: str,
) -> Optional[Payment]:
    stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_payment_by_id(
    session: AsyncSession,
    payment_id: uuid.UUID,
) -> Payment:
    stmt = select(Payment).where(Payment.id == payment_id)
    result = await session.execute(stmt)
    payment = result.scalar_one_or_none()
    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Payment {payment_id} not found",
        )
    return payment


async def create_payment_with_outbox(
    session: AsyncSession,
    data: PaymentCreate,
    idempotency_key: str,
) -> Tuple[Payment, bool]:
    """
    Creates a payment and an outbox event in a single database transaction.
    Returns a tuple of (Payment, created: bool).
    If idempotency key already exists with identical payload, returns (existing_payment, False).
    If payload differs, raises 409 Conflict.
    """
    # 1. Quick check for existing idempotency key
    existing = await get_payment_by_idempotency_key(session, idempotency_key)
    if existing:
        if not _compare_payloads(existing, data):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key conflict: request payload differs from existing payment",
            )
        return existing, False

    payment = Payment(
        id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata_info=data.metadata or {},
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=str(data.webhook_url),
    )
    session.add(payment)
    await session.flush()

    outbox_entry = OutboxMessage(
        id=uuid.uuid4(),
        event_type="payments.new",
        payload={
            "payment_id": str(payment.id),
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "description": payment.description,
            "metadata": payment.metadata_info,
            "webhook_url": payment.webhook_url,
            "created_at": payment.created_at.isoformat(),
        },
        status=OutboxStatus.PENDING,
    )
    session.add(outbox_entry)

    try:
        await session.commit()
        await session.refresh(payment)
        logger.info("Payment %s created with outbox event %s", payment.id, outbox_entry.id)
        try:
            from app.services.outbox import outbox_publisher
            outbox_publisher.trigger()
        except Exception:
            pass
        return payment, True
    except IntegrityError:
        await session.rollback()
        # Race condition handling: check if another concurrent request committed first
        concurrent_existing = await get_payment_by_idempotency_key(session, idempotency_key)
        if concurrent_existing:
            if not _compare_payloads(concurrent_existing, data):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key conflict: request payload differs from existing payment",
                )
            return concurrent_existing, False
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database integrity error while creating payment",
        )
