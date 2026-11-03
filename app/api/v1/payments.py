import uuid
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_idempotency_key, verify_api_key
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentResponse
from app.services.payment import create_payment_with_outbox, get_payment_by_id

router = APIRouter(
    prefix="/payments",
    tags=["Payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a new payment",
    description="Accepts a payment request, stores it in the database and registers an outbox event.",
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: str = Depends(get_idempotency_key),
    db: AsyncSession = Depends(get_db),
) -> PaymentAccepted:
    payment, _ = await create_payment_with_outbox(
        session=db,
        data=data,
        idempotency_key=idempotency_key,
    )
    return PaymentAccepted(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    status_code=status.HTTP_200_OK,
    summary="Get payment details",
    description="Retrieves detailed information about a payment by its unique ID.",
)
async def get_payment(
    payment_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> PaymentResponse:
    payment = await get_payment_by_id(session=db, payment_id=payment_id)
    return PaymentResponse.from_orm_payment(payment)
