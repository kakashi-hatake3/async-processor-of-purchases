from contextlib import asynccontextmanager
import logging
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.payments import router as payments_router
from app.broker.rabbitmq import broker, declare_topology
from app.config import settings
from app.services.outbox import outbox_publisher

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("payments_api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Starting Payments API service...")
    try:
        await broker.connect()
        await declare_topology(broker)
        logger.info("Connected to RabbitMQ and declared topology")
    except Exception as exc:
        logger.warning(
            "Could not connect to RabbitMQ on API startup (Outbox publisher will retry on next poll): %s",
            exc,
        )

    await outbox_publisher.start()
    yield
    logger.info("Shutting down Payments API service...")
    await outbox_publisher.stop()
    try:
        await broker.close()
    except Exception:
        pass
    logger.info("Payments API stopped")


app = FastAPI(
    title="Async Payment Processing Service",
    description="Microservice for asynchronous payment processing with Outbox pattern, RabbitMQ, and Webhook notifications.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(payments_router, prefix="/api/v1")


@app.get("/health", tags=["Health"], summary="Service Health Check")
async def health_check() -> dict:
    return {
        "status": "healthy",
        "service": "async-processor-of-purchases",
        "environment": settings.APP_ENV,
    }
