import asyncio
import logging

from faststream import FastStream

import app.broker.consumer  # noqa: F401 - registers @broker.subscriber handler
from app.broker.rabbitmq import broker, declare_topology
from app.config import settings

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] [Worker] %(name)s: %(message)s",
)
logger = logging.getLogger("payments_worker")

app = FastStream(broker)


@app.after_startup
async def after_startup() -> None:
    logger.info("Worker: connected to broker, declaring RabbitMQ topology...")
    await declare_topology(broker)
    logger.info("Worker: topology initialized (payments.new -> payments.dlq on failure)")


if __name__ == "__main__":
    logger.info("Starting FastStream consumer worker...")
    try:
        asyncio.run(app.run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker stopped by user")
