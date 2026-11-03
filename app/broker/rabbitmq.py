from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

# Exchanges
payments_exchange = RabbitExchange(
    name="payments.exchange",
    type=ExchangeType.DIRECT,
    durable=True,
)

dlx_exchange = RabbitExchange(
    name="payments.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)

# Queues
dlq_queue = RabbitQueue(
    name="payments.dlq",
    routing_key="payments.dlq",
    durable=True,
)

payments_queue = RabbitQueue(
    name="payments.new",
    routing_key="payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dlq",
    },
)

# FastStream Broker
broker = RabbitBroker(settings.RABBITMQ_URL)


async def declare_topology(b: RabbitBroker) -> None:
    """Declares exchanges, queues and bindings for payments and DLQ."""
    if hasattr(b, "_connection") and not b._connection:
        await b.connect()

    # Declare DLX and DLQ
    await b.declare_exchange(dlx_exchange)
    dlq = await b.declare_queue(dlq_queue)
    if hasattr(dlq, "bind"):
        await dlq.bind(exchange=dlx_exchange.name, routing_key="payments.dlq")

    # Declare Main Exchange and Queue with DLQ arguments
    await b.declare_exchange(payments_exchange)
    main_q = await b.declare_queue(payments_queue)
    if hasattr(main_q, "bind"):
        await main_q.bind(exchange=payments_exchange.name, routing_key="payments.new")
