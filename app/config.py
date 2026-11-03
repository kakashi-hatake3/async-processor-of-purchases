from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_ENV: str = "development"
    DEBUG: bool = False
    PORT: int = 8000
    API_KEY: str = "test-secret-api-key-12345"

    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/payments_db"
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"

    OUTBOX_POLL_INTERVAL: float = 2.0
    OUTBOX_BATCH_SIZE: int = 50

    WEBHOOK_TIMEOUT: float = 10.0
    WEBHOOK_MAX_RETRIES: int = 3
    WEBHOOK_BACKOFF_FACTOR: float = 1.0

    GATEWAY_MIN_DELAY: float = 2.0
    GATEWAY_MAX_DELAY: float = 5.0
    GATEWAY_SUCCESS_RATE: float = 0.9

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
