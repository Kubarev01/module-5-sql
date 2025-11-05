from celery import Celery

BROKER_URL = "amqp://guest:guest@localhost:5672//"

# URL backend'а для хранения результатов (Redis)
# формат: redis://host:port/db_number
RESULT_BACKEND = "redis://localhost:6379/0"

celery_app = Celery(
    "my_app",  # имя приложения (любое осмысленное)
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
)

# Дополнительные настройки (по желанию, можно не трогать для задания)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)