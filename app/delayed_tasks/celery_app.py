from celery import Celery


BROKER_URL = "amqp://guest:guest@rabbbitmq:5672//"

RESULT_BACKEND = "rpc://"

celery_app = Celery(
    "my_app",  
    broker=BROKER_URL,
    backend=RESULT_BACKEND,

)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)