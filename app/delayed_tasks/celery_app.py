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
    task_acks_late=True,     
    task_reject_on_worker_lost=True,
    timezone="UTC",
    beat_schedule={
        "every-5-seconds": {
            "task": "delayed_tasks.worker_service.process_order",
            "schedule": 5.0,
            "args": (),
        }
    }
    enable_utc=True
)