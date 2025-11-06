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
    enable_utc=True
)

celery_app.conf.beat_schedule = {
    "nightly-report-every-5-min":{
        "task": "delayed_tasks.worker_service.nightly_report",
        "schedule": 5*60,
        "args": (),
    }
}