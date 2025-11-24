import time
import structlog
from app.logging_config import setup_logging
from app.delayed_tasks.celery_app import celery_app

setup_logging("order-worker")
log = structlog.get_logger()


@celery_app.task(name="process_order", bind=True)
def process_order(self, order_id: int):
    max_retries = 3

    try:
        log.info(
            "process_order_start",
            order_id=order_id,
        )

        time.sleep(10)

        if order_id == 42:
            # Симуляция падения БД
            raise Exception("db_failure_simulation")

        log.info(
            "process_order_completed",
            order_id=order_id,
        )

        return {"status": "completed", "order_id": order_id}

    except Exception as exc:
        attempt = self.request.retries + 1

        log.error(
            "process_order_error",
            order_id=order_id,
            error=str(exc),
            attempt=attempt,
            max_retries=max_retries,
        )

        # retry оставляем как есть
        raise self.retry(exc=exc, max_retries=max_retries, countdown=5)


@celery_app.task(name="nightly_report", bind=True)
def nightly_report(self):
    log.info("nightly_report_start")

    time.sleep(10)

    log.info("nightly_report_completed")

    return {"status": "completed"}