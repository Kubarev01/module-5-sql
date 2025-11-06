# worker_service.py
import time
from delayed_tasks.celery_app import celery_app


@celery_app.task(name="process_order",bind=True)
def process_order(self,order_id: int):
    try:
        print(f"[process_order] Начинаю обработку заказа {order_id}")
        time.sleep(10)  
        if order_id == 42:
            raise Exception("Симуляция падения БД")
        print(f"[process_order] Заказ {order_id} обработан")
        return {"status": "completed", "order_id": order_id}
    except Exception as exc:
        print(
            f"[process_order] Ошибка при обработке заказа {order_id}: {exc}. "
            f"Попытка {self.request.retries + 1} из {3}"
        )
        raise self.retry(exc=exc,max_retries=3,countdown=5)
