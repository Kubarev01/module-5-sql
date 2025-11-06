# worker_service.py
import time
from delayed_tasks.celery_app import celery_app


@celery_app.task(name="process_order",)
def process_order(order_id: int):
    print(f"[process_order] Начинаю обработку заказа {order_id}")
    time.sleep(10)  
    print(f"[process_order] Заказ {order_id} обработан")
    return {"status": "completed", "order_id": order_id}
