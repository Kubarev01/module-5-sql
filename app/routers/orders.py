from fastapi import routing
from app.delayed_tasks.worker_service import process_order

router = routing.APIRouter(
    prefix="/order",
    tags=["заказы"]
)


@router.post("/", summary = "Создание нового заказа", status_code=202)
async def create_order(order_id: int):
    process_order.delay(order_id=order_id)
    return {"status": "Order processing started", "order_id": order_id}
