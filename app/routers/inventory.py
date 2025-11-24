from fastapi import APIRouter, HTTPException
from app.database.redis_client import redis_client as redis
from app.schemas import InventoryUpdate

router = APIRouter(
    prefix="/inventory",
    tags=["склад"]
)


@router.post("/", summary="Инициализировать складской запас")
async def initialize_inventory(product_id: int, initial_stock: int):
    # Редис потому что быстрый доступ нужен
    await redis.set(f"inventory:{product_id}", initial_stock)
    return {"product_id": product_id, "initial_stock": initial_stock}


@router.put("/{product_id}")
async def update_inventory(product_id: int, update: InventoryUpdate):
    lock_key = f"inventory_lock:{product_id}"

    async with redis.lock(lock_key, timeout=10, blocking_timeout=5):
        current_stock = await redis.get(f"inventory:{product_id}")
        if current_stock is None:
            current_stock = 0
        else:
            current_stock = int(current_stock)

        new_stock = current_stock + update.delta
        if new_stock < 0:
            raise HTTPException(status_code=400, detail="Недостаточно товара на складе")

        await redis.set(f"inventory:{product_id}", new_stock)

        return {
            "product_id": product_id,
            "old_stock": current_stock,
            "new_stock": new_stock
        }

@router.get("/{product_id}", summary="Получить текущий складской запас")
async def get_inventory(product_id: int):
    current_stock = await redis.get(f"inventory:{product_id}")
    if current_stock is None:
        raise HTTPException(status_code=404, detail="Запас не найден")
    return {
        "product_id": product_id,
        "current_stock": int(current_stock)
    }