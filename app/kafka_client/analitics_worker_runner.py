
import asyncio
import structlog
from app.logging_config import setup_logging
from app.kafka_client.analitics_worker import AnaliticsWorker

setup_logging("order-worker")
log = structlog.get_logger()

async def main():
    worker = AnaliticsWorker()
    try:
        await worker.start()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        log.info("Received interrupt, shutting down...")
    finally:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())