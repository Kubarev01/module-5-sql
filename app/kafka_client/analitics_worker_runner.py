
import asyncio
from kafka_client.analitics_worker import AnaliticsWorker

async def main():
    worker = AnaliticsWorker()
    try:
        await worker.start()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Received interrupt, shutting down...")
    finally:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())