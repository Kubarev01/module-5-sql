from fastapi import BackgroundTasks
from kafka_client.producer import producer
import asyncio
import json

# для отправки сообщения в отдельном потоке в kafka

def send_book_view_event(topic: str, book_id: int):
    payload = json.dumps({"book_id": book_id}).encode("utf-8")
    producer.produce(topic=topic, value=payload)
    producer.flush()


async def send_book_view_in_thread(topic: str, book_id: int):
    await asyncio.to_thread(send_book_view_event, topic, book_id)

class BookService:
    def __init__(self, repo, redis = None):
        self.repo = repo
        self.redis = redis

    def create(self, data):
        return self.repo.create(data)

    def create_book_with_author(self, book_data, author_data):
        return self.repo.create_book_with_author(book_data, author_data)

    def get_by_id(self, book_id: int, background_tasks: BackgroundTasks):
        
        background_tasks.add_task(
            send_book_view_in_thread,
            "book_views",
            book_id,
        )
        return self.repo.get_by_id(book_id)

    def update_by_id(self, book_id, new_data):
        updated = self.repo.update_by_id(book_id, new_data)
        if updated:
            self.redis.publish("cache:invalidate", str(book_id))
        return updated

    def delete_by_id(self, book_id):
        return self.repo.delete_by_id(book_id)

