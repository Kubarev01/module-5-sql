# services/book_service.py
class BookService:
    def __init__(self, repo, redis = None):
        self.repo = repo
        self.redis = redis

    def create(self, data):
        return self.repo.create(data)

    def create_book_with_author(self, book_data, author_data):
        return self.repo.create_book_with_author(book_data, author_data)

    def get_by_id(self, book_id):
        return self.repo.get_by_id(book_id)

    def update_by_id(self, book_id, new_data):
        updated = self.repo.update_by_id(book_id, new_data)
        if updated:
            self.redis.publish("cache:invalidate", str(book_id))
        return updated

    def delete_by_id(self, book_id):
        return self.repo.delete_by_id(book_id)

