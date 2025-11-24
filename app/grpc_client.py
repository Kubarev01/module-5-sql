import grpc
import structlog
from app import app_pb2, app_pb2_grpc
from app.logging_config import setup_logging

setup_logging("grpc-client")
log = structlog.get_logger()


def get_author(stub, author_id: int):
    request = app_pb2.AuthorRequest(id=author_id)
    response = stub.GetAuthor(request)
    log.info(f"Author #{response.id}: {response.name} — {response.biography}")


def list_authors(stub):
    request = app_pb2.Empty()
    for author in stub.ListAuthors(request):
        log.info(f"Author #{author.id}: {author.name}")


def main(host: str = "localhost", port: int = 50051):
    target = f"{host}:{port}"
    with grpc.insecure_channel(target) as channel:
        stub = app_pb2_grpc.AuthorServiceStub(channel)
        get_author(stub, author_id=1)
        list_authors(stub)


if __name__ == "__main__":
    main()
