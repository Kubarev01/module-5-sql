# app/grpc_server.py
import logging
import grpc
from concurrent import futures

from app import app_pb2, app_pb2_grpc


class AuthorService(app_pb2_grpc.AuthorServiceServicer):
    """Implementation of the RPCs defined in app.proto."""

    def GetAuthor(self, request, context):
        author_id = request.id
        author = app_pb2.AuthorReply(
            id=author_id,
            name=f"Author #{author_id}",
            biography="Generated via gRPC server",
        )
        return author

    def ListAuthors(self, request, context):
        # request: app_pb2.Empty
        for idx in range(1, 4):
            yield app_pb2.AuthorReply(
                id=idx,
                name=f"Author #{idx}",
                biography="Streaming example",
            )


def serve(port: int = 50051) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    app_pb2_grpc.add_AuthorServiceServicer_to_server(AuthorService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info("gRPC server started on port %s", port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
