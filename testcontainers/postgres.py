from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional


class PostgresContainer:
    """
    Stub implementation that provides a file-backed SQLite database.

    The real ``testcontainers`` package spins up Docker containers, which is
    unavailable in the execution environment used for tests. Exposing a thin
    compatibility layer keeps the API surface identical for the tests while
    avoiding the Docker dependency.
    """

    def __init__(
        self,
        image: str = "postgres:latest",
        *,
        dbname: str = "test",
        user: str = "test",
        password: str = "test",
    ):
        self.image = image
        self.dbname = dbname
        self.user = user
        self.password = password
        self._tmpdir: Optional[TemporaryDirectory[str]] = None
        self._connection_url: Optional[str] = None

    def __enter__(self) -> "PostgresContainer":
        self._tmpdir = TemporaryDirectory(prefix="stub-postgres-")
        db_path = Path(self._tmpdir.name) / "db.sqlite"
        # Touch the file so SQLAlchemy can open a connection immediately.
        db_path.touch(exist_ok=True)
        self._connection_url = f"sqlite:///{db_path}"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None
        self._connection_url = None

    def get_connection_url(self) -> str:
        if self._connection_url is None:
            raise RuntimeError("PostgresContainer is not running")
        return self._connection_url
