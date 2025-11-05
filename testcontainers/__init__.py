"""
Lightweight stub for the ``testcontainers`` package used in tests.

It exposes only what the test-suite needs: ``PostgresContainer``.
"""

from .postgres import PostgresContainer

__all__ = ["PostgresContainer"]
