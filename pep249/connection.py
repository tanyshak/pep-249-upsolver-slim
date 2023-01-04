"""
database connection implementation, conformant with PEP 249.

"""

from typing import TypeVar

from  pep249.logging import logger

from .transactions import TransactionContextMixin, DummyTransactionContextMixin
from .cursor import CursorType, Cursor


ConnectionType = TypeVar(
    "ConnectionType", "Connection", "TransactionlessConnection"
)

def connect(token, api_url):
    logger.debug(f"pep249 Creating connection for object ")
    return Connection(token, api_url)

class BaseConnection:  # pylint: disable=too-few-public-methods
    """A Connection without an associated context."""

    def cursor(self):
        logger.debug(f"pep249 Cursor creating for object {self.__class__.__name__}")
        return Cursor(self)

class Connection(TransactionContextMixin, BaseConnection):
    """A PEP 249 compliant Connection protocol."""

    def __init__(self, token, api_url):
        self.token = token
        self.api_url = api_url

    def close(self):
        logger.debug(f"pep249 close {self.__class__.__name__}")
        pass

    def commit(self):
        logger.debug(f"pep249 commit {self.__class__.__name__}")
        pass

    def rollback(self):
        logger.debug(f"pep249 rollback {self.__class__.__name__}")
        pass

    def query(self, sql):
        logger.debug(f"pep249 Query for object")
        return sql

class TransactionlessConnection(
    DummyTransactionContextMixin, BaseConnection):
    """
    A PEP 249 compliant Connection protocol for databases without
    transaction support.

    """
