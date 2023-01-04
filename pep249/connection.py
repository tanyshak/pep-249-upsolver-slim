"""
database connection implementation, conformant with PEP 249.

"""
from typing import TypeVar
from .transactions import TransactionContextMixin, DummyTransactionContextMixin
from .cursor import CursorType, Cursor
import logging

ConnectionType = TypeVar(
    "ConnectionType", "Connection", "TransactionlessConnection"
)

logging.basicConfig(level=logging.INFO, filename="pep249.log",
                    format="%(asctime)s %(levelname)s %(message)s")

def connect(token, api_url):
    logging.debug(f"pep249 Creating connection for object ")
    print(f"pep249 Creating connection for object ")
    return Connection(token, api_url)

class BaseConnection:  # pylint: disable=too-few-public-methods
    """A Connection without an associated context."""

    def cursor(self):
        logging.debug(f"pep249 Cursor creating for object {self.__class__.__name__}")
        print(f"pep249 Cursor creating for object {self.__class__.__name__}")
        return Cursor(self)

class Connection(TransactionContextMixin, BaseConnection):
    """A PEP 249 compliant Connection protocol."""

    def __init__(self, token, api_url):
        self.token = token
        self.api_url = api_url

    def close(self):
        logging.debug(f"pep249 close {self.__class__.__name__}")
        print(f"pep249 close {self.__class__.__name__}")
        pass

    def commit(self):
        logging.debug(f"pep249 commit {self.__class__.__name__}")
        print(f"pep249 commit {self.__class__.__name__}")
        pass

    def rollback(self):
        logging.debug(f"pep249 rollback {self.__class__.__name__}")
        print(f"pep249 rollback {self.__class__.__name__}")
        pass

    def query(self, sql):
        logging.debug(f"pep249 Query for object")
        print(f"pep249 Query for object")
        return sql

class TransactionlessConnection(
    DummyTransactionContextMixin, BaseConnection):
    """
    A PEP 249 compliant Connection protocol for databases without
    transaction support.

    """
