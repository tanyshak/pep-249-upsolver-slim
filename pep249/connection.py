"""
An abstract database connection implementation, conformant with PEP 249.

"""
from abc import ABCMeta, abstractmethod
from typing import TypeVar
from .transactions import TransactionContextMixin, DummyTransactionContextMixin
from .cursor import CursorType

ConnectionType = TypeVar(
    "ConnectionType", "Connection", "TransactionlessConnection"
)


class BaseConnection:  # pylint: disable=too-few-public-methods
    """A Connection without an associated context."""

    #@abstractmethod
    #def cursor(self: ConnectionType) -> CursorType:
        #"""Return a database cursor."""
        #raise NotImplementedError


    def cursor(self, cursor=None):
        if cursor:
            return cursor(self)
            print('Create cursor')
        return

    # The following methods are INTERNAL USE ONLY (called from Cursor)


class Connection(TransactionContextMixin, BaseConnection, metaclass=ABCMeta):
    """A PEP 249 compliant Connection protocol."""
    def __init__(self, token, api_url):
        self.token = token
        self.api_url = api_url

    def connect(token, api_url):
        connection = Connection(token, api_url)
        print('Create connection')
        return connection

    def query(self, sql):
        return sql


class TransactionlessConnection(
    DummyTransactionContextMixin, BaseConnection, metaclass=ABCMeta
):
    """
    A PEP 249 compliant Connection protocol for databases without
    transaction support.

    """
