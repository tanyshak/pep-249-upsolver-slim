"""
An abstract database cursor implementation, conformant with PEP 249.

Because it is common for database connections to implement the execute
functionality of the cursor, returning a cursor containing the results,
this is implemented using a set of mixins:

 - CursorExecuteMixin
 - CursorFetchMixin
 - CursorSetSizeMixin

"""
from abc import ABCMeta, abstractmethod
from typing import Optional, Sequence, Type, TypeVar, Union
from upsolver.exceptions import NotSupportedError, InterfaceError
from upsolver.transactions import TransactionFreeContextMixin, TransactionContextMixin
from upsolver.types_definitions import (
    QueryParameters,
    ResultRow,
    ResultSet,
    SQLQuery,
    ColumnDescription,
    ProcName,
    ProcArgs,
)

from upsolver.logging_utils import logger

CursorType = TypeVar("CursorType", "Cursor", "TransactionalCursor")


class CursorExecuteMixin(metaclass=ABCMeta):
    """
    The execute portions of a PEP 249 compliant Cursor protocol.

    This could also be used to implement 'execute' support within the
    database connection, as SQLite does.

    """

    def execute(
        self: CursorType,
        operation: SQLQuery,
        parameters: Optional[QueryParameters] = None,
    ) -> CursorType:
        """
        Execute an SQL query. Values may be bound by passing parameters
        as outlined in PEP 249.

        """
        logger.debug(f"pep249 execute {self.__class__.__name__} query '{SQLQuery}'")

    def executemany(
        self: CursorType,
        operation: SQLQuery,
        seq_of_parameters: Sequence[QueryParameters],
    ) -> CursorType:
        """
        Execute an SQL query, parameterising the query with sequences
        or mappings passed as parameters.

        """
        logger.debug(f"pep249 executemany {self.__class__.__name__}")

    def callproc(
        self: CursorType, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        """
        Execute an SQL stored procedure, passing the sequence of parameters.
        The parameters should contain one entry for each procedure argument.

        The result of the call is returned as a modified copy of the input
        parameters. The procedure may also provide a result set, which
        can be made available through the standard fetch methods.

        """
        logger.debug(f"pep249 callproc {self.__class__.__name__}")
        

class CursorSetSizeMixin(metaclass=ABCMeta):
    """An implementation of size setting for cursor."""

    def setinputsizes(
        self: CursorType, sizes: Sequence[Optional[Union[int, Type]]]
    ) -> None:
        """
        Can be used before a call to `execute` to predefine memory areas
        for the operation's parameters.

        `sizes` is a sequence containing an item - a type, or an integer
        specifying the maximum length for a string - for each input
        parameter. If the item is None, no memory will be reserved for
        that column.

        Implementations are free to have this method do nothing.
        """
        logger.debug(f"pep249 setinputsizes {self.__class__.__name__}")

    def setoutputsize(self: CursorType, size: int, column: Optional[int]) -> None:
        """
        Can be used before a call to `execute` to predefine buffer
        sizes for fetches of 'large' columns (e.g. LONG, BLOB, etc.).

        `size` is an int, referring to the size of the column.

        `column` is an optional int, referring to the index in the
        result sequence. If this is not provided, this will set
        the default size for all 'large' columns in the cursor.

        Implementations are free to have this method do nothing.
        """
        logger.debug(f"pep249 setoutputsize {self.__class__.__name__}")
        

class CursorFetchMixin(metaclass=ABCMeta):
    """The fetch portions of a PEP 249 compliant Cursor protocol."""

    @property
    def description(self: CursorType) -> Optional[Sequence[ColumnDescription]]:
        """
        A read-only attribute returning a sequence containing a description
        (a seven-item sequence) for each column in the result set.

        The values returned for each column are outlined in the PEP:
        https://www.python.org/dev/peps/pep-0249/#description

        If there is no result set, return None.
        """
        logger.debug(f"pep249 description {self.__class__.__name__}")

    @property
    def rowcount(self: CursorType) -> int:
        """
        A read-only attribute returning the number of rows that the last
        execute call returned (for e.g. SELECT calls) or affected (for e.g.
        UPDATE/INSERT calls).

        If no execute has been performed or the rowcount cannot be determined,
        this should return -1.
        """
        logger.debug(f"pep249 rowcount {self.__class__.__name__}")

    @property
    def arraysize(self: CursorType) -> int:
        """
        An attribute specifying the number of rows to fetch at a time with
        `fetchmany`.

        Defaults to 1, meaning fetch a single row at a time.
        """
        logger.debug(f"pep249 arraysize {self.__class__.__name__}")

        return getattr(self, "_arraysize", 1)

    @arraysize.setter
    def arraysize(self: CursorType, value: int):
        setattr(self, "_arraysize", value)

        logger.debug(f"pep249 arraysize {self.__class__.__name__}")

    def fetchone(self: CursorType) -> Optional[ResultRow]:
        """
        Fetch the next row from the query result set as a sequence of Python
        types (or return None when no more rows are available).

        If the previous call to `execute` did not produce a result set, an
        error can be raised.

        """
        logger.debug(f"pep249 fetchone {self.__class__.__name__}")

    def fetchmany(self: CursorType, size: Optional[int] = None) -> Optional[ResultSet]:
        """
        Fetch the next `size` rows from the query result set as a list
        of sequences of Python types.

        If the size parameter is not supplied, the arraysize property will
        be used instead.

        If rows in the result set have been exhausted, an an empty list
        will be returned. If the previous call to `execute` did not
        produce a result set, an error can be raised.

        """
        logger.debug(f"pep249 fetchmany {self.__class__.__name__}")

    def fetchall(self: CursorType) -> ResultSet:
        """
        Fetch the remaining rows from the query result set as a list of
        sequences of Python types.

        If rows in the result set have been exhausted, an an empty list
        will be returned. If the previous call to `execute` did not
        produce a result set, an error can be raised.

        """
        logger.debug(f"pep249 fetchall {self.__class__.__name__}")

    def nextset(self: CursorType) -> Optional[bool]:
        """
        Skip the cursor to the next available result set, discarding
        rows from the current set. If there are no more sets, return
        None, otherwise return True.

        This method is optional, as not all databases implement multiple
        result sets.
        """
        logger.debug(f"pep249 nextset {self.__class__.__name__}")


class BaseCursor(
    CursorFetchMixin, CursorExecuteMixin, CursorSetSizeMixin, metaclass=ABCMeta
):
    """A Cursor without an associated context."""


class Cursor(TransactionFreeContextMixin, BaseCursor, metaclass=ABCMeta):
    """A PEP 249 compliant Cursor protocol."""
    def __init__(self, connection):
        self._connection = connection
        self._result = None
        self._iterator = None

    def execute(self, operation: SQLQuery, parameters: Optional[QueryParameters] = None):
        # TODO: handle parameters
        logger.debug(f"pep249 _query {self.__class__.__name__} query '{operation}'")

        result = self._query(operation)
        self._result = result
        self._iterator = iter(result)
        return result

    def executemany(self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]) -> CursorType:
        raise NotSupportedError

    def callproc(self: CursorType, procname: ProcName, parameters: Optional[ProcArgs] = None) -> Optional[ProcArgs]:
        # TODO: implement
        pass

    def description(self: CursorType) -> Optional[Sequence[ColumnDescription]]:
        # TODO: implement
        pass

    @property
    def rowcount(self: CursorType) -> int:
        if self._result is None:
            return -1

        return len(self._result)

    def fetchone(self: CursorType) -> Optional[ResultRow]:
        if self._iterator is None:
            raise InterfaceError

        try:
            return next(self._iterator)
        except StopIteration:
            return None

    def fetchmany(self: CursorType, size: Optional[int] = None) -> Optional[ResultSet]:
        if self._iterator is None:
            raise InterfaceError

        result = []
        for _ in range(size or self.arraysize):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def fetchall(self: CursorType) -> ResultSet:
        if self._iterator is None:
            raise InterfaceError

        result = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def nextset(self: CursorType) -> Optional[bool]:
        raise NotSupportedError

    def close(self):
        self._connection = None
        TransactionFreeContextMixin.close(self)

    def _query(self, q):
        if not self._connection:
            raise InterfaceError

        result = self._connection.query(q)
        logger.debug(f"pep249 _query {self.__class__.__name__} query '{q}'")
        return result


class TransactionalCursor(
    TransactionContextMixin,
    BaseCursor,
    metaclass=ABCMeta,
):
    """
    A slightly non-compliant Cursor for a database which implements
    transactions on a per-cursor level.

    """
