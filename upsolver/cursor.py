"""
Implementation of cursor by the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

"""
from functools import wraps
from typing import Optional, Sequence, Type, Union

from upsolver.logging_utils import logger
from upsolver.exceptions import NotSupportedError, InterfaceError
from upsolver.type_constructors import STRING
from upsolver.types_definitions import (
    QueryParameters,
    ResultRow,
    ResultSet,
    SQLQuery,
    ColumnDescription,
    ProcName,
    ProcArgs,
)


def check_closed(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if self.closed:
            raise InterfaceError
        return func(self, *args, **kwargs)
    return wrapped


class Cursor:
    """A PEP 249 compliant Cursor protocol."""
    def __init__(self, connection):
        self._connection = connection
        self._arraysize = 1
        self._result = None
        self._iterator = None
        self._closed = False

    @check_closed
    def execute(self, operation: SQLQuery, parameters: Optional[QueryParameters] = None):
        """
        Execute an SQL query. Values may be bound by passing parameters
        as outlined in PEP 249.

        """
        logger.debug(f"pep249 execute {self.__class__.__name__} query '{operation}'")
        if parameters is not None:
            raise NotSupportedError

        result = self._connection.query(operation)
        self._result = result
        self._iterator = iter(result)
        return result

    def executemany(self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]):
        raise NotSupportedError

    def callproc(self, procname: ProcName, parameters: Optional[ProcArgs] = None) -> Optional[ProcArgs]:
        raise NotSupportedError

    @check_closed
    @property
    def description(self) -> Optional[Sequence[ColumnDescription]]:
        """
        A read-only attribute returning a sequence containing a description
        (a seven-item sequence) for each column in the result set. The first
        item of the sequence is a column name, the second is a column type,
        which is always STRING in current implementation, other items are not
        meaningful.

        If no execute has been performed or there is no result set, return None.
        """
        logger.debug(f"pep249 description {self.__class__.__name__}")

        if self._result is None:
            return None

        example_result = self._result[0]
        return [
            (name, STRING, None, None, None, None, None)
            for name in example_result.keys()
        ]

    @check_closed
    @property
    def rowcount(self) -> int:
        """
        A read-only attribute returning the number of rows that the last
        execute call returned (for e.g. SELECT calls) or affected (for e.g.
        UPDATE/INSERT calls).

        If no execute has been performed or the rowcount cannot be determined,
        this should return -1.
        """
        logger.debug(f"pep249 rowcount {self.__class__.__name__}")

        if self._result is None:
            return -1

        return len(self._result)

    @property
    @check_closed
    def arraysize(self) -> int:
        """
        An attribute specifying the number of rows to fetch at a time with
        `fetchmany`.

        Defaults to 1, meaning fetch a single row at a time.
        """
        logger.debug(f"pep249 arraysize {self.__class__.__name__}")

        return self._arraysize

    @arraysize.setter
    @check_closed
    def arraysize(self, value: int):
        logger.debug(f"pep249 arraysize {self.__class__.__name__}")

        if value > 0:
            self._arraysize = value
        else:
            raise InterfaceError

    @check_closed
    def fetchone(self) -> Optional[ResultRow]:
        """
        Fetch the next row from the query result set as a sequence of Python
        types (or return None when no more rows are available).

        If the previous call to `execute` did not produce a result set, an
        error can be raised.

        """
        logger.debug(f"pep249 fetchone {self.__class__.__name__}")

        if self._iterator is None:
            raise InterfaceError

        try:
            return next(self._iterator)
        except StopIteration:
            return None

    @check_closed
    def fetchmany(self, size: Optional[int] = None) -> Optional[ResultSet]:
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

        if self._iterator is None:
            raise InterfaceError

        result = []
        for _ in range(size or self.arraysize):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    @check_closed
    def fetchall(self) -> ResultSet:
        """
        Fetch the remaining rows from the query result set as a list of
        sequences of Python types.

        If rows in the result set have been exhausted, an an empty list
        will be returned. If the previous call to `execute` did not
        produce a result set, an error can be raised.

        """
        logger.debug(f"pep249 fetchall {self.__class__.__name__}")

        if self._iterator is None:
            raise InterfaceError

        result = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    @check_closed
    def nextset(self) -> Optional[bool]:
        raise NotSupportedError

    def setinputsizes(self, sizes: Sequence[Optional[Union[int, Type]]]) -> None:
        raise NotSupportedError

    def setoutputsize(self, size: int, column: Optional[int]) -> None:
        raise NotSupportedError

    @check_closed
    def close(self) -> None:
        logger.debug(f"pep249 close {self.__class__.__name__}")
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
