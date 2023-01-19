"""
Implementation of cursor by the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

"""
from typing import Optional, Sequence, Type, Union

from upsolver.logging_utils import logger
from upsolver.exceptions import NotSupportedError, InterfaceError
from upsolver.types_definitions import (
    QueryParameters,
    ResultRow,
    ResultSet,
    SQLQuery,
    ColumnDescription,
    ProcName,
    ProcArgs,
)


class Cursor:
    """A PEP 249 compliant Cursor protocol."""
    def __init__(self, connection):
        self._connection = connection
        self._arraysize = 1
        self._result = None
        self._iterator = None

    def execute(self, operation: SQLQuery, parameters: Optional[QueryParameters] = None):
        """
        Execute an SQL query. Values may be bound by passing parameters
        as outlined in PEP 249.

        """
        logger.debug(f"pep249 execute {self.__class__.__name__} query '{operation}'")

        # TODO: handle parameters
        result = self._query(operation)
        self._result = result
        self._iterator = iter(result)
        return result

    def executemany(self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]):
        raise NotSupportedError

    def callproc(self, procname: ProcName, parameters: Optional[ProcArgs] = None) -> Optional[ProcArgs]:
        """
        Execute an SQL stored procedure, passing the sequence of parameters.
        The parameters should contain one entry for each procedure argument.

        The result of the call is returned as a modified copy of the input
        parameters. The procedure may also provide a result set, which
        can be made available through the standard fetch methods.

        """
        logger.debug(f"pep249 callproc {self.__class__.__name__}")
        # TODO: implement

    @property
    def description(self) -> Optional[Sequence[ColumnDescription]]:
        """
        A read-only attribute returning a sequence containing a description
        (a seven-item sequence) for each column in the result set.

        The values returned for each column are outlined in the PEP:
        https://www.python.org/dev/peps/pep-0249/#description

        If there is no result set, return None.
        """
        logger.debug(f"pep249 description {self.__class__.__name__}")
        # TODO: implement

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
    def arraysize(self) -> int:
        """
        An attribute specifying the number of rows to fetch at a time with
        `fetchmany`.

        Defaults to 1, meaning fetch a single row at a time.
        """
        logger.debug(f"pep249 arraysize {self.__class__.__name__}")

        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        logger.debug(f"pep249 arraysize {self.__class__.__name__}")

        if value > 0:
            self._arraysize = value
        else:
            raise InterfaceError

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

    def nextset(self) -> Optional[bool]:
        """
        Skip the cursor to the next available result set, discarding
        rows from the current set. If there are no more sets, return
        None, otherwise return True.
        """
        # TODO: check if it is possible to support
        raise NotSupportedError

    def setinputsizes(self, sizes: Sequence[Optional[Union[int, Type]]]) -> None:
        raise NotSupportedError

    def setoutputsize(self, size: int, column: Optional[int]) -> None:
        raise NotSupportedError

    def close(self) -> None:
        logger.debug(f"pep249 close {self.__class__.__name__}")
        self._connection = None

    def _query(self, sql_query):
        if not self._connection:
            raise InterfaceError

        result = self._connection.query(sql_query)
        return result
