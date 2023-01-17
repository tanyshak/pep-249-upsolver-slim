"""
database connection implementation, conformant with PEP 249.

"""

from upsolver.logging_utils import logger

from upsolver.exceptions import NotSupportedError, InterfaceError
from upsolver.transactions import TransactionContextMixin, DummyTransactionContextMixin
from upsolver.cursor import CursorType, Cursor

from cli.upsolver.query import RestQueryApi
from cli.upsolver.requester import Requester
from cli.upsolver.poller import SimpleResponsePoller
from cli.upsolver.auth_filler import TokenAuthFiller
from cli.utils import convert_time_str


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

    def __init__(self, token, api_url, timeout_sec='30s'):
        self._api = RestQueryApi(
            requester=Requester(
                base_url=api_url,
                auth_filler=TokenAuthFiller(token)
            ),
            poller_builder=lambda to_sec: SimpleResponsePoller(max_time_sec=to_sec)
        )

        self._timeout_sec = convert_time_str(None, None, timeout_sec)

    def cursor(self):
        if self._api is None:
            raise InterfaceError

        return BaseConnection.cursor(self)

    def close(self):
        logger.debug(f"pep249 close {self.__class__.__name__}")
        self._api = None

    def commit(self):
        logger.debug(f"pep249 commit {self.__class__.__name__}")
        pass

    def rollback(self):
        logger.debug(f"pep249 rollback {self.__class__.__name__}")
        raise NotSupportedError

    def query(self, command):
        logger.debug(f"pep249 Execute query")
        if self._api is None:
            raise InterfaceError

        responses = []
        for res in self._api.execute(command, self._timeout_sec):
            for res_part in res:
                response_kind = res_part.get("kind")
                if response_kind == "upsolver_query_response":
                    responses.append(res_part.get("message"))
                else:
                    responses.append(res_part)
        return responses


class TransactionlessConnection(DummyTransactionContextMixin, BaseConnection):
    """
    A PEP 249 compliant Connection protocol for databases without
    transaction support.

    """