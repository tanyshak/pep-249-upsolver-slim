"""
Implementation of connection by the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

"""

from upsolver.logging_utils import logger
from upsolver.exceptions import NotSupportedError, InterfaceError
from upsolver.cursor import Cursor, check_closed

from cli.upsolver.query import RestQueryApi
from cli.upsolver.requester import Requester
from cli.upsolver.poller import SimpleResponsePoller
from cli.upsolver.auth_filler import TokenAuthFiller
from cli.utils import convert_time_str


def connect(token, api_url):
    logger.debug(f"pep249 Creating connection for object ")
    return Connection(token, api_url)


class Connection:
    """A PEP 249 compliant Connection protocol."""

    def __init__(self, token, api_url, timeout_sec='60s'):
        # TODO: timeout generic
        self._api = RestQueryApi(
            requester=Requester(
                base_url=api_url,
                auth_filler=TokenAuthFiller(token)
            ),
            poller_builder=lambda to_sec: SimpleResponsePoller(max_time_sec=to_sec)
        )

        self._timeout_sec = convert_time_str(None, None, timeout_sec)
        self._closed = False

    @check_closed
    def cursor(self):
        logger.debug(f"pep249 Cursor creating for object {self.__class__.__name__}")

        if self._api is None:
            raise InterfaceError

        return Cursor(self)

    @check_closed
    def close(self) -> None:
        logger.debug(f"pep249 close {self.__class__.__name__}")
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    @check_closed
    def query(self, command):
        logger.debug(f"pep249 Execute query")
        if self._api is None:
            raise InterfaceError

        # TODO: response as generator
        # TODO: handle command from file
        responses = []
        for res in self._api.execute(command, self._timeout_sec):
            for res_part in res:
                response_kind = res_part.get("kind")
                if response_kind == "upsolver_query_response":
                    responses.append(res_part.get("message"))
                else:
                    responses.append(res_part)
        return responses
