import time
import logging

import singer
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.json_result import DictJsonResult


class MillisLoggingCursor(SnowflakeCursor):
    def execute(self, command, **kwargs):
        timestamp = time.monotonic()

        try:
            super(MillisLoggingCursor, self).execute(command, **kwargs)
        finally:
            self.connection.LOGGER.info(
            "MillisLoggingCursor: {} millis spent executing: {}".format(
                int((time.monotonic() - timestamp) * 1000),
                command
            ))

        return self


class MillisLoggingDictCursor(MillisLoggingCursor):
    def __init__(self, connection):
        MillisLoggingCursor.__init__(self, connection, DictJsonResult)


class Connection(SnowflakeConnection):
    def __init__(self, **kwargs):
        self.LOGGER = singer.get_logger()

        self.configured_warehouse = kwargs.get('warehouse')
        self.configured_database = kwargs.get('database')
        self.configured_schema = kwargs.get('schema')

        SnowflakeConnection.__init__(self, **kwargs)

    def cursor(self, as_dict=False):
        cursor_class = MillisLoggingCursor
        if as_dict:
            cursor_class = MillisLoggingDictCursor

        return SnowflakeConnection.cursor(self, cursor_class)

    def initialize(self, logger):
        self.LOGGER = logger


def connect(**kwargs):
    return Connection(**kwargs)
