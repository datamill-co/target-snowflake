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

        # level = logging.getLevelName('DEBUG')
        # self.LOGGER.setLevel(level)

        self.__objects_set = False
        SnowflakeConnection.__init__(self, **kwargs)
        self.__set_current_objects()

    def __set_current_objects(self):
        if self.__objects_set:
            return None
        
        self.__objects_set = True

        self._set_current_objects()

    def cursor(self, as_dict=False):
        cursor_class = MillisLoggingCursor
        if as_dict:
            cursor_class = MillisLoggingDictCursor

        return SnowflakeConnection.cursor(self, cursor_class)

    def initialize(self, logger):
        self.LOGGER = logger


def connect(**kwargs):
    return Connection(**kwargs)
