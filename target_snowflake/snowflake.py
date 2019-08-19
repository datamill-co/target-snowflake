from copy import deepcopy
import re
import snowflake.connector

from psycopg2 import sql
from target_postgres import json_schema
from target_postgres.postgres import PostgresError, PostgresTarget, RESERVED_NULL_DEFAULT
from target_postgres.singer_stream import (
    SINGER_LEVEL
)
from target_postgres.sql_base import SEPARATOR


class SnowflakeError(PostgresError):
    """
    Raise this when there is an error with regards to Snowflake streaming
    """

class SnowflakeTarget(PostgresTarget):
    """
    Specific Snowflake implementation of a Singer Target.
    """

    IDENTIFIER_FIELD_LENGTH = 127
    DEFAULT_COLUMN_LENGTH = 1000
    MAX_VARCHAR = 65535

    def __init__(self, connection, *args, schema='public', logging_level=None, persist_empty_tables=False, **kwargs):
        self.LOGGER.info('SnowflakeTarget created...')

        if logging_level:
            level = logging.getLevelName(logging_level)
            self.LOGGER.setLevel(level)

        # try:
        #     connection.initialize(self.LOGGER)
        #     self.LOGGER.debug('SnowflakeTarget set to log all queries.')
        # except AttributeError:
        #     self.LOGGER.debug('SnowflakeTarget disabling logging all queries.')

        self.conn = connection
        self.postgres_schema = schema
        self.persist_empty_tables = persist_empty_tables
        if self.persist_empty_tables:
            self.LOGGER.debug('SnowflakeTarget is persisting empty tables')


    def write_batch(self, stream_buffer):
        # WARNING: Using mutability here as there's no simple way to copy the necessary data over
        nullable_stream_buffer = stream_buffer
        nullable_stream_buffer.schema = _make_schema_nullable(stream_buffer.schema)

        return PostgresTarget.write_batch(self, nullable_stream_buffer)

    def upsert_table_helper(self, connection, table_schema, metadata, log_schema_changes=True):
        nullable_table_schema = deepcopy(table_schema)
        nullable_table_schema['schema'] = _make_schema_nullable(nullable_table_schema['schema'])
        return PostgresTarget.upsert_table_helper(self,
                                                  connection,
                                                  nullable_table_schema,
                                                  metadata,
                                                  log_schema_changes=log_schema_changes)

    def add_table(self, cur, name, metadata):
        self._validate_identifier(name)

        create_table_sql = sql.SQL('CREATE TABLE {}.{}()').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(name))

        cur.execute(sql.SQL('{};').format(
            create_table_sql))

        self._set_table_metadata(cur, name, {'version': metadata.get('version', None)})

        self.add_column_mapping(cur,
                                name,
                                (self.CREATE_TABLE_INITIAL_COLUMN,),
                                self.CREATE_TABLE_INITIAL_COLUMN,
                                json_schema.make_nullable({'type': json_schema.BOOLEAN}))

    def sql_type_to_json_schema(self, sql_type, is_nullable):
        if sql_type == 'character varying':
            schema = {'type': [json_schema.STRING]}
            if is_nullable:
                return json_schema.make_nullable(schema)
            return schema

        return PostgresTarget.sql_type_to_json_schema(self, sql_type, is_nullable)

    def json_schema_to_sql_type(self, schema):
        psql_type = PostgresTarget.json_schema_to_sql_type(self, schema)

        max_length = schema.get('maxLength', self.default_column_length)
        if max_length > self.MAX_VARCHAR:
            max_length = self.MAX_VARCHAR

        if psql_type.upper() == 'TEXT':
            return 'varchar({})'.format(max_length)
        elif psql_type.upper() == 'TEXT NOT NULL':
            return 'varchar({}) NOT NULL'.format(max_length)

        return psql_type

    def persist_csv_rows(self,
                         cur,
                         remote_schema,
                         temp_table_name,
                         columns,
                         csv_rows):
        key_prefix = temp_table_name + SEPARATOR

        bucket, key = self.s3.persist(csv_rows,
                                      key_prefix=key_prefix)

        credentials = self.s3.credentials()

        copy_sql = sql.SQL('COPY {}.{} ({}) FROM {} CREDENTIALS {} FORMAT AS CSV NULL AS {}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.Literal('s3://{}/{}'.format(bucket, key)),
            sql.Literal('aws_access_key_id={};aws_secret_access_key={}'.format(
                credentials.get('aws_access_key_id'),
                credentials.get('aws_secret_access_key'))),
            sql.Literal(RESERVED_NULL_DEFAULT))

        cur.execute(copy_sql)

        pattern = re.compile(SINGER_LEVEL.format('[0-9]+'))
        subkeys = list(filter(lambda header: re.match(pattern, header) is not None, columns))

        update_sql = self._get_update_sql(remote_schema['name'],
                                          temp_table_name,
                                          remote_schema['key_properties'],
                                          columns,
                                          subkeys)

        cur.execute(update_sql)
