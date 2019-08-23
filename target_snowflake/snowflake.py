from copy import deepcopy
import json
import logging
import re
import uuid

import arrow
from psycopg2 import sql
from target_postgres import json_schema
from target_postgres.postgres import PostgresError, PostgresTarget, RESERVED_NULL_DEFAULT
from target_postgres.singer_stream import (
    SINGER_LEVEL
)
from target_postgres.sql_base import SEPARATOR, SQLInterface

from target_snowflake import sql
from target_snowflake.connection import connect
from target_snowflake.exceptions import SnowflakeError

class SnowflakeTarget(SQLInterface):
    """
    Specific Snowflake implementation of a Singer Target.
    """

    IDENTIFIER_FIELD_LENGTH = 256
    DEFAULT_COLUMN_LENGTH = 1000
    MAX_VARCHAR = 65535

    CREATE_TABLE_INITIAL_COLUMN = '_sdc_target_snowflake_create_table_placeholder'
    CREATE_TABLE_INITIAL_COLUMN_TYPE = 'BOOLEAN'

    def __init__(self, connection, *args, logging_level=None, persist_empty_tables=False, **kwargs):
        self.LOGGER.info('SnowflakeTarget created...')

        if logging_level:
            level = logging.getLevelName(logging_level)
            self.LOGGER.setLevel(level)

        try:
            connection.initialize(self.LOGGER)
            self.LOGGER.debug('SnowflakeTarget set to log all queries.')
        except AttributeError:
            self.LOGGER.debug('SnowflakeTarget disabling logging all queries.')

        self.connection = connection
        self.persist_empty_tables = persist_empty_tables
        if self.persist_empty_tables:
            self.LOGGER.debug('SnowflakeTarget is persisting empty tables')

    def metrics_tags(self):
        return {'warehouse': self.connection.warehouse,
                'database': self.connection.database,
                'schema': self.connection.schema}

    def setup_table_mapping_cache(self, cur):
        self.table_mapping_cache = {}

        cur.execute(
            '''
            SHOW TABLES IN SCHEMA {}.{}
            '''.format(
                sql.identifier(self.connection.database),
                sql.identifier(self.connection.schema)))

        for row in cur.fetchall():
            mapped_name = row[1]
            raw_json = row[5]

            table_path = None
            if raw_json:
                table_path = json.loads(raw_json).get('path', None)
            self.LOGGER.info("Mapping: {} to {}".format(mapped_name, table_path))
            if table_path:
                self.table_mapping_cache[tuple(table_path)] = mapped_name

    def write_batch(self, stream_buffer):
        if not self.persist_empty_tables and stream_buffer.count == 0:
            return None

        with self.connection.cursor() as cur:
            try:
                self.setup_table_mapping_cache(cur)

                root_table_name = self.add_table_mapping_helper((stream_buffer.stream,), self.table_mapping_cache)['to']
                current_table_schema = self.get_table_schema(cur, root_table_name)

                current_table_version = None

                if current_table_schema:
                    current_table_version = current_table_schema.get('version', None)

                    if set(stream_buffer.key_properties) \
                            != set(current_table_schema.get('key_properties')):
                        raise SnowflakeError(
                            '`key_properties` change detected. Existing values are: {}. Streamed values are: {}'.format(
                                current_table_schema.get('key_properties'),
                                stream_buffer.key_properties
                            ))

                    for key_property in stream_buffer.key_properties:
                        canonicalized_key, remote_column_schema = self.fetch_column_from_path((key_property,),
                                                                                              current_table_schema)
                        if self.json_schema_to_sql_type(remote_column_schema) \
                                != self.json_schema_to_sql_type(stream_buffer.schema['properties'][key_property]):
                            raise SnowflakeError(
                                ('`key_properties` type change detected for "{}". ' +
                                 'Existing values are: {}. ' +
                                 'Streamed values are: {}, {}, {}').format(
                                    key_property,
                                    json_schema.get_type(current_table_schema['schema']['properties'][key_property]),
                                    json_schema.get_type(stream_buffer.schema['properties'][key_property]),
                                    self.json_schema_to_sql_type(
                                        current_table_schema['schema']['properties'][key_property]),
                                    self.json_schema_to_sql_type(stream_buffer.schema['properties'][key_property])
                                ))

                target_table_version = current_table_version or stream_buffer.max_version

                self.LOGGER.info('Stream {} ({}) with max_version {} targetting {}'.format(
                    stream_buffer.stream,
                    root_table_name,
                    stream_buffer.max_version,
                    target_table_version
                ))

                root_table_name = stream_buffer.stream
                if current_table_version is not None and \
                        stream_buffer.max_version is not None:
                    if stream_buffer.max_version < current_table_version:
                        self.LOGGER.warning('{} - Records from an earlier table version detected.'
                                            .format(stream_buffer.stream))
                        self.connection.rollback()
                        return None

                    elif stream_buffer.max_version > current_table_version:
                        root_table_name += SEPARATOR + str(stream_buffer.max_version)
                        target_table_version = stream_buffer.max_version

                self.LOGGER.info('Root table name {}'.format(root_table_name))

                written_batches_details = self.write_batch_helper(cur,
                                                                  root_table_name,
                                                                  stream_buffer.schema,
                                                                  stream_buffer.key_properties,
                                                                  stream_buffer.get_batch(),
                                                                  {'version': target_table_version})

                self.connection.commit()

                return written_batches_details
            except Exception as ex:
                self.connection.rollback()
                message = 'Exception writing records'
                self.LOGGER.exception(message)
                raise SnowflakeError(message, ex)

    def activate_version(self, stream_buffer, version):
        with self.conn.cursor() as cur:
            try:
                self.setup_table_mapping_cache(cur)
                root_table_name = self.add_table_mapping(cur, (stream_buffer.stream,), {})
                current_table_schema = self.get_table_schema(cur, root_table_name)

                if not current_table_schema:
                    self.LOGGER.error('{} - Table for stream does not exist'.format(
                        stream_buffer.stream))
                elif current_table_schema.get('version') is not None and current_table_schema.get('version') >= version:
                    self.LOGGER.warning('{} - Table version {} already active'.format(
                        stream_buffer.stream,
                        version))
                else:
                    versioned_root_table = root_table_name + SEPARATOR + str(version)

                    names_to_paths = dict([(v, k) for k, v in self.table_mapping_cache.items()])

                    cur.execute(
                        '''
                        SHOW TABLES LIKE '{}%' IN SCHEMA {}.{}
                        '''.format(
                            versioned_root_table,
                            sql.identifier(self.connection.database),
                            sql.identifier(self.connection.schema)))

                    for versioned_table_name in [x[1] for x in cur.fetchall()]:
                        table_name = root_table_name + versioned_table_name[len(versioned_root_table):]
                        table_path = names_to_paths[table_name]

                        args = {'db_schema': '{}.{}'.format(
                                    sql.identifier(self.connection.database),
                                    sql.identifier(self.connection.schema)),
                                'stream_table_old': sql.identifier(table_name +
                                                                SEPARATOR +
                                                                'old'),
                                'stream_table': sql.identifier(table_name),
                                'version_table': sql.identifier(versioned_table_name)}

                        cur.execute(
                            '''
                            ALTER TABLE {db_schema}.{stream_table} RENAME {stream_table_old}
                            '''.format(**args))

                        cur.execute(
                            '''
                            ALTER TABLE {db_schema}.{verions_table} RENAME {stream_table}
                            '''.format(**args))

                        cur.execute(
                            '''
                            DROP TABLE {table_schema}.{stream_table_old}
                            '''.format(**args))

                        self.connection.commit()

                        metadata = self._get_table_metadata(cur, table_name)

                        self.LOGGER.info('Activated {}, setting path to {}'.format(
                            metadata,
                            table_path
                        ))

                        metadata['path'] = table_path
                        self._set_table_metadata(cur, table_name, metadata)
            except Exception as ex:
                self.connection.rollack()
                message = '{} - Exception activating table version {}'.format(
                    stream_buffer.stream,
                    version)
                self.LOGGER.exception(message)
                raise SnowflakeError(message, ex)

    def canonicalize_identifier(self, identifier):
        if not identifier:
            identifier = '_'

        return re.sub(r'[^\w\d_]', '_', identifier.lower())

    def add_key_properties(self, cur, table_name, key_properties):
        if not key_properties:
            return None

        metadata = self._get_table_metadata(cur, table_name)

        if not 'key_properties' in metadata:
            metadata['key_properties'] = key_properties
            self._set_table_metadata(cur, table_name, metadata)

    def add_table(self, cur, path, name, metadata):
        sql.valid_identifier(name)

        cur.execute('''
            CREATE TABLE {}.{}.{} ({} {})
            '''.format(
                sql.identifier(self.connection.database),
                sql.identifier(self.connection.schema),
                sql.identifier(name),
                # Snowflake does not allow for creation of tables with no columns
                self.CREATE_TABLE_INITIAL_COLUMN,
                self.CREATE_TABLE_INITIAL_COLUMN_TYPE
            ))

        self._set_table_metadata(cur, name, {'path': path,
                                             'version': metadata.get('version', None),
                                             'schema_version': metadata['schema_version'],
                                             'mappings': {}})

        self.add_column_mapping(cur,
                                name,
                                (self.CREATE_TABLE_INITIAL_COLUMN,),
                                self.CREATE_TABLE_INITIAL_COLUMN,
                                json_schema.make_nullable({'type': json_schema.BOOLEAN}))

    def add_table_mapping(self, cur, from_path, metadata):
        mapping = self.add_table_mapping_helper(from_path, self.table_mapping_cache)

        if not mapping['exists']:
            self.table_mapping_cache[from_path] = mapping['to']

        return mapping['to']

    # def _get_update_sql(self, target_table_name, temp_table_name, key_properties, columns, subkeys):
    #     full_table_name = sql.SQL('{}.{}').format(
    #         sql.Identifier(self.postgres_schema),
    #         sql.Identifier(target_table_name))
    #     full_temp_table_name = sql.SQL('{}.{}').format(
    #         sql.Identifier(self.postgres_schema),
    #         sql.Identifier(temp_table_name))

    #     pk_temp_select_list = []
    #     pk_where_list = []
    #     pk_null_list = []
    #     cxt_where_list = []
    #     for pk in key_properties:
    #         pk_identifier = sql.Identifier(pk)
    #         pk_temp_select_list.append(sql.SQL('{}.{}').format(full_temp_table_name,
    #                                                            pk_identifier))

    #         pk_where_list.append(
    #             sql.SQL('{table}.{pk} = "dedupped".{pk}').format(
    #                 table=full_table_name,
    #                 temp_table=full_temp_table_name,
    #                 pk=pk_identifier))

    #         pk_null_list.append(
    #             sql.SQL('{table}.{pk} IS NULL').format(
    #                 table=full_table_name,
    #                 pk=pk_identifier))

    #         cxt_where_list.append(
    #             sql.SQL('{table}.{pk} = "pks".{pk}').format(
    #                 table=full_table_name,
    #                 pk=pk_identifier))
    #     pk_temp_select = sql.SQL(', ').join(pk_temp_select_list)
    #     pk_where = sql.SQL(' AND ').join(pk_where_list)
    #     pk_null = sql.SQL(' AND ').join(pk_null_list)
    #     cxt_where = sql.SQL(' AND ').join(cxt_where_list)

    #     sequence_join = sql.SQL(' AND "dedupped".{} >= {}.{}').format(
    #         sql.Identifier(SINGER_SEQUENCE),
    #         full_table_name,
    #         sql.Identifier(SINGER_SEQUENCE))

    #     distinct_order_by = sql.SQL(' ORDER BY {}, {}.{} DESC').format(
    #         pk_temp_select,
    #         full_temp_table_name,
    #         sql.Identifier(SINGER_SEQUENCE))

    #     if len(subkeys) > 0:
    #         pk_temp_subkey_select_list = []
    #         for pk in (key_properties + subkeys):
    #             pk_temp_subkey_select_list.append(sql.SQL('{}.{}').format(full_temp_table_name,
    #                                                                       sql.Identifier(pk)))
    #         insert_distinct_on = sql.SQL(', ').join(pk_temp_subkey_select_list)

    #         insert_distinct_order_by = sql.SQL(' ORDER BY {}, {}.{} DESC').format(
    #             insert_distinct_on,
    #             full_temp_table_name,
    #             sql.Identifier(SINGER_SEQUENCE))
    #     else:
    #         insert_distinct_on = pk_temp_select
    #         insert_distinct_order_by = distinct_order_by

    #     insert_columns_list = []
    #     dedupped_columns_list = []
    #     for column in columns:
    #         insert_columns_list.append(sql.SQL('{}').format(sql.Identifier(column)))
    #         dedupped_columns_list.append(sql.SQL('{}.{}').format(sql.Identifier('dedupped'),
    #                                                              sql.Identifier(column)))
    #     insert_columns = sql.SQL(', ').join(insert_columns_list)
    #     dedupped_columns = sql.SQL(', ').join(dedupped_columns_list)

    #     return sql.SQL('''
    #         DELETE FROM {table} USING (
    #                 SELECT "dedupped".*
    #                 FROM (
    #                     SELECT *,
    #                            ROW_NUMBER() OVER (PARTITION BY {pk_temp_select}
    #                                               {distinct_order_by}) AS "pk_ranked"
    #                     FROM {temp_table}
    #                     {distinct_order_by}) AS "dedupped"
    #                 JOIN {table} ON {pk_where}{sequence_join}
    #                 WHERE pk_ranked = 1
    #             ) AS "pks" WHERE {cxt_where};
    #         INSERT INTO {table}({insert_columns}) (
    #             SELECT {dedupped_columns}
    #             FROM (
    #                 SELECT *,
    #                        ROW_NUMBER() OVER (PARTITION BY {insert_distinct_on}
    #                                           {insert_distinct_order_by}) AS "pk_ranked"
    #                 FROM {temp_table}
    #                 {insert_distinct_order_by}) AS "dedupped"
    #             LEFT JOIN {table} ON {pk_where}
    #             WHERE pk_ranked = 1 AND {pk_null}
    #         );
    #         DROP TABLE {temp_table};
    #         ''').format(table=full_table_name,
    #                     temp_table=full_temp_table_name,
    #                     pk_temp_select=pk_temp_select,
    #                     pk_where=pk_where,
    #                     cxt_where=cxt_where,
    #                     sequence_join=sequence_join,
    #                     distinct_order_by=distinct_order_by,
    #                     pk_null=pk_null,
    #                     insert_distinct_on=insert_distinct_on,
    #                     insert_distinct_order_by=insert_distinct_order_by,
    #                     insert_columns=insert_columns,
    #                     dedupped_columns=dedupped_columns)

    def serialize_table_record_null_value(self, remote_schema, streamed_schema, field, value):
        return value

    def serialize_table_record_datetime_value(self, remote_schema, streamed_schema, field, value):
        return arrow.get(value).format('YYYY-MM-DD HH:mm:ss.SSSSZZ')

    # def persist_csv_rows(self,
    #                      cur,
    #                      remote_schema,
    #                      temp_table_name,
    #                      columns,
    #                      csv_rows):

    #     copy = sql.SQL('COPY {}.{} ({}) FROM STDIN WITH CSV NULL AS {}').format(
    #         sql.Identifier(self.postgres_schema),
    #         sql.Identifier(temp_table_name),
    #         sql.SQL(', ').join(map(sql.Identifier, columns)),
    #         sql.Literal(RESERVED_NULL_DEFAULT))
    #     cur.copy_expert(copy, csv_rows)

    #     pattern = re.compile(SINGER_LEVEL.format('[0-9]+'))
    #     subkeys = list(filter(lambda header: re.match(pattern, header) is not None, columns))

    #     canonicalized_key_properties = [self.fetch_column_from_path((key_property,), remote_schema)[0]
    #                                     for key_property in remote_schema['key_properties']]

    #     update_sql = self._get_update_sql(remote_schema['name'],
    #                                       temp_table_name,
    #                                       canonicalized_key_properties,
    #                                       columns,
    #                                       subkeys)
    #     cur.execute(update_sql)

    def write_table_batch(self, cur, table_batch, metadata):
        remote_schema = table_batch['remote_schema']

        target_table_name = 'tmp_' + str(uuid.uuid4()).replace('-', '_')

        ## Create temp table to upload new data to
        target_schema = deepcopy(remote_schema)
        target_schema['path'] = (target_table_name,)
        self.upsert_table_helper(cur,
                                 target_schema,
                                 {'version': remote_schema['version']},
                                 log_schema_changes=False)

        # ## Make streamable CSV records
        # csv_headers = list(remote_schema['schema']['properties'].keys())
        # rows_iter = iter(table_batch['records'])

        # def transform():
        #     try:
        #         row = next(rows_iter)

        #         with io.StringIO() as out:
        #             writer = csv.DictWriter(out, csv_headers)
        #             writer.writerow(row)
        #             return out.getvalue()
        #     except StopIteration:
        #         return ''

        # csv_rows = TransformStream(transform)

        # ## Persist csv rows
        # self.persist_csv_rows(cur,
        #                       remote_schema,
        #                       target_table_name,
        #                       csv_headers,
        #                       csv_rows)

        return len(table_batch['records'])

    def add_column(self, cur, table_name, column_name, column_schema):

        cur.execute('''
            ALTER TABLE {database}.{table_schema}.{table_name}
            ADD COLUMN {column_name} {data_type}
            '''.format(
                database=sql.identifier(self.connection.database),
                table_schema=sql.identifier(self.connection.schema),
                table_name=sql.identifier(table_name),
                column_name=sql.identifier(column_name),
                data_type=self.json_schema_to_sql_type(column_schema)))

    def migrate_column(self, cur, table_name, from_column, to_column):
        cur.execute('''
            UPDATE {database}.{table_schema}.{table_name}
            SET {to_column} = {from_column}
            '''.format(
                database=sql.identifier(self.connection.database),
                table_schema=sql.identifier(self.connection.schema),
                table_name=sql.identifier(table_name),
                to_column=sql.identifier(to_column),
                from_column=sql.identifier(from_column)))

    def drop_column(self, cur, table_name, column_name):
        cur.execute('''
            ALTER TABLE {database}.{table_schema}.{table_name}
            DROP COLUMN {column_name}
            '''.format(
                database=sql.identifier(self.connection.database),
                table_schema=sql.identifier(self.connection.schema),
                table_name=sql.identifier(table_name),
                column_name=sql.identifier(column_name)))

    def make_column_nullable(self, cur, table_name, column_name):
        cur.execute('''
            ALTER TABLE {database}.{table_schema}.{table_name}
            ALTER COLUMN {column_name} DROP NOT NULL
            '''.format(
            database=sql.identifier(self.connection.database),
            table_schema=sql.identifier(self.connection.schema),
            table_name=sql.Identifier(table_name),
            column_name=sql.Identifier(column_name)))

    def _set_table_metadata(self, cur, table_name, metadata):
        """
        Given a Metadata dict, set it as the comment on the given table.
        :param self: Snowflake
        :param cur: Cursor
        :param table_name: String
        :param metadata: Metadata Dict
        :return: None
        """
        cur.execute('''
            COMMENT ON TABLE {}.{}.{} IS '{}'
            '''.format(
            sql.identifier(self.connection.database),
            sql.identifier(self.connection.schema),
            sql.identifier(table_name),
            json.dumps(metadata)))

    def _get_table_metadata(self, cur, table_name):
        cur.execute(
            '''
            SHOW TABLES LIKE '{}' IN SCHEMA {}.{}
            '''.format(
                table_name,
                sql.identifier(self.connection.database),
                sql.identifier(self.connection.schema),))
        tables = cur.fetchall()

        if not tables:
            return None
        
        if len(tables) != 1:
            raise SnowflakeError(
                '{} tables returned while searching for: {}.{}.{}'.format(
                    len(tables),
                    self.connection.database,
                    self.connection.schema,
                    table_name
                ))

        comment = tables[0][5]

        if comment:
            try:
                comment_meta = json.loads(comment)
            except:
                self.LOGGER.exception('Could not load table comment metadata')
                raise
        else:
            comment_meta = None

        return comment_meta

    def add_column_mapping(self, cur, table_name, from_path, to_name, mapped_schema):
        metadata = self._get_table_metadata(cur, table_name)

        mapping = {'type': json_schema.get_type(mapped_schema),
                   'from': from_path}

        if 't' == json_schema.shorthand(mapped_schema):
            mapping['format'] = 'date-time'

        metadata['mappings'][to_name] = mapping

        self._set_table_metadata(cur, table_name, metadata)

    def drop_column_mapping(self, cur, table_name, mapped_name):
        metadata = self._get_table_metadata(cur, table_name)

        metadata['mappings'].pop(mapped_name, None)

        self._set_table_metadata(cur, table_name, metadata)

    def is_table_empty(self, cur, table_name):
        cur.execute('''
            SELECT COUNT(1) FROM {}.{}.{}
            '''.format(
                sql.identifier(self.connection.database),
                sql.identifier(self.connection.schema),
                sql.identifier(table_name)
            ))

        return cur.fetchone()[0] == 0

    def get_table_schema(self, cur, name):
        metadata = self._get_table_metadata(cur, name)

        if not metadata:
            return None

        cur.execute('''
            SELECT column_name, data_type, is_nullable
            FROM {}.information_schema.columns
            WHERE table_schema = '{}' AND table_name = '{}'
            '''.format(
                sql.identifier(self.connection.database),
                self.connection.schema,
                name
            ))

        properties = {}
        for column in cur.fetchall():
            properties[column[0]] = self.sql_type_to_json_schema(column[1], column[2] == 'YES')

        metadata['name'] = name
        metadata['type'] = 'TABLE_SCHEMA'
        metadata['schema'] = {'properties': properties}

        return metadata

    def sql_type_to_json_schema(self, sql_type, is_nullable):
        """
        Given a string representing a SnowflakeSQL column type, and a boolean indicating whether
        the associated column is nullable, return a compatible JSONSchema structure.
        :param sql_type: String
        :param is_nullable: boolean
        :return: JSONSchema
        """
        _format = None
        sql_type = sql_type.lower()
        if re.match(r'time.*', sql_type):
            json_type = 'string'
            _format = 'date-time'
        elif sql_type == 'number':
            json_type = 'integer'
        elif sql_type == 'real':
            json_type = 'number'
        elif sql_type == 'boolean':
            json_type = 'boolean'
        elif sql_type == 'text':
            json_type = 'string'
        else:
            raise SnowflakeError('Unsupported type `{}` in existing target table'.format(sql_type))

        json_type = [json_type]
        if is_nullable:
            json_type.append(json_schema.NULL)

        ret_json_schema = {'type': json_type}
        if _format:
            ret_json_schema['format'] = _format

        return ret_json_schema

    def json_schema_to_sql_type(self, schema):
        _type = json_schema.get_type(schema)
        not_null = True
        ln = len(_type)
        if ln == 1:
            _type = _type[0]
        if ln == 2 and json_schema.NULL in _type:
            not_null = False
            if _type.index(json_schema.NULL) == 0:
                _type = _type[1]
            else:
                _type = _type[0]
        elif ln > 2:
            raise SnowflakeError('Multiple types per column not supported')

        sql_type = 'text'

        if 'format' in schema and \
                schema['format'] == 'date-time' and \
                _type == 'string':
            sql_type = 'timestamp_tz'
        elif _type == 'boolean':
            sql_type = 'boolean'
        elif _type == 'integer':
            sql_type = 'number'
        elif _type == 'number':
            sql_type = 'real'

        if not_null:
            sql_type += ' NOT NULL'

        return sql_type
