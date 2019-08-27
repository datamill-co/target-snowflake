from copy import deepcopy
from datetime import datetime

from psycopg2 import sql
import pytest

from fixtures import CatStream, CONFIG, db_prep, MultiTypeStream, NestedStream, TEST_DB
from target_postgres import singer_stream
from target_postgres.target_tools import TargetError

from target_snowflake import main, sql
from target_snowflake.connection import connect


def assert_columns_equal(cursor, table_name, expected_column_tuples):
    cursor.execute('''
        SELECT column_name, data_type, is_nullable
        FROM {}.information_schema.columns
        WHERE table_schema = '{}' AND table_name = '{}'
    '''.format(
        sql.identifier(cursor.connection.database),
        cursor.connection.schema,
        table_name))

    columns = []
    for column in cursor.fetchall():
        columns.append((column[0], column[1], column[2]))

    assert set(columns) == expected_column_tuples


def get_count_sql(cursor, table_name):
    return '''
        SELECT COUNT(*) FROM {}.{}.{}
    '''.format(
        sql.identifier(cursor.connection.database),
        sql.identifier(cursor.connection.schema),
        sql.identifier(table_name))

def assert_count_equal(cursor, table_name, expected_count):
    cursor.execute(get_count_sql(cursor, table_name))

    assert cursor.fetchone()[0] == expected_count

def get_pk_key(pks, obj, subrecord=False):
    pk_parts = []
    for pk in pks:
        pk_parts.append(str(obj[pk]))
    if subrecord:
        for key, value in obj.items():
            if key[:11] == '_sdc_level_':
                pk_parts.append(str(value))
    return ':'.join(pk_parts)


def flatten_record(old_obj, subtables, subpks, new_obj=None, current_path=None, level=0):
    if not new_obj:
        new_obj = {}

    for prop, value in old_obj.items():
        if current_path:
            next_path = current_path + '__' + prop
        else:
            next_path = prop

        if isinstance(value, dict):
            flatten_record(value, subtables, subpks, new_obj=new_obj, current_path=next_path, level=level)
        elif isinstance(value, list):
            if next_path not in subtables:
                subtables[next_path] = []
            row_index = 0
            for item in value:
                new_subobj = {}
                for key, value in subpks.items():
                    new_subobj[key] = value
                new_subpks = subpks.copy()
                new_subobj[singer_stream.SINGER_LEVEL.format(level)] = row_index
                new_subpks[singer_stream.SINGER_LEVEL.format(level)] = row_index
                subtables[next_path].append(flatten_record(item,
                                                           subtables,
                                                           new_subpks,
                                                           new_obj=new_subobj,
                                                           level=level + 1))
                row_index += 1
        else:
            new_obj[next_path] = value
    return new_obj


def assert_record(a, b, subtables, subpks):
    a_flat = flatten_record(a, subtables, subpks)
    for prop, value in a_flat.items():
        if value is None:
            if prop in b:
                assert b[prop] == None
        elif isinstance(b[prop], datetime):
            assert value == b[prop].isoformat()[:19]
        else:
            assert value == b[prop]


def assert_records(conn, records, table_name, pks, match_pks=False):
    if not isinstance(pks, list):
        pks = [pks]

    with conn.cursor(True) as cur:
        cur.execute("set timezone='UTC';")

        cur.execute('''
            SELECT * FROM {}.{}.{}
        '''.format(
            sql.identifier(conn.database),
            sql.identifier(conn.schema),
            sql.identifier(table_name)))
        persisted_records_raw = cur.fetchall()

        persisted_records = {}
        for persisted_record in persisted_records_raw:
            pk = get_pk_key(pks, persisted_record)
            persisted_records[pk] = persisted_record

        subtables = {}
        records_pks = []
        for record in records:
            pk = get_pk_key(pks, record)
            records_pks.append(pk)
            persisted_record = persisted_records[pk]
            subpks = {}
            for pk in pks:
                subpks[singer_stream.SINGER_SOURCE_PK_PREFIX + pk] = persisted_record[pk]
            assert_record(record, persisted_record, subtables, subpks)

        if match_pks:
            assert sorted(list(persisted_records.keys())) == sorted(records_pks)

        sub_pks = list(map(lambda pk: singer_stream.SINGER_SOURCE_PK_PREFIX + pk, pks))
        for subtable_name, items in subtables.items():
            cur.execute('''
                SELECT * FROM {}.{}.{}
            '''.format(
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier(table_name + '__' + subtable_name)))
            persisted_records_raw = cur.fetchall()

            persisted_records = {}
            for persisted_record in persisted_records_raw:
                pk = get_pk_key(sub_pks, persisted_record, subrecord=True)
                persisted_records[pk] = persisted_record

            subtables = {}
            records_pks = []
            for record in items:
                pk = get_pk_key(sub_pks, record, subrecord=True)
                records_pks.append(pk)
                persisted_record = persisted_records[pk]
                assert_record(record, persisted_record, subtables, subpks)
            assert len(subtables.values()) == 0

            if match_pks:
                assert sorted(list(persisted_records.keys())) == sorted(records_pks)


def test_connect(db_prep):
    with connect(**TEST_DB) as connection:
        with connection.cursor() as cur:
            assert cur.execute('select 1').fetchall()


def test_loading__empty(db_prep):
    stream = CatStream(0)


def test_loading__empty__enabled_config(db_prep):
    config = CONFIG.copy()
    config['persist_empty_tables'] = True

    stream = CatStream(0)
    main(config, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('adoption__adopted_on', 'TIMESTAMP_TZ', 'YES'),
                                     ('adoption__was_foster', 'BOOLEAN', 'YES'),
                                     ('age', 'NUMBER', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('name', 'TEXT', 'NO'),
                                     ('paw_size', 'NUMBER', 'NO'),
                                     ('paw_colour', 'TEXT', 'NO'),
                                     ('flea_check_complete', 'BOOLEAN', 'NO'),
                                     ('pattern', 'TEXT', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_source_key_id', 'NUMBER', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('date_administered', 'TIMESTAMP_TZ', 'YES'),
                                     ('type', 'TEXT', 'YES')
                                 })

            assert_count_equal(cur, 'cats', 0)

def test_loading__empty__enabled_config__repeatability(db_prep):
    config = CONFIG.copy()
    config['persist_empty_tables'] = True

    main(config, input_stream=CatStream(0))

    main(config, input_stream=CatStream(0))

    main(config, input_stream=CatStream(0))


def test_loading__simple(db_prep):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('adoption__adopted_on', 'TIMESTAMP_TZ', 'YES'),
                                     ('adoption__was_foster', 'BOOLEAN', 'YES'),
                                     ('age', 'NUMBER', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('name', 'TEXT', 'NO'),
                                     ('paw_size', 'NUMBER', 'NO'),
                                     ('paw_colour', 'TEXT', 'NO'),
                                     ('flea_check_complete', 'BOOLEAN', 'NO'),
                                     ('pattern', 'TEXT', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_source_key_id', 'NUMBER', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('date_administered', 'TIMESTAMP_TZ', 'YES'),
                                     ('type', 'TEXT', 'YES')
                                 })

            assert_count_equal(cur, 'cats', 100)

        for record in stream.records:
            record['paw_size'] = 314159
            record['paw_colour'] = ''
            record['flea_check_complete'] = False

        assert_records(conn, stream.records, 'cats', 'id')


def test_loading__nested_tables(db_prep):
    main(CONFIG, input_stream=NestedStream(10))

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_count_equal(cur, 'root', 10)

            assert_count_equal(cur, 'root__array_scalar', 50)

            assert_count_equal(
                cur,
                'root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar',
                50)

            assert_count_equal(cur, 'root__array_of_array', 20)

            assert_count_equal(cur, 'root__array_of_array___sdc_value', 80)

            assert_count_equal(cur, 'root__array_of_array___sdc_value___sdc_value', 200)

            assert_columns_equal(cur,
                                 'root',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('null', 'NUMBER', 'YES'),
                                     ('nested_null__null', 'NUMBER', 'YES'),
                                     ('object_of_object_0__object_of_object_1__object_of_object_2__a', 'NUMBER', 'NO'),
                                     ('object_of_object_0__object_of_object_1__object_of_object_2__b', 'NUMBER', 'NO'),
                                     ('object_of_object_0__object_of_object_1__object_of_object_2__c', 'NUMBER', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar',
                                 {
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_source_key_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_value', 'BOOLEAN', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                 })

            assert_columns_equal(cur,
                                 'root__array_of_array',
                                 {
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_source_key_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                 })

            assert_columns_equal(cur,
                                 'root__array_of_array___sdc_value',
                                 {
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_source_key_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_1_id', 'NUMBER', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                 })

            assert_columns_equal(cur,
                                 'root__array_of_array___sdc_value___sdc_value',
                                 {
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_source_key_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_1_id', 'NUMBER', 'NO'),
                                     ('_sdc_level_2_id', 'NUMBER', 'NO'),
                                     ('_sdc_value', 'NUMBER', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                 })


def test_loading__new_non_null_column(db_prep):
    cat_count = 50
    main(CONFIG, input_stream=CatStream(cat_count))

    class NonNullStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            return record

    non_null_stream = NonNullStream(cat_count)
    non_null_stream.schema = deepcopy(non_null_stream.schema)
    non_null_stream.schema['schema']['properties']['paw_toe_count'] = {'type': 'integer',
                                                                       'default': 5}

    main(CONFIG, input_stream=non_null_stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('adoption__adopted_on', 'TIMESTAMP_TZ', 'YES'),
                                     ('adoption__was_foster', 'BOOLEAN', 'YES'),
                                     ('age', 'NUMBER', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('name', 'TEXT', 'NO'),
                                     ('paw_size', 'NUMBER', 'NO'),
                                     ('paw_colour', 'TEXT', 'NO'),
                                     ('paw_toe_count', 'NUMBER', 'YES'),
                                     ('flea_check_complete', 'BOOLEAN', 'NO'),
                                     ('pattern', 'TEXT', 'YES')
                                 })

            cur.execute('''
                SELECT {}, {} FROM {}.{}.{}
            '''.format(
                sql.identifier('id'),
                sql.identifier('paw_toe_count'),
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats')
            ))

            persisted_records = cur.fetchall()

            ## Assert that the split columns before/after new non-null data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[1] is None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])


def test_loading__column_type_change(db_prep):
    cat_count = 20
    main(CONFIG, input_stream=CatStream(cat_count))

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('adoption__adopted_on', 'TIMESTAMP_TZ', 'YES'),
                                     ('adoption__was_foster', 'BOOLEAN', 'YES'),
                                     ('age', 'NUMBER', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('name', 'TEXT', 'NO'),
                                     ('paw_size', 'NUMBER', 'NO'),
                                     ('paw_colour', 'TEXT', 'NO'),
                                     ('flea_check_complete', 'BOOLEAN', 'NO'),
                                     ('pattern', 'TEXT', 'YES')
                                 })

            cur.execute('''
                SELECT {} FROM {}.{}.{}
            '''.format(
                sql.identifier('name'),
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the original data is present
            assert cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])

    class NameBooleanCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record['name'] = False
            return record

    stream = NameBooleanCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'boolean'}

    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('adoption__adopted_on', 'TIMESTAMP_TZ', 'YES'),
                                     ('adoption__was_foster', 'BOOLEAN', 'YES'),
                                     ('age', 'NUMBER', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('name__s', 'TEXT', 'YES'),
                                     ('name__b', 'BOOLEAN', 'YES'),
                                     ('paw_size', 'NUMBER', 'NO'),
                                     ('paw_colour', 'TEXT', 'NO'),
                                     ('flea_check_complete', 'BOOLEAN', 'NO'),
                                     ('pattern', 'TEXT', 'YES')
                                 })

            cur.execute('''
                SELECT {}, {} FROM {}.{}.{}
            '''.format(
                sql.identifier('name__s'),
                sql.identifier('name__b'),
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is not None and x[1] is not None])

    class NameIntegerCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (2 * cat_count)
            record['name'] = 314
            return record

    stream = NameIntegerCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'integer'}

    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('adoption__adopted_on', 'TIMESTAMP_TZ', 'YES'),
                                     ('adoption__was_foster', 'BOOLEAN', 'YES'),
                                     ('age', 'NUMBER', 'YES'),
                                     ('id', 'NUMBER', 'NO'),
                                     ('name__s', 'TEXT', 'YES'),
                                     ('name__b', 'BOOLEAN', 'YES'),
                                     ('name__i', 'NUMBER', 'YES'),
                                     ('paw_size', 'NUMBER', 'NO'),
                                     ('paw_colour', 'TEXT', 'NO'),
                                     ('flea_check_complete', 'BOOLEAN', 'NO'),
                                     ('pattern', 'TEXT', 'YES')
                                 })

            cur.execute('''
                SELECT {}, {}, {} FROM {}.{}.{}
            '''.format(
                sql.identifier('name__s'),
                sql.identifier('name__b'),
                sql.identifier('name__i'),
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 3 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert cat_count == len([x for x in persisted_records if x[2] is not None])
            assert 0 == len(
                [x for x in persisted_records if x[0] is not None and x[1] is not None and x[2] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is None and x[1] is None and x[2] is None])


def test_loading__multi_types_columns(db_prep):
    stream_count = 50
    main(CONFIG, input_stream=MultiTypeStream(stream_count))

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'root',
                                 {
                                     ('_sdc_primary_key', 'TEXT', 'NO'),
                                     ('_sdc_batched_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_received_at', 'TIMESTAMP_TZ', 'YES'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_table_version', 'NUMBER', 'YES'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                     ('every_type__i', 'NUMBER', 'YES'),
                                     ('every_type__f', 'FLOAT', 'YES'),
                                     ('every_type__b', 'BOOLEAN', 'YES'),
                                     ('every_type__t', 'TIMESTAMP_TZ', 'YES'),
                                     ('every_type__i__1', 'NUMBER', 'YES'),
                                     ('every_type__f__1', 'FLOAT', 'YES'),
                                     ('every_type__b__1', 'BOOLEAN', 'YES'),
                                     ('number_which_only_comes_as_integer', 'FLOAT', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__every_type',
                                 {
                                     ('_sdc_source_key__sdc_primary_key', 'TEXT', 'NO'),
                                     ('_sdc_level_0_id', 'NUMBER', 'NO'),
                                     ('_sdc_sequence', 'NUMBER', 'YES'),
                                     ('_sdc_value', 'NUMBER', 'NO'),
                                     ('_sdc_target_snowflake_create_table_placeholder', 'BOOLEAN', 'YES'),
                                 })

            cur.execute('''
                SELECT {} FROM {}.{}.{}
            '''.format(
                sql.identifier('number_which_only_comes_as_integer'),
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('root')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the column is has migrated data
            assert stream_count == len(persisted_records)
            assert stream_count == len([x for x in persisted_records if isinstance(x[0], float)])


def test_upsert(db_prep):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_count_equal(cur, 'cats', 100)
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_count_equal(cur, 'cats', 100)
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(200)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_count_equal(cur, 'cats', 200)
        assert_records(conn, stream.records, 'cats', 'id')


def test_nested_delete_on_parent(db_prep):
    stream = CatStream(100, nested_count=3)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            high_nested = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100, nested_count=2)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            low_nested = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id')

    assert low_nested < high_nested


def test_full_table_replication(db_prep):
    stream = CatStream(110, version=0, nested_count=3)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            version_0_count = cur.fetchone()[0]
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            version_0_sub_count = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id', match_pks=True)

    assert version_0_count == 110
    assert version_0_sub_count == 330

    stream = CatStream(100, version=1, nested_count=3)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            version_1_count = cur.fetchone()[0]
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            version_1_sub_count = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id', match_pks=True)

    assert version_1_count == 100
    assert version_1_sub_count == 300

    stream = CatStream(120, version=2, nested_count=2)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            version_2_count = cur.fetchone()[0]
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            version_2_sub_count = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id', match_pks=True)

    assert version_2_count == 120
    assert version_2_sub_count == 240

    ## Test that an outdated version cannot overwrite
    stream = CatStream(314, version=1, nested_count=2)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            older_version_count = cur.fetchone()[0]

    assert older_version_count == version_2_count

@pytest.mark.xfail
def test_deduplication_newer_rows(db_prep):
    stream = CatStream(100, nested_count=3, duplicates=2)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            table_count = cur.fetchone()[0]
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            nested_table_count = cur.fetchone()[0]

            cur.execute('''
                SELECT "_sdc_sequence"
                FROM {}.{}.{}
                WHERE "id" in ({})
            '''.format(
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats'),
                ','.join(["'{}'".format(x) for x in stream.duplicate_pks_used])
            ))
            dup_cat_records = cur.fetchall()

    assert stream.record_message_count == 102
    assert table_count == 100
    assert nested_table_count == 300

    for record in dup_cat_records:
        assert record[0] == stream.sequence + 200


def test_deduplication_older_rows(db_prep):
    stream = CatStream(100, nested_count=2, duplicates=2, duplicate_sequence_delta=-100)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            table_count = cur.fetchone()[0]
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            nested_table_count = cur.fetchone()[0]
            
            cur.execute('''
                SELECT "_sdc_sequence"
                FROM {}.{}.{}
                WHERE "id" in ({})
            '''.format(
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats'),
                ','.join(["'{}'".format(x) for x in stream.duplicate_pks_used])
            ))
            dup_cat_records = cur.fetchall()

    assert stream.record_message_count == 102
    assert table_count == 100
    assert nested_table_count == 200

    for record in dup_cat_records:
        assert record[0] == stream.sequence


def test_deduplication_existing_new_rows(db_prep):
    stream = CatStream(100, nested_count=2)
    main(CONFIG, input_stream=stream)

    original_sequence = stream.sequence

    stream = CatStream(100,
                       nested_count=2,
                       sequence=original_sequence - 20)
    main(CONFIG, input_stream=stream)

    with connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql(cur, 'cats'))
            table_count = cur.fetchone()[0]
            cur.execute(get_count_sql(cur, 'cats__adoption__immunizations'))
            nested_table_count = cur.fetchone()[0]

            cur.execute('''
                SELECT DISTINCT "_sdc_sequence"
                FROM {}.{}.{}
            '''.format(
                sql.identifier(conn.database),
                sql.identifier(conn.schema),
                sql.identifier('cats')
            ))
            sequences = cur.fetchall()

    assert table_count == 100
    assert nested_table_count == 200

    assert len(sequences) == 1
    assert sequences[0][0] == original_sequence
