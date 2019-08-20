import singer
from singer import utils
from target_postgres import target_tools

from target_snowflake.connection import connect
from target_snowflake.snowflake import SnowflakeTarget

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'snowflake_account',
    'snowflake_warehouse',
    'snowflake_database',
    'snowflake_username',
    'snowflake_password'
]


def main(config, input_stream=None):
    with connect(
            user=config.get('snowflake_username'),
            password=config.get('snowflake_password'),
            account=config.get('snowflake_account'),
            warehouse=config.get('snowflake_warehouse'),
            database=config.get('snowflake_database')
    ) as connection:
        target = SnowflakeTarget(
            connection,
            schema=config.get('snowflake_schema', 'public'),
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables')
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, target, config=config)
        else:
            target_tools.main(target)


def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
