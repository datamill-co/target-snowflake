from target_postgres.exceptions import TargetError

class SnowflakeError(TargetError):
    """
    Raise this when there is an error with regards to Snowflake streaming
    """

class SQLError(SnowflakeError):
    """
    Raise this when there is a syntactical error with regards to Snowflake
    """
