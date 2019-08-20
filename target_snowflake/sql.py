import re

def valid_identifier(x):
    """
    https://docs.snowflake.net/manuals/sql-reference/identifiers-syntax.html
    """
    assert isinstance(x, str) and len(x) < 256 and re.match('^[a-zA-Z_]\w+$', x)

def identifier(x):
    valid_identifier(x)
    return '"{}"'.format(x)
