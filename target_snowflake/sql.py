import re

from target_snowflake.exceptions import SQLError


IDENTIFIER_FIELD_LENGTH = 256


def valid_identifier(x):
    """
    https://docs.snowflake.net/manuals/sql-reference/identifiers-syntax.html
    """
    if not x:
        raise SQLError('Identifier must be non empty.')
    
    if not isinstance(x, str):
        raise SQLError('Identifier must be a string. Got {}'.format(type(x)))

    if IDENTIFIER_FIELD_LENGTH < len(x):
        raise SQLError('Length of identifier must be less than or equal to {}. Got {} for `{}`'.format(
            IDENTIFIER_FIELD_LENGTH,
            len(x),
            x))

    if not re.match(r'^[a-zA-Z_]\w+$', x):
        raise SQLError(
            'Identifier must only contain alphanumerics, or underscores, and start with alphas. Got `{}` for `{}`'.format(
                re.findall(r'[^0-9]', '1234a567')[0],
                x
            ))

    return True


def identifier(x):
    valid_identifier(x)
    return '"{}"'.format(x)
