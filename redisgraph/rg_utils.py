import logging
logger = logging.getLogger(__name__)

import random
import re
import string
import tqdm

from typing import Any, Sequence

""" Identify most valid cypher identifiers. See the Neo4j docs for more
details: https://neo4j.com/docs/cypher-manual/current/syntax/naming/#_naming_rules

In particular, this regex does not check the length of the string, and it does
not pass for strings with spaces surrounded by backticks.
"""
VALID_CYPHER_IDENIFIER = re.compile(r'^[a-zA-Z_][0-9a-zA-Z_]*$')

def random_string(length:int=10) -> str:
    """ Return a random N chracter long string
    """
    return ''.join(random.choice(string.ascii_lowercase) for x in range(length))

def quote_string(v:Any) -> str:
    """ Wrap `v` in quotes, if it is a string
    """

    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v
    if len(v) == 0:
        return '""'

    if v[0] != '"':
        v = '"' + v

    if v[-1] != '"':
        v = v + '"'

    return v

def replace_symbols(s:str, replacement:str='_') -> str:
    s = re.sub('[^0-9a-zA-Z]', replacement, s)
    return s

def replace_all_symbols(
    strings:Sequence[str],
    replacement:str='_') -> Sequence[str]:
    rs = [
        replace_symbols(s) for s in strings
    ]

    return rs

def validate_cypher_identifier(
        identifier:str,
        none_is_okay:bool=True,
        raise_on_invalid:bool=True) -> bool:
    """ Validate `identifier`, and either raise an error or print a warning
    and return that the identifier was invalid
    """
    is_valid = True
    msg = None

    if identifier is None:
        if not none_is_okay:
            is_valid = False
            msg = "Invalid Cypher identifier. Found 'None'"

    # only perform the reg ex check if a cheaper check failed
    if is_valid and (identifier is not None):
        if VALID_CYPHER_IDENIFIER.match(identifier) is None:
            is_valid = False
            msg = "Invalid Cypher identifier: '{}'".format(identifier)

    if not is_valid:
        if raise_on_invalid:
            raise ValueError(msg)
        else:
            logger.warning(msg)

    return is_valid