import time
from contextlib import contextmanager
from functools import singledispatch, wraps
from typing import Optional, Union

from fabric import Connection
from fabric.connection import Connection as Connection2

# from paramiko.ssh_exception import SSHException

connection_type = Optional[Union[dict, Connection]]


@contextmanager
def connect_to_remote(connection: Optional[connection_type] = None
                      ) -> Connection:
    """Connect to remote server.

    Parameters
    ----------
    connection : fabric.Connection or dict, optional
        Connection object or options, by default None

    Returns
    -------
    fabric.Connection
        Connection object.

    """
    c = connect(connection)
    c.open()
    try:
        yield c
    finally:
        c.close()


@singledispatch
def connect(connection: connection_type, **kwargs) -> Connection:
    """Connect to remote server.

    Parameters
    ----------
    connection : dict or fabric.Connection, optional
        Connection (options).

    Returns
    -------
    fabric.Connection
        Connection object.

    """
    if connection is None:
        raise ValueError('connection must be a dict or fabric.Connection '+
                         f'not {type(connection)}')
    try:
        c_dict = connection.__dict__
    except AttributeError:
        raise NotImplementedError(
            'connect is not implemented for ' + f'{type(connection)}'
        )
    else:
        return connect(dict(c_dict), **kwargs)


@connect.register(Connection)
@connect.register(Connection2)
def _(connection: Connection, **kwargs) -> Connection:
    """Connect to remote server.

    Parameters
    ----------
    connection : fabric.Connection
        Connection object.

    Returns
    -------
    fabric.Connection
        Connection object.

    """
    if kwargs.get('transfer', None) is not None:
        for k, v in kwargs['transfer'].items():
            setattr(connection, k, v)
    return connection


@connect.register(dict)
def _(connection: dict, **kwargs) -> Connection:
    """Connect to remote server.

    Parameters
    ----------
    connection : mapping
        Connection options.

    Returns
    -------
    fabric.Connection
        Connection object.

    Raises
    ------
    ValueError
        If host is not specified in options.

    """
    copt = connection.copy()
    connection_keys = [
        'host',
        'user',
        'port',
        'config',
        'gateway',
        'forward_agent',
        'connect_timeout',
        'connect_kwargs',
        'inline_ssh_env',
    ]
    transfer_keys = ['auto_connect', 'max_connect_attempts']
    transfer_values = [copt.pop(k, None) for k in transfer_keys]
    transfer = dict(zip(transfer_keys, transfer_values))
    copt = {k: copt.pop(k) for k in connection_keys if k in copt}
    if any(copt[k] == '' for k in ['host', 'user']):
        raise ValueError(
            'host and user must be specified in ' + 'connection_options'
        )
    host = copt.pop('host', None)
    if host is None:
        raise ValueError('host must be specified in ' + 'connection_options')
    return connect(Connection(host, **copt), transfer=transfer, **kwargs)
