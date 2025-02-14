from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Callable, Union

from hyset import RunSettings

from ..result import ComputeResult, Result, get_result


@asynccontextmanager
async def acompute(method: Callable, obj: Any, *args, **kwargs
                   ) -> AsyncGenerator[Union[dict, Result, str], None]:
    """Run the calculation in an asynchronous context-based manner.

    This method accepts a callable method, an object of type HylleraasObject,
    along with a variable number of positional and keyword arguments. It
    checks if the method is callable and has the required attributes. Then,
    it sets up the run settings, runs the calculation, parses the result,
    and tears down the run settings.

    Parameters
    ----------
    method : Callable
        The method to be used for the calculation. It must be callable and
        have 'setup', 'compute_settings', and 'parse' attributes.
    obj : HylleraasObject
        The object on which the calculation is to be run.
    *args : tuple
        A variable number of positional arguments to be passed to the
        'setup' method of the calculation method.
    **kwargs : dict
        A variable number of keyword arguments to be passed to the
        'setup' method of the calculation method.

    Yields
    ------
    Result or dict
        The result of the calculation, as parsed by the 'parse' method of
        the calculation method.

    Notes
    -----
    Currently the default yielding-type is dictionary. This can be controlled
    by the keywarod argument 'return_class'

    Raises
    ------
    AttributeError
        If the method does not have 'setup', 'compute_settings', or 'parse'
        attributes.

    """
    if any(not hasattr(method, attr) for attr in ('setup', 'compute_settings',
                                                  'parse')):
        raise AttributeError('method must have setup, compute_settings, and '
                             'parse attributes')

    result_kwargs = kwargs.pop('result_kwargs', {})

    run_settings: RunSettings = method.setup(obj,  # type: ignore
                                             *args, **kwargs)
    output_type = getattr(run_settings, 'output_type', None)
    result: ComputeResult
    result = await method.compute_settings.arun(run_settings)  # type: ignore
    try:
        yield get_result(method.parse(result),  # type: ignore
                         output_type=output_type,
                         input_object=obj,
                         logger=getattr(run_settings, 'logger', None),
                         **result_kwargs)
    finally:
        await method.compute_settings.ateardown(run_settings)  # type: ignore
