import json
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

import yaml
from hytools.logger import Logger, get_logger

from .result import Result

OUTPUT_MAPPING: Dict[str, Callable] = {'yaml': yaml.dump,
                                       'json': json.dumps}


# def add_input_object_to_dict(d: dict,
#                              input_object: Optional[Any] = None,
#                              logger: Optional[Logger] = None,
#                              **kwargs) -> dict:
#     """Add input object to dictionary."""
#     default_mapping_file = Path(__file__).parent /
#                           'input_obj_keys_to_add.yml'
#     input_mapping = (kwargs.get('input_mapping', None)
#                      or
#                      yaml.safe_load(default_mapping_file.read_text()))
#     mapping = input_mapping.get(str(input_object.__class__.__name__),
#                                 input_mapping)
#     if logger:
#         logger.debug(f'Adding input object {input_object} to result' +
#                      f' using mapping {mapping}')

#     for k, v in mapping.items():
#         if not isinstance(v, str):
#             continue
#         try:
#             val = getattr(input_object, k, None)
#         except TypeError:
#             continue
#         else:
#             d[v] = val
#     return d

def apply_to_list(func):
    """Decorate to execute a function on a list of arguments."""
    @wraps(func)
    def wrapper(result, *args, **kwargs):
        """Execute function on list."""
        if isinstance(result, list):
            return [func(r, *args, **kwargs) for r in result]
        return func(result, *args, **kwargs)
    return wrapper


@apply_to_list
def get_result(result: Any, **kwargs
               ) -> Union[list, dict, Result, str]:
    """Convert and return the result in the specified output format.

    This function takes a result object and returns it in the specified output
    format. The result can be a list, in which case the function is applied to
    each element. If units are provided, the result is converted from the
    source units to the target units.

    Parameters
    ----------
    result : Any
        The result to be converted. This can be any type, but if it's not a
        list or a dictionary, it must have a __dict__ attribute.
    output_type : Optional[str], default 'dict'
        The format in which to return the result. This can be 'dict', 'Result',
        'yaml', 'json', or a file path. If a file path is provided, the result
        is written to this file in YAML format.
    logger : Optional[Logger], default LoggerDummy()
        The logger to use for logging messages. If not provided, a dummy logger
        is used.
    units_to : Optional[Union[str, Units]]
        The target units for unit conversion. If not provided, no unit
        conversion is performed.
    units_from : Optional[Union[str, Units]]
        The source units for unit conversion. If not provided, no unit
        conversion is performed.
    **kwargs
        Additional keyword arguments.

    Returns
    -------
    Union[list, dict, Result, str]
        The result in the specified output format. If the output type is a
        file path, the absolute path
        to the file is returned.

    Raises
    ------
    TypeError
        If the result is not a list or a dictionary and does not have a
        __dict__ attribute.

    """
    rm = ResultManager(**kwargs)

    if rm.units_to and rm.units_from:
        result = rm.convert_units(result, **kwargs)

    return rm.dump(rm.convert_to_dict(result))


class ResultManager:
    """Manage the result of a calculation."""

    def __init__(self, logger: Optional[Logger] = None,
                 output_type: Optional[str] = None,
                 units_to: Optional[Any] = None,
                 units_from: Optional[Any] = None,
                 **kwargs):
        self.logger = logger or get_logger()
        self.output_type = output_type or 'dict'
        self.units_to = units_to
        self.units_from = units_from

    def convert_units(self, result: Any, **kwargs) -> Any:
        """Convert units of the result."""
        try:
            from hytools.units import convert_units
        except ImportError:
            self.logger.error('Failed to import Units and convert_units')
            return result
        else:
            return convert_units(result, **kwargs)

    def convert_to_dict(self, result: Any) -> dict:
        """Convert result to dictionary."""
        if isinstance(result, dict):
            return result
        if hasattr(result, '__dict__'):
            return vars(result)
        raise TypeError('Result must be a dictionary-ish object')

    def dump(self, result: dict) -> Union[dict, str, Result]:
        """Dump result to output type."""
        if self.output_type in OUTPUT_MAPPING:
            return OUTPUT_MAPPING[self.output_type](result)
        if self.output_type == 'Result':
            return Result(**result)
        return result
