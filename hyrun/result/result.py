import json
import pprint
from functools import singledispatchmethod
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

import yaml

try:
    from hytools.units import Units, convert_units
except ImportError:
    try:
        from hyobj.units import Units, convert_units
    except ImportError:
        Units = None
        convert_units = None
from hytools.logger import Logger, LoggerDummy


class Result:
    """General Result class mimicking dict and DataClass."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        d = args[0] if args else kwargs
        if not isinstance(d, dict):
            try:
                d = d.__dict__
            except AttributeError as e:
                raise TypeError('Only dict or dataclass is allowed') from e
        self.construct(d)

    def items(self):
        """Items."""
        return self.__dict__.items()

    def keys(self):
        """Keys."""
        return self.__dict__.keys()

    def values(self):
        """Values."""
        return self.__dict__.values()

    def __iter__(self):
        """Iterate."""
        return iter(self.__dict__.items())

    def __getitem__(self, key):
        """Get item."""
        return getattr(self, key)

    def __repr__(self):
        """Repr."""
        return pprint.pformat(self.__dict__)

    def __str__(self):
        """Str."""
        return pprint.pformat(self.__dict__)

    def __eq__(self, other):
        """Equality."""
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Inequality."""
        return not self.__eq__(other)

    def __len__(self):
        """Length."""
        return len(self.__dict__)

    def __contains__(self, item):
        """Contains."""
        return item in self.__dict__

    def __setitem__(self, key, value):
        """Set item."""
        setattr(self, key, value)

    def __delitem__(self, key):
        """Delete item."""
        delattr(self, key)

    def get(self, key, default=None):
        """Get."""
        return getattr(self, key, default)

    @singledispatchmethod
    def construct(self, d: Any) -> None:
        """Construct Result from input."""
        raise NotImplementedError(  # pragma: no cover
            f'Cannot construct Result from {type(d)}')

    @construct.register(dict)
    def _(self, d: dict) -> None:
        """Construct Result from dict."""
        for k, w in d.items():
            setattr(self, k, w)

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


def get_result(result: Any,
               output_type: Optional[str] = None,
               logger: Optional[Logger] = None,
               units_to: Optional[Union[str, Units]] = None,
               units_from: Optional[Union[str, Units]] = None,
               **kwargs
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
    output_mapping: Dict[str, Callable] = {'dict': dict,
                                           'Result': Result,
                                           'yaml': yaml.dump,
                                           'json': json.dumps}
    logger = logger or LoggerDummy()
    output_type = output_type or 'dict'
    # logger.disabled = False
    logger.debug(f'Constructing result type {output_type} from {type(result)}')

    if not any([units_to is None, units_from is None]):
        try:
            converted = convert_units(result,
                                      units_to=units_to,
                                      units_from=units_from,
                                      record_success=True,)
        except Exception as e:
            logger.warning(f'Failed to convert units: {e}')
        else:
            logger.info(f'Units of {converted.get("converted")} ' +
                        'successfully converted')
            result = converted
            if isinstance(result, dict):
                result['units'] = units_to
                result['units_default'] = units_from

    if isinstance(result, list):
        return [get_result(r, output_type) for r in result]

    if not isinstance(result, dict):
        if hasattr(result, '__dict__'):
            result = vars(result)
        else:
            raise TypeError('Result must be a dictionary')

    if output_type in output_mapping:
        logger.info(f'Returning result as {output_type}')
        return output_mapping[output_type](result)

    logger.info(f'Writing result to file {output_type}')
    p = Path(output_type)
    p.write_text(yaml.dump(result))
    return str(p.resolve())
