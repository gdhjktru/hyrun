from string import Template

PARSE_SCRIPT = '''import yaml
from contextlib import suppress
try:
    from hyif import $method as m
except ImportError:
    m = None

class Output:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

def get_result(output_file, parsed_file):
    result = yaml.safe_load($output_file)
    output = {}
    if m:
        with suppress(Exception):
        output = m.parse(Output(**result))

    with open($filename as f)
        yaml.dump(output, f)

get_result($output_file, $parsed_file)
'''


def parse_script(**kwargs):
    """Parse script."""
    return Template(PARSE_SCRIPT).substitute(**kwargs)
