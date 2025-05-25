from .job import JobGraph, job, get_workflow
from hytools.logger import get_logger


def run(*args, **kwargs):
    """Run."""
    logger = kwargs.pop('logger') or get_logger(print_level='DEBUG')
    workflow = get_workflow(*args, **kwargs)
    print(workflow)