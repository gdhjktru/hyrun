
from typing import Mapping, Type

from .abc import Scheduler
from .local import LocalScheduler
from .pbs import PbsScheduler
from .slurm import SlurmScheduler

SCHEDULER_MAPPING: Mapping[str, Type[Scheduler]]
SCHEDULER_MAPPING = {'pbs': PbsScheduler,  # type: ignore
                     'torque': PbsScheduler,  # type: ignore
                     'slurm': SlurmScheduler,  # type: ignore
                     'local': LocalScheduler,  # type: ignore
                     'conda': LocalScheduler,  # type: ignore
                     'docker': LocalScheduler,  # type: ignore
                     'singularity': LocalScheduler}  # type: ignore


def get_scheduler(*args, **kwargs) -> Scheduler:
    """Get scheduler."""
    if isinstance(args[0], Scheduler):
        return args[0]
    elif isinstance(args[0], str):
        scheduler_name = args[0]
    elif isinstance(args[0], dict):
        scheduler_name = args[0].get('scheduler_type', '')
    else:
        scheduler_name = kwargs.get('scheduler_type', '')

    if scheduler_name not in SCHEDULER_MAPPING:
        raise ValueError(f'Invalid scheduler: {scheduler_name}. '
                         'Available schedulers: ',
                         f'{list(SCHEDULER_MAPPING.keys())}')
    return SCHEDULER_MAPPING[scheduler_name](**kwargs)
