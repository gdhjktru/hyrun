
from typing import Mapping, Optional, Type

from .abc import Scheduler
from .local import LocalScheduler
from .pbs import PbsScheduler
from .slurm import SlurmScheduler

SCHEDULER_MAPPING: Mapping[str,
                           Type[Scheduler]] = {'pbs': PbsScheduler,
                                               'torque': PbsScheduler,
                                               'slurm': SlurmScheduler,
                                               'local': LocalScheduler,
                                               'conda': LocalScheduler,
                                               'docker': LocalScheduler,
                                               'singularity': LocalScheduler,}


def get_scheduler(scheduler_name: Optional[str] = None,
                  **kwargs) -> Scheduler:
    """Get scheduler."""
    if scheduler_name not in SCHEDULER_MAPPING:
        raise ValueError(f'Invalid scheduler: {scheduler_name}.'
                         'Available schedulers: ',
                         f'{list(SCHEDULER_MAPPING.keys())}')
    return SCHEDULER_MAPPING[scheduler_name](**kwargs)
