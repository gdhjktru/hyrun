
from typing import Mapping, Optional, Type

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


def get_scheduler(scheduler_type: Optional[str] = None,
                  **kwargs) -> Scheduler:
    """Get scheduler."""
    if scheduler_type not in SCHEDULER_MAPPING:
        raise ValueError(f'Invalid scheduler: {scheduler_type}. '
                         'Available schedulers: ',
                         f'{list(SCHEDULER_MAPPING.keys())}')
    return SCHEDULER_MAPPING[scheduler_type](**kwargs)
