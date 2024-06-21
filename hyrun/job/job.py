
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Union

from hydb import Database

from hyrun.scheduler.abc import Scheduler

from .job_info import JobInfo
from .output import Output

# from tqdm import tqdm


@dataclass
class Job(JobInfo):
    """HSP job."""

    tasks: Optional[List[Any]] = None
    outputs: Optional[List[Output]] = None
    job_script: Optional[str] = None
    database: Optional[Union[str, Path, Database]] = None
    scheduler: Optional[Union[str, Scheduler]] = None
    metadata: Optional[dict] = field(default_factory=dict)
