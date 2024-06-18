
from dataclasses import dataclass
from typing import Any, List, Optional

from fabric.connection import Connection

from .job_info import JobInfo
from .output import Output

# from tqdm import tqdm


@dataclass
class Job(JobInfo):
    """HSP job."""

    # progress_bar: Optional[tqdm] = None
    tasks: Optional[List[Any]] = None
    outputs: Optional[List[Output]] = None
    job_script: Optional[str] = None
