
from dataclasses import dataclass
from typing import Any, List, Optional, Union

from fabric.connection import Connection

from hyrun.scheduler.abc import Scheduler

from .job_info import JobInfo
from .output import Output

# from tqdm import tqdm


@dataclass
class Job(JobInfo, Output):
    """HSP job."""

    db_id: Optional[int] = None
    # progress_bar: Optional[tqdm] = None
    run_settings: Any = None
    tasks: Optional[List[Any]] = None
    connection: Optional[Connection] = None
    job_script: Optional[str] = None
    local_files: Optional[List[str]] = None
    remote_files: Optional[List[str]] = None
    database: Optional[Any] = None
    scheduler: Optional[Union[str, Scheduler]] = None
    metadata: Optional[dict] = None
