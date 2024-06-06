
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from hydb import Database

from hyrun.scheduler.abc import Scheduler


@dataclass
class JobInfo:
    """Dataclass containing information about a job."""

    job_id: int = -1
    job_name: Optional[str] = None
    job_finished: bool = False
    job_status: Optional[str] = None
    job_hash: Optional[str] = None
    database: Optional[Union[str, Path, Database]] = None
    scheduler: Optional[Union[str, Scheduler]] = None
    metadata: Optional[dict] = None
