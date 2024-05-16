
from dataclasses import dataclass
from typing import Optional


@dataclass
class JobInfo:
    """Dataclass containing information about a job."""

    job_id: int = -1
    job_name: Optional[str] = None
    scheduler: Optional[str] = None
    job_finished: bool = False
    job_status: Optional[str] = None
    job_hash: Optional[str] = None
