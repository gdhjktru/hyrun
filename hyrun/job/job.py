
from dataclasses import dataclass
from .output import Output
from .job_info import JobInfo
from tqdm import tqdm
from typing import Optional, Any
from fabric.connection import Connection


@dataclass
class Job(JobInfo, Output):
    """HSP job."""
    db_id: Optional[int] = None
    progress_bar: Optional[tqdm] = None
    run_settings: Any = None
    connection: Optional[Connection] = None
    job_script: Optional[str] = None