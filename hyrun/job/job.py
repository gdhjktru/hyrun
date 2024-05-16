
from dataclasses import dataclass
from typing import Any, List, Optional

from fabric.connection import Connection

from .job_info import JobInfo
from .output import Output

# from tqdm import tqdm


@dataclass
class Job(JobInfo, Output):
    """HSP job."""
    db_id: Optional[int] = None
    # progress_bar: Optional[tqdm] = None
    run_settings: Any = None
    connection: Optional[Connection] = None
    job_script: Optional[str] = None
    local_files: Optional[List[str]] = None
    remote_files: Optional[List[str]] = None
