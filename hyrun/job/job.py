
from dataclasses import dataclass
from .output import Output
from .job_info import JobInfo
from tqdm import tqdm
from typing import Optional, Any, List, Union
from fabric.connection import Connection
from hytools.file import File
from pathlib import Path


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