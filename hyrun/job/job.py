
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Union

from hydb import Database, get_database

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
    database_opt: Optional[dict] = field(default_factory=dict)
    scheduler_opt: Optional[dict] = field(default_factory=dict)
    metadata: Optional[dict] = field(default_factory=dict)

    def __post_init__(self):
        """Post init."""
        self.database = self.database or 'dummy'
        self.scheduler = self.scheduler or 'local'
        # not sure if this is needed here
        if not self.tasks and not self.outputs and self.db_id:
            db = get_database(self.database, **self.database_opt)
            entry = db.search_one(id=self.db_id)
            # update all fields from self
            self.__dict__.update(entry)
