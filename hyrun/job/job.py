
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Union

from hydb import Database

from hyrun.scheduler.abc import Scheduler

from .output import Output

# from tqdm import tqdm


@dataclass
class JobInfo:
    """Dataclass containing information about a job."""

    id: Optional[int] = None
    name: Optional[str] = None
    finished: bool = False
    status: Optional[str] = None
    job_hash: Optional[str] = None
    metadata: Optional[dict] = field(default_factory=dict)


@dataclass
class Job(JobInfo):
    """HSP job."""

    tasks: Optional[List[Any]] = None
    outputs: Optional[List[Output]] = None
    job_script: Optional[str] = None
    database: Optional[Union[str, Path, Database]] = 'dummy'
    db_id: Optional[int] = None
    database_opt: Optional[dict] = field(default_factory=dict)
    scheduler: Optional[Union[str, Scheduler]] = 'local'
    scheduler_opt: Optional[dict] = field(default_factory=dict)
    files: Optional[List[Union[Path, str]]] = None

    # def __post_init__(self):
    #     """Post init."""
    #     self.database = self.database or 'dummy'
    #     self.scheduler = self.scheduler or 'local'
    #     # not sure if this is the right place here
    #     if not self.tasks and not self.outputs and self.db_id:
    #         db = get_database(self.database, **self.database_opt)
    #         entry = db.search_one(id=self.db_id)
    #         # update all fields from self
    #         self.__dict__.update(entry)
