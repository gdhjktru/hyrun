
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, List, Optional, Union

from hydb import Database
from hytools.connection import Connection

from hyrun.scheduler.abc import Scheduler

from .output import Output


@dataclass
class JobInfo:
    """Dataclass containing information about a job."""

    id: Optional[int] = None
    db_id: Optional[int] = None
    status: Optional[str] = None
    job_hash: Optional[str] = None
    metadata: Optional[dict] = None

    def __post_init__(self):
        """Post init."""
        self.metadata = self.metadata or {}
        self.metadata.setdefault('time', datetime.now().isoformat())

@dataclass
class Job(JobInfo):
    """HSP job."""

    tasks: Optional[List[Any]] = None
    outputs: Optional[List[Output]] = None
    job_script: Optional[str] = None

    database: Optional[Union[str, Path, dict, Database]] = 'dummy'
    scheduler: Optional[Union[str, dict, Scheduler]] = 'local'
    connection: Optional[Union[str, dict, Connection]] = ''
    # files: Optional[List[Union[Path, str]]] = None # already in tasks

    def __post_init__(self):
        """Post init."""
        self.database_opt = self.database_opt or {}
        self.scheduler_opt = self.scheduler_opt or {}
        self.connection_opt = self.connection_opt or {}



    #     self.database = self.database or 'dummy'
    #     self.scheduler = self.scheduler or 'local'
    #     # not sure if this is the right place here
    #     if not self.tasks and not self.outputs and self.db_id:
    #         db = get_database(self.database, **self.database_opt)
    #         entry = db.search_one(id=self.db_id)
    #         # update all fields from self
    #         self.__dict__.update(entry)
