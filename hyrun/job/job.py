
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, List, Optional, Union
from functools import singledispatch, register

from hydb import Database

from hyrun.scheduler.abc import Scheduler

from .output import Output
from hashlib import sha256

# from tqdm import tqdm


@dataclass
class Job:
    """Dataclass containing information about a job."""

    job_id: Optional[int] = None
    job_script: Optional[str] = None
    status: Optional[str] = None
    hash: Optional[str] = None
    metadata: Optional[dict] = field(default_factory=dict)
    tasks: Optional[List[Any]] = field(default_factory=list)
    outputs: Optional[List[Output]] = field(default_factory=list)

    def __post_init__(self):
        """Post init."""
        self.set_hash()
        check_common(self.tasks, keys=['database', 'scheduler'])


    def set_hash(self):
        """Get hash of the job."""
        if self.hash:
            return
        if self.job_script:
            txt = getattr(self.job_script, 'content', None) or ''
            self.hash = sha256(txt.encode()).hexdigest()


# @dataclass
# class Job(JobInfo):
#     """HSP job."""

    
#     job_script: Optional[str] = None
#     database: Optional[Union[str, Path, Database]] = 'dummy'
#     db_id: Optional[int] = None
#     database_opt: Optional[dict] = field(default_factory=dict)
#     scheduler: Optional[Union[str, Scheduler]] = 'local'
#     scheduler_opt: Optional[dict] = field(default_factory=dict)
#     connection_type: Optional[str] = None
#     connection_opt: Optional[dict] = field(default_factory=dict)
#     files: Optional[List[Union[Path, str]]] = None

#     # def __post_init__(self):
#     #     """Post init."""
#     #     self.database = self.database or 'dummy'
#     #     self.scheduler = self.scheduler or 'local'
#     #     # not sure if this is the right place here
#     #     if not self.tasks and not self.outputs and self.db_id:
#     #         db = get_database(self.database, **self.database_opt)
#     #         entry = db.search_one(id=self.db_id)
#     #         # update all fields from self
#     #         self.__dict__.update(entry)


@singledispatch
def get_job(job: Any) -> Job:
    """Convert input to Job."""
    raise TypeError(f'Cannot convert {type(job)} to Job')

@get_job.register(Job)
def _(job: Job) -> Job:
    """Convert Job to Job."""
    return job

@get_job.register(dict)
def _(job: dict) -> Job:
    """Convert dictionary to Job."""
    job_fields = {f.name for f in fields(Job)}
    filtered = {k: v for k, v in job.items() if k in job_fields}
    return Job(**filtered)

def check_common(tasks: List[Any], keys: List[str]):
    """Check that jobs have the same values for a list of attributes."""
    for attr in keys:
        values = []
        for task in tasks:
            value = getattr(task, attr, None)
            if isinstance(value, dict):
                # Convert dictionary to a tuple of sorted items
                value = tuple(sorted(value.items()))
            values.append(value)
        if len(set(values)) > 1:
            raise ValueError(
                f'All tasks in a job must have the same {attr} attribute'
            )
    return 

