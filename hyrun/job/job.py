
from dataclasses import asdict, dataclass, field, fields
from functools import singledispatch
from hashlib import sha256
from typing import Any, List, Optional

from .output import Output

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
        self.tasks = ([self.tasks]
                      if not isinstance(self.tasks, list) else self.tasks)
        check_common_dataclass(self.tasks,
                               keys=['database', 'scheduler', 'connection'])

    def set_hash(self):
        """Get hash of the job."""
        if self.hash:
            return
        if self.job_script:
            txt = getattr(self.job_script, 'content', None) or ''
            self.hash = sha256(txt.encode()).hexdigest()


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


@get_job.register(list)
def _(job: list) -> Job:
    """Convert dictionary to Job."""
    # if list or list of lists
    if not job:
        return Job()
    return Job(tasks=job)


def check_common_dataclass(tasks: List[Any], keys: List[str]):
    """Check that jobs have the same values for a list of attributes."""
    if len(tasks) < 2:
        return
    for key in keys:
        # Collect all non-None attribute values as dicts
        values = []
        for task in tasks:
            val = getattr(task, key, None)
            if val is None:
                continue
            if hasattr(val, '__dataclass_fields__'):
                val = asdict(val)
            elif not isinstance(val, dict):
                raise ValueError(
                    f'Attribute {key} must be a dataclass or a dictionary'
                    )
            values.append(val)
        # If there are any values, they must all be the same
        if values and any(v != values[0] for v in values[1:]):
            raise ValueError(
                f'All tasks in a job must have the same {key} attribute'
                )
    return
