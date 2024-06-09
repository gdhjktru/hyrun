
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

from hydb import Database

from hyrun.scheduler.abc import Scheduler


@dataclass
class JobInfo:
    """Dataclass containing information about a job."""

    id: int = -1
    db_id: Optional[int] = None
    name: Optional[str] = None
    finished: bool = False
    status: Optional[str] = None
    hash: Optional[str] = None
    database: Optional[Union[str, Path, Database]] = None
    scheduler: Optional[Union[str, Scheduler]] = None
    metadata: Optional[dict] = field(default_factory=dict)
