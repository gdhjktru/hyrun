
from dataclasses import dataclass
from typing import Optional


@dataclass
class JobInfo:
    """Dataclass containing information about a job."""

    id: int = -1
    db_id: Optional[int] = None
    name: Optional[str] = None
    finished: bool = False
    status: Optional[str] = None
    hash: Optional[str] = None
