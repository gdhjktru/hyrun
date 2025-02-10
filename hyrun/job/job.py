
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, List, Optional, Union

from hydb import Database
from hyset import RunSettings
from hytools.connection import Connection
from hytools.file import File

from hyrun.scheduler.abc import Scheduler

from .output import Output


@dataclass
class Job:
    """Dataclass containing information about a job."""

    # general information
    scheduler_id: Optional[int] = None
    database_id: Optional[int] = None
    # not part of hash
    status: Optional[str] = None
    job_hash: Optional[str] = None
    metadata: Optional[dict] = None
    # actual job stuff
    tasks: Optional[List[RunSettings]] = None
    outputs: Optional[List[Output]] = None
    job_script: Optional[Union[str, Path, File]] = None
    # to be teared down
    database: Optional[Union[str, Path, dict, Database]] = 'dummy'
    scheduler: Optional[Union[str, dict, Scheduler]] = 'local'
    connection: Optional[Union[str, dict, Connection]] = ''

    def __post_init__(self):
        """Post init."""
        self.metadata = self.metadata or {}
        self.metadata.setdefault('time', datetime.now().isoformat())

    def teardown(self):
        """Teardown."""
        self.scheduler = getattr(self.scheduler, 'teardown', self.scheduler)
        self.connection = getattr(self.connection, 'teardown', self.connection)
        self.database = getattr(self.database, 'teardown', self.database)

    def gen_hash(self) -> str:
        """Generate hash."""
        hash_str = f'{self.scheduler_id or ""}'
        hash_str += f'{self.database_id or ""}'
        hash_str += f'{getattr(self.database, "name", "")}'
        hash_str += f'{getattr(self.scheduler, "name", "")}'
        hash_str += f'{getattr(self.connection, "connection_type", "")}'
        hash_str += f'{getattr(self.connection, "user", "")}'
        hash_str += f'{getattr(self.connection, "host", "")}'
        hash_str += f'{self.job_script or ""}'
        if not hash_str:
            raise ValueError('job_hash is empty')
        self.job_hash = sha256(hash_str.encode()).hexdigest()
        return self.job_hash
