
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import List, Optional, Union

from hydb import Database
from hyset import RunSettings
from hytools.connection import Connection
from hytools.file import File

from hyrun.scheduler.abc import Scheduler

from .output import Output

STATUS_MAP = {'UNKNOWN': 0,
              'PENDING': 10,
              'RUNNING': 20,
              'COMPLETED': 30,
              'FAILED': 30,
              'CANCELLED': 30,
              'TIMEOUT': 30,
              'DEADLINE': 30,
              'PREEMPTED': 30,
              'NODE_FAIL': 30,
              'OUT_OF_MEMORY': 30,
              'BOOT_FAIL': 30}


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
    scheduler: Optional[Union[str, dict, Scheduler]] = None
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

    def gen_hash(self, **kwargs) -> str:
        """Generate hash."""
        hash_str = f'{self.scheduler_id or ""}'
        hash_str += f'{self.database_id or ""}'
        hash_str += f'{getattr(self.database, "name", "")}'
        hash_str += f'{getattr(self.scheduler, "name", "")}'
        hash_str += f'{getattr(self.connection, "connection_type", "")}'
        hash_str += f'{getattr(self.connection, "user", "")}'
        hash_str += f'{getattr(self.connection, "host", "")}'
        hash_str += f'{kwargs.get("job_script", self.job_script) or ""}'
        if not hash_str:
            raise ValueError('job_hash is empty')
        return sha256(hash_str.encode()).hexdigest()

    def get_level(self) -> int:
        """Get level."""
        return STATUS_MAP.get(str(self.status).upper(), 0)
