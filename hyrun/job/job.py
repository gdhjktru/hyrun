
from dataclasses import asdict, dataclass, replace
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
              'SUBMITTED': 10,
              'PENDING': 20,
              'RUNNING': 30,
              'FAILED': 40,
              'CANCELLED': 40,
              'TIMEOUT': 40,
              'DEADLINE': 40,
              'PREEMPTED': 40,
              'NODE_FAIL': 40,
              'OUT_OF_MEMORY': 40,
              'BOOT_FAIL': 40,
              'COMPLETED': 50}


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
        self.metadata.setdefault('time_init', datetime.now().isoformat())

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

    def get_level(self, d: Optional[dict] = {}) -> int:
        """Get level."""
        status = d.get('status', self.status)
        return STATUS_MAP.get(str(status).upper(), 0)

    def update(self, d: Optional[dict] = None):
        """Update."""
        if not d:
            return
        if self.get_level(d) <= self.get_level():
            return

        updated_data = {
            **asdict(self),
            **{k: v for k, v in d.items() if v is not None}
        }
        self.__dict__.update(replace(self, **updated_data).__dict__)
