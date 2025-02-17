from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional, Union

from hytools.memory import get_memory
from hytools.time import get_time, get_timedelta


@dataclass
class JobMetaData:
    """Dataclass containing meta information about a job see https://slurm.schedmd.com/sacct.html."""

    submit: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    elapsed: Optional[timedelta] = None
    time_limit: Optional[timedelta] = None
    cpu_time: Optional[timedelta] = None
    max_rss: Optional[str] = None
    max_vm_size: Optional[str] = None
    max_disk_read: Optional[str] = None
    max_disk_write: Optional[str] = None
    req_cpus: Optional[int] = None
    alloc_cpus: Optional[int] = None
    alloc_nodes: Optional[int] = None
    req_mem: Optional[str] = None
    node_list: Optional[str] = None

    def __post_init__(self):
        """Post init."""
        self.start = get_time(self.start)
        self.submit = get_time(self.submit)
        self.end = get_time(self.end)
        self.elapsed = get_timedelta(self.elapsed)
        self.time_limit = get_timedelta(self.time_limit)
        self.cpu_time = get_timedelta(self.cpu_time)
        self.max_rss = get_memory(self.max_rss)
        self.max_vm_size = get_memory(self.max_vm_size)
        self.max_disk_read = get_memory(self.max_disk_read)
        self.max_disk_write = get_memory(self.max_disk_write)
        self.req_mem = get_memory(self.req_mem)
