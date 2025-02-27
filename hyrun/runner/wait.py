from time import sleep
from typing import Generator, Optional, List, Union

from hytools.time import get_timedelta
from hytools.connection import Connection
from hytools.logger import Logger
from datetime import timedelta

from .progress_bar import ProgressBar
from ..job import Job


def get_timeout(jobs: List[Job],
                wait: Optional[Union[bool, timedelta]] = None) -> int:
    """Get timeout."""
    timeout = max(
        (task.wait.total_seconds() for job in jobs for task in job.tasks),
        default=0
        ) if not wait else get_timedelta(wait).total_seconds()
    return timeout


def get_progress_bars(jobs: List[Job],
                      timeout: Optional[int] = None) -> List[ProgressBar]:
    """Get progress bars."""
    timeout = timeout or 0
    progress_bars = []
    for job in jobs:
        show_progress = any(task.progress_bar for task in job.tasks)
        if not show_progress:
            progress_bars.append(None)
            continue
        progress_bar = ProgressBar(msg=f'Job {job.job_hash}',
                                   length=timeout)
        progress_bars.append(progress_bar)
    return progress_bars


def update_progress_bars(progress_bars: List[Optional[ProgressBar]],
                         msg: Optional[Union[str, List[str]]] = None,
                         percentage: Optional[Union[float, list]] = None,
                         logger: Optional[Logger] = None
                         ) -> None:
    """Update progress bars."""
    percentage = percentage or []
    msg = msg or []
    if isinstance(percentage, float):
        percentage = [percentage] * len(progress_bars)
    if isinstance(msg, str):
        msg = [msg] * len(progress_bars)

    if len(percentage) != len(progress_bars):
        logger.error('Length of percentage and progress_bars do not match')
        return
    if len(msg) != len(progress_bars):
        logger.error('Length of msg and progress_bars do not match')
        return
    for i, pb in enumerate(progress_bars):
        if pb:
            pb.update(msg[i], percentage=percentage[i])


def wait_for_jobs(jobs: List[Job],
                  connection: Optional[Connection] = None,
                  timeout: Optional[int] = None,
                  progress_bars: Optional[list] = []) -> List[Job]:
    """Wait for job to finish."""
    timeout = timeout or 0
    progress_bars = progress_bars or []
    incrementer = Wait._increment_and_sleep(1)
    for t in incrementer:
        jobs = [job.scheduler.get_status(job, connection=connection)
                for job in jobs]
        for pb in progress_bars:
            pb.update('Running', percentage=t / timeout)
        if t >= timeout or all(job.get_level() > 30 for job in jobs):
            break
    return jobs


class Wait:
    """Wait class."""

    @classmethod
    def _increment_and_sleep(cls, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = cls._increment_t(t)

    @classmethod
    def _increment_t(cls, t, tmin=1, tmax=60) -> int:
        """Increment t."""
        return min(max(2 * t, tmin), tmax)
