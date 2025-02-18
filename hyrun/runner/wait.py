from hytools.time import get_timedelta
from .progress_bar import ProgressBar
from time import sleep
from typing import Generator
from .prepare_job import JobPrep



def get_timeout(jobs, wait=None) -> int:
    """Get timeout."""
    timeout = max(
        (task.wait.total_seconds() for job in jobs for task in job.tasks),
        default=0
    ) if not wait else get_timedelta(wait).total_seconds()
    return timeout

def get_progress_bars(jobs, timeout) -> list:
    """Get progress bars."""
    progress_bars = []
    for job in jobs:
        show_progress = any(task.progress_bar for task in job.tasks)
        if not show_progress:
            continue
        progress_bar = ProgressBar(msg=f'Job {job.job_hash}',
                                   length=timeout)
        progress_bars.append(progress_bar)
    return progress_bars



def _increment_t(t, tmin=1, tmax=60) -> int:
    """Increment t."""
    return min(max(2 * t, tmin), tmax)

def _increment_and_sleep(t) -> Generator[int, None, None]:
    """Increment and sleep."""
    while True:
        yield t
        sleep(t)
        t = _increment_t(t)


def wait_for_jobs(jobs, connection=None, timeout=None, progress_bars=[]) -> dict:
    """Wait for job to finish."""
    incrementer = _increment_and_sleep(1)
    for t in incrementer:
        print('increenting', t)
        jobs = [job.scheduler.get_status(job, connection=connection) for job in jobs]
        for pb in progress_bars:
            pb.update('Running', percentage=t/timeout)
        if t >= timeout or all(job.get_level() > 30 for job in jobs):
            break
    return jobs



# class Wait:




#     def is_finished(self, jobs) -> bool:
#         """Check if job is finished."""
#         return all(job['scheduler'].is_finished(job['job'])
#                    for job in jobs.values())

#     @classmethod
#     def _get_timeout(self, jobs):
#         """Get timeout."""
#         return max(sum([t.job_time.total_seconds()
#                         if isinstance(t.job_time, timedelta)
#                         else t.job_time for t in j['job'].tasks])
#                    for j in jobs.values())

   
   