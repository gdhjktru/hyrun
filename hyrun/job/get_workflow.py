from ..job import JobGraph, get_job
from typing import Any

def get_workflow(*args, **kwargs):
    if len(args) == 0:
        raise ValueError('No arguments provided')
    
    jobs = normalize_input(*args)
    return JobGraph(jobs=jobs, **kwargs)

def normalize_input(jobs: Any) -> list:
    """Normalize input to a list of lists."""
    jobs = [jobs] if not isinstance(jobs, list) else jobs
    return [get_job(job) for job in jobs]





