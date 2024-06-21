from functools import wraps


def loop_update_jobs(func):
    """Decorate to loop over jobs."""
    @wraps(func)
    def wrapper(self, jobs: dict, *args, **kwargs) -> dict:
        """Loop over jobs."""
        if not isinstance(jobs, dict):
            raise ValueError('@loop_over_jobs: jobs must be a dictionary')
        for i, job in jobs.items():
            jobs[i]['job'] = func(self, *args, **job, **kwargs)
        return jobs
    return wrapper
