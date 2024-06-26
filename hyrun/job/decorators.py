from functools import wraps


def loop_update_jobs(func):
    """Decorate to loop over jobs."""
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> dict:
        """Loop over jobs."""
        if len(args) < 1:
            return func(self, **kwargs)
        if not isinstance(args[0], dict):
            return func(self, *args, **kwargs)
        # if not isinstance(jobs, dict):
        #     raise ValueError('@loop_over_jobs: jobs must be a dictionary')
        for i, job in args[0].items():
            args[0][i]['job'] = func(self, *args, **job, **kwargs)
        return args[0]
    return wrapper
