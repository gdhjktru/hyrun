from hytools.logger import LoggerDummy

from ..abc import Scheduler


class SlurmScheduler(Scheduler):
    """Slurm scheduler."""

    def __init__(self, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', LoggerDummy())
        self.name = 'slurm'
        self.default_data_path = 'data_path_remote'

    def check_job_params(self, job):
        """Check job params."""
        keys_to_be_identical = ['user', 'host']
        for k in keys_to_be_identical:
            if len(set([getattr(t, k) for t in job.tasks])) > 1:
                raise ValueError(f'All slurm tasks must have the same {k}')
        return job

    def cancel(self, *args, **kwargs):
        """Cancel job."""
        pass

    def fetch_results(self, *args, **kwargs):
        """Fetch results."""
        pass

    def quick_return(self, *args, **kwargs):
        """Quick return."""
        pass

    def run_ctx(self, *args, **kwargs):
        """Run context manager."""
        pass

    def teardown(self, *args, **kwargs):
        """Teardown job."""
        pass
