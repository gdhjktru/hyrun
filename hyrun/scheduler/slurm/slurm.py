from hytools.logger import LoggerDummy

from hyrun.remote import connect_to_remote
from .job_script import gen_job_script as gjs
from ..abc import Scheduler

ssh_kws = ['host', 'user', 'port', 'config', 'gateway', 'forward_agent',
           'connect_timeout', 'connect_kwargs', 'inline_ssh_env']





class SlurmScheduler(Scheduler):
    """Slurm scheduler."""

    def __init__(self, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', LoggerDummy())
        self.name = 'slurm'
        self.default_data_path = 'data_path_remote'
        self.connection = kwargs.get('connection',
                                     self.get_connection(**kwargs))

    def __eq__(self, other):
        """Check equality."""
        return (self.name == other.name
                and self.connection == other.connection)

    def __hash__(self):
        """Hash."""
        connection_items = (tuple((k, v)
                                  for k, v in sorted(self.connection.items())
                                  if not isinstance(v, dict)))
        return hash((self.name, connection_items))

    def get_connection(self, **kwargs):
        """Get connection."""
        return {k: v for k, v in kwargs.items() if k in ssh_kws}

    def check_job_params(self, job):
        """Check job params."""
        keys_to_be_identical = ssh_kws + ['memory_per_cpu', 'cpus_per_task',
                                          'ntasks', 'slurm_account']
        for k in keys_to_be_identical:
            for t in job.tasks:
                if getattr(t, k) != getattr(job.tasks[0], k):
                    raise ValueError(f'All slurm tasks must have the same {k}')
        return job
    
    def gen_job_script(self, job):
        """Generate job script."""
        return gjs(job)

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
        return connect_to_remote(self.connection)

    def teardown(self, *args, **kwargs):
        """Teardown job."""
        pass

    def check_finished(self, run_settings) -> bool:
        """Check if output file exists and return True if it does."""
        files_to_check = [getattr(f, 'work_path_local')
                          for f in [run_settings.output_file,
                                    run_settings.stdout_file,
                                    run_settings.stderr_file]
                          if getattr(f, 'work_path_local', None) is not None]
        files_to_check = [f for f in files_to_check
                          if f.name not in ['stdout.out', 'stderr.out']]
        if any(f.exists() for f in files_to_check if f is not None):
            self.logger.debug(f'(one of) output file(s) {files_to_check} ' +
                              'exists')
        else:
            return False

        force_recompute = run_settings.force_recompute
        self.logger.info('force_recompute is %s, will %srecompute\n',
                         'set' if force_recompute else 'not set',
                         '' if force_recompute else 'not ')
        return not force_recompute
