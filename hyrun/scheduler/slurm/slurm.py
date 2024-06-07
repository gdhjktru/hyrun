import hashlib

from dataclasses import replace
from hytools.file import File
from hytools.logger import LoggerDummy

from hyrun.remote import connect_to_remote, rsync
from pathlib import Path

from ..abc import Scheduler
from .job_script import gen_job_script as gjs

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
    
    def _resolve_file(self, file, parent='work_path_local'):
        p = (Path(file.folder) / file.name if file.folder is not None
             else getattr(file, parent, None))
        return str(p)
    
    def resolve_files(self, job):
        """Resolve files."""
        files_to_transfer = {}
        for t in job.tasks:
            for f in t.files_to_write:
                local = self._resolve_file(f, parent='work_path_local')
                remote = str(Path(self._resolve_file(f, parent='work_path_remote')).parent)
                if remote not in files_to_transfer:
                    files_to_transfer[remote] = []
                files_to_transfer[remote].append(local)
        local = self._resolve_file(job.job_script, parent='submit_path_local')
        remote = str(Path(self._resolve_file(job.job_script, parent='submit_path_remote')).parent)
        if remote not in files_to_transfer:
            files_to_transfer[remote] = []
        files_to_transfer[remote].append(local)
        return files_to_transfer 

    def get_connection(self, **kwargs):
        """Get connection."""
        return {k: v for k, v in kwargs.items() if k in ssh_kws}

    def check_job_params(self, job):
        """Check job params."""
        keys_to_be_identical = ssh_kws + ['memory_per_cpu', 'cpus_per_task',
                                          'ntasks', 'slurm_account',
                                          'submit_dir_remote']
        for k in keys_to_be_identical:
            for t in job.tasks:
                if getattr(t, k) != getattr(job.tasks[0], k):
                    raise ValueError(f'All slurm tasks must have the same {k}')
        return job
    
    def submit(self, job, connection):
        """Submit job."""
        remote_dir = Path(job.job_script.submit_path_remote).parent 
        job_script_name = Path(job.job_script.submit_path_remote).name
        cmd = f'sbatch ./{job_script_name}'
        with connection.cd(remote_dir):  # type: ignore
            # c = connection.run(cmd, hide='stdout')  # type: ignore
            c = connection.run(cmd)  # type: ignore
            if c.ok:
                output = c.stdout.strip()
                db_id = int(output.split()[-1])
            else:
                raise RuntimeError(c.stderr.strip())
        return replace(job, db_id=db_id)
                


    def gen_job_script(self, job):
        """Generate job script."""
        return gjs(job)
    
    def transfer_files(self, files_to_transfer, connection):
        """Transfer files."""
        result = []
        for target in files_to_transfer.keys():
            print('target', target)
            sources = ' '.join(files_to_transfer[target])
            print('sources', sources)
            r =  rsync(connection, sources, [target])
            result.append(r)
        return result

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
