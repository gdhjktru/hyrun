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
    
    def get_status(self, job, connection = None):
        if  connection:
            return self._get_state_in_ctx(job, connection)
        with connect_to_remote(self.connection) as connection:
            return self._get_state_in_ctx(job, connection)
        
    def _get_state_in_ctx(self, job, connection):
        # cmd = f'squeue -j {job.job_id}'
        # c = connection.run(cmd, warn=True)
        # if c.ok:
        #     return 'running'
        cmd = f'sacct -j {job.job_id}.0 --format=state --noheader'
        c = connection.run(cmd, warn=True)
        if c.ok:
            return replace(job, job_status=c.stdout.strip())
        # return replace(job, job_status='unknown')
        return replace(job, job_status=c.stdout.strip())

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
            c = connection.run(cmd, hide='stdout')  # type: ignore
            job_id = -1
            if c.ok:
                output = c.stdout.strip()
                job_id = int(output.split()[-1])
            else:
                raise RuntimeError(c.stderr.strip())
        return replace(job, job_id=job_id, job_status= 'SUBMITTED')
                

    def gen_job_script(self, job):
        """Generate job script."""
        return gjs(job)
    
    def transfer_files(self, files_to_transfer, connection):
        """Transfer files."""
        result = []
        for target in files_to_transfer.keys():
            sources = ' '.join(files_to_transfer[target])
            r =  rsync(connection, sources, [target])
            result.append(r)
        return result

    def cancel(self, *args, **kwargs):
        """Cancel job."""
        pass

    def is_finished(self, job):
        """Check if job is finished."""
        finished = ['COMPLETED', 'FINISHED', 'FAILED', 'CANCELLED']
        # if not connection:
        #     return self.get_status(job, connection).job_status in finished
        return job.job_status.upper() in finished

    def fetch_results(self, job, *args, **kwargs):
        """Fetch results."""
        remote_dirs = []
        local_dirs = []
        for rs in job.tasks:
            for dr in ['work_dir_remote', 'submit_dir_remote']:
                if not hasattr(rs, dr):
                    continue
                if getattr(rs, dr) not in remote_dirs:
                    remote_dirs.append(str(getattr(rs, dr)) + '/*')
            for dl in ['work_dir_local', 'submit_dir_local']:
                if not hasattr(rs, dl):
                    continue
                if getattr(rs, dl) not in local_dirs:
                    local_dirs.append(str(getattr(rs, dl))+ '/')
       
        r = []
        with connect_to_remote(self.connection) as connection:
            for remote_dir, local_dir in zip(remote_dirs, local_dirs):
                r.append(rsync(connection,  remote_dir, [local_dir],download=True))
        return r


        #  files_to_get = self.run_remote(cmd=f'ls -d {remote_dir}/*',
        #                                connection=job.connection)

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
