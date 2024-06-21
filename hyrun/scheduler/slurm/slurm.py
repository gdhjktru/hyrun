import datetime
import json
from dataclasses import replace
from pathlib import Path
from typing import Optional, Union

from hytools.logger import LoggerDummy

from hyrun.job import Job
from hyrun.remote import connect_to_remote, rsync

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

    def __repr__(self):
        """Represent."""
        return f'{self.__class__.__name__}({self.connection})'

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
                remote = str(Path(self._resolve_file(
                    f, parent='work_path_remote')).parent)
                if remote not in files_to_transfer:
                    files_to_transfer[remote] = []
                files_to_transfer[remote].append(local)
        local = self._resolve_file(job.job_script, parent='submit_path_local')
        remote = str(Path(self._resolve_file(
            job.job_script, parent='submit_path_remote')).parent)
        if remote not in files_to_transfer:
            files_to_transfer[remote] = []
        files_to_transfer[remote].append(local)
        return files_to_transfer

    def get_files_to_transfer(self, job):
        """Get files to transfer."""
        files_to_transfer = {}
        for t in job.tasks:
            for f in t.files_to_write:
                host = str(f['remote']['host'])
                path_local = str(f['local']['path'])
                if host not in files_to_transfer:
                    files_to_transfer[host] = []
                files_to_transfer[host].append(path_local)
        # add job script
        host = str(job.job_script['remote']['host'])
        path_local = str(job.job_script['local']['path'])
        if host not in files_to_transfer:
            files_to_transfer[host] = []
        files_to_transfer[host].append(path_local)
        return files_to_transfer

    def get_status(self, job=None, connection=None):
        """Get status."""
        if connection:
            return self._get_state_in_ctx(job, connection)
        with connect_to_remote(self.connection) as connection:
            return self._get_state_in_ctx(job, connection)

    def _get_state_in_ctx(self, job, connection):
        if job.id == -1:
            return replace(job, status='UNKNOWN')
        # cmd = f'squeue -j {job.job_id}'
        # c = connection.run(cmd, warn=True)
        # if c.ok:
        # sacct -j 11758903 --format Timelimit,NCPUS,WorkDir,JobID,Start,
        # State,Submit,End
        #     return 'running'
        # cmd = f'sacct -j {job.job_id}.0 --format=state --noheader'
        cmd = f'sacct -j {job.id} --json'
        c = connection.run(cmd, hide='stdout', warn=True)
        if c.stderr:
            self.logger.error(c.stderr)
        try:
            status = json.loads(c.stdout.strip()).get('jobs', [{}])[0]
        except (json.JSONDecodeError, IndexError):
            return replace(job, status='UNKNOWN')

        metadata_keys = ['account', 'cluster', 'job_id', 'name', 'user',
                         'working_directory', 'time']
        metadata = {k: status.get(k, None) for k in metadata_keys}
        metadata['time'] = {k: v for k, v in metadata['time'].items()
                            if isinstance(v, int)}

        for k, v in metadata['time'].items():
            if v == 0:
                continue
            metadata['time'][k] = (datetime.datetime.fromtimestamp(v)
                                   .isoformat())
        status = status.get('state', {}).get('current', ['UNKNOWN'])[0]
        self.logger.info(f'Job {job.id} has status {status}')

        return replace(job, status=status, metadata=metadata)

    def teardown(self, *args, **kwargs):
        """Teardown."""
        pass

    def get_connection(self, **kwargs):
        """Get connection."""
        return {k: v for k, v in kwargs.items() if k in ssh_kws}

    def check_job_params(self, job):
        """Check job params."""
        keys_to_be_identical = ssh_kws + ['memory_per_cpu', 'cpus_per_task',
                                          'ntasks', 'slurm_account',
                                          'submit_dir_remote']
        for k in keys_to_be_identical:
            ref = getattr(job['job'].tasks[0], k)
            if not ref:
                continue
            for t in job['job'].tasks:
                if getattr(t, k) != ref:
                    raise ValueError(f'All slurm tasks must have the same {k}')
        return job

    def submit(self,
               job: Job,
               connection=None,
               remote_folder=None,
               **kwargs):
        """Submit job."""
        job_script_name = Path(job.job_script.get('path')).name  # type: ignore
        self.logger.debug(f'submitting job with job script {job_script_name}')
        cmd = f'sbatch ./{job_script_name}'
        if connection is None:
            with connect_to_remote(self.connection) as connection:
                return self._submit_in_ctx(job, connection, remote_folder, cmd)
        return self._submit_in_ctx(job, connection, remote_folder, cmd)

    def _submit_in_ctx(self, job, connection, remote_dir, cmd):
        """Submit in context."""
        with connection.cd(remote_dir):  # type: ignore
            c = connection.run(cmd, hide='stdout')  # type: ignore
            job_id = -1
            if c.ok:
                output = c.stdout.strip()
                job_id = int(output.split()[-1])
            else:
                raise RuntimeError(c.stderr.strip())
        return replace(job,
                       id=job_id,
                       status='SUBMITTED')

    def gen_job_script(self, job):
        """Generate job script."""
        return gjs(job)

    def transfer_files(self,
                       files_to_transfer: Optional[list] = None,
                       connection=None,
                       folder: Optional[Union[str, Path]] = None,
                       **kwargs):
        """Transfer files."""
        files_to_transfer = files_to_transfer or []
        host = connection.host
        download = (files_to_transfer[0]['host'] == host)
        sources = [str(f['path']) for f in files_to_transfer]

        if not download:
            connection.run(f'mkdir -p {folder}', hide='stdout')

        return rsync(connection,
                     ' '.join(sources),
                     str(folder),
                     download=download)

    def cancel(self, *args, **kwargs):
        """Cancel job."""
        pass

    def is_finished(self, job):
        """Check if job is finished."""
        success = ['COMPLETED']
        failed = ['BOOT_FAIL', 'CANCELLED', 'DEADLINE', 'FAILED', 'NODE_FAIL',
                  'OUT_OF_MEMORY', 'PREEMPTED', 'TIMEOUT']
        if job.status in failed:
            self.logger.error(f'Job {job.id} with id {job.db_id} in ' +
                              f'database {job.database} failed with status ' +
                              f'{job.status}')
        return job.status in success + failed

    def fetch_results(self, job, *args, **kwargs):
        """Fetch results."""
        exclude = kwargs.get('exclude', ())
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
                    local_dirs.append(str(getattr(rs, dl)) + '/')
        connection = kwargs.get('connection', None)
        if not connection:
            with connect_to_remote(self.connection) as conn:
                connection = conn

        r = [rsync(connection, remote_dir, [local_dir], download=True,
                   exclude=exclude)
             for remote_dir, local_dir in zip(remote_dirs, local_dirs)]
        # with connect_to_remote(self.connection) as connection:
        #     for remote_dir, local_dir in zip(remote_dirs, local_dirs):
        #         r.append(rsync(connection,
        #                        remote_dir,
        #                        [local_dir],
        #                        download=True))
        return r

    def quick_return(self, *args, **kwargs):
        """Quick return."""
        pass

    def run_ctx(self, *args, **kwargs):
        """Run context manager."""
        return connect_to_remote(self.connection)

    # def check_finished(self, run_settings) -> bool:
    #     """Check if output file exists and return True if it does."""
    #     work_dir_local = getattr(run_settings, 'work_dir_local', Path('.'))
    #     files_to_check = [Path(work_dir_local)/Path(f.name).name
    #                       for f in [run_settings.output_file,
    #                                 run_settings.stdout_file,
    #                                 run_settings.stderr_file]]
    #     files_to_check = [f for f in files_to_check
    #                       if f.name not in ['stdout.out', 'stderr.out']]

    #     if any(f.exists() for f in files_to_check if f is not None):
    #         self.logger.debug(f'(one of) output file(s) {files_to_check} ' +
    #                           'exists')
    #     else:
    #         return False

    #     force_recompute = run_settings.force_recompute
    #     self.logger.info('force_recompute is %s, will %srecompute\n',
    #                      'set' if force_recompute else 'not set',
    #                      '' if force_recompute else 'not ')
    #     return not force_recompute
