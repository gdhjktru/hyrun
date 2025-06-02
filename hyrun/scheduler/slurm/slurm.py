import datetime
import json
from dataclasses import replace
from pathlib import Path
from typing import Optional, Union
from hashlib import sha256
from string import Template
from subprocess import CompletedProcess

from hytools.logger import LoggerDummy

from hyrun.remote import connect_to_remote, rsync

from ..abc import Scheduler
from .job_script import get_job_script as gjs
from .job_script import gen_job_name

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
        if not isinstance(other, SlurmScheduler):
            return False
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

    # def resolve_files(self, job):
    #     """Resolve files."""
    #     files_to_transfer = {}
    #     for t in job.tasks:
    #         for f in t.files_to_write:

    #             local = self._resolve_file(f, parent='work_path_local')
    #             remote = str(Path(self._resolve_file(
    #                 f, parent='work_path_remote')).parent)
    #             if remote not in files_to_transfer:
    #                 files_to_transfer[remote] = []
    #             files_to_transfer[remote].append(local)
    #     local = self._resolve_file(job.job_script, parent='
    # submit_path_local')
    #     remote = str(Path(self._resolve_file(
    #         job.job_script, parent='submit_path_remote')).parent)
    #     if remote not in files_to_transfer:
    #         files_to_transfer[remote] = []
    #     files_to_transfer[remote].append(local)
    #     return files_to_transfer

    def get_status(self, job=None, connection=None, **kwargs):
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
        keys_to_be_identical = ssh_kws + ['memory_per_cpu',
                                          'cpus_per_task',
                                          'ntasks',
                                          'slurm_account',
                                          'submit_dir_remote'
                                          'work_dir_remote']
        first_task = job.tasks[0]
        for k in keys_to_be_identical:
            ref = getattr(first_task, k, None)
            if ref is None:
                continue
            for t in job.tasks[1:]:
                if getattr(t, k) != ref:
                    raise ValueError(f'All slurm tasks must have the same {k}')
        return job

    def submit(self,
               job=None,
               **kwargs):
        """Submit job."""
        job_script_name = Path(job.job_script.get('path')).name  # type: ignore
        self.logger.debug(f'submitting job with job script {job_script_name}')
        cmd = f'sbatch ./{job_script_name}'
        # if connection is None:
        #     with connect_to_remote(self.connection) as connection:
        # return self._submit_in_ctx(job, connection, remote_folder, cmd)
        return self._submit_in_ctx(job=job,
                                   connection=kwargs.get('connection'),
                                   remote_folder=kwargs.get('remote_folder'),
                                   cmd=cmd)

    def _submit_in_ctx(self,
                       job=None,
                       connection=None,
                       remote_folder=None,
                       cmd=None):
        """Submit in context."""
        if connection is None:
            raise ValueError('connection must be provided')
        with connection.cd(remote_folder):  # type: ignore
            c = connection.run(cmd, hide='stdout')  # type: ignore
            job_id = -1
            if c.ok:
                output = c.stdout.strip()
                job_id = int(output.split()[-1])
            else:
                raise RuntimeError(c.stderr.strip())
        job = self.update_remote_wdirs(job=job,
                                       job_id=job_id,
                                       host=connection.host)
        job.id = job_id
        job.status = 'SUBMITTED'
        return job

    def update_remote_wdirs(self, job=None, job_id=None, host=None):
        """Update remote work directories."""
        for t, o in zip(job.tasks, job.outputs):
            wdir = Path(t.work_dir_remote)
            if 'job_id' in wdir.name:
                t.work_dir_remote = wdir.parent / str(job_id)
            else:
                t.work_dir_remote = wdir / str(job_id)
            files = ['output_file', 'stderr_file', 'stdout_file']
            for f in files:
                ff = getattr(o, f, None)
                if ff is None:
                    continue
                name = Path(ff['path']).name
                p = Path(t.work_dir_remote) / name
                setattr(o, f, {'path': str(p), 'host': host})
                setattr(t, f, {'path': str(p), 'host': host})
        return job

    def gen_job_script(self, job):
        """Generate job script."""
        return gjs(job)

    def get_job_summary(self, job):
        """Get job summary."""
        summary = getattr(job,'metadata', {})
        cmd = f'seff {job.id}'
        # execute command


        # update job.metdadata
#         Job ID: 14005811
# Cluster: saga
# User/Group: tilmann/tilmann
# State: COMPLETED (exit code 0)
# Cores: 1
# CPU Utilized: 00:00:02
# CPU Efficiency: 50.00% of 00:00:04 core-walltime
# Job Wall-clock time: 00:00:04
# Memory Utilized: 43.54 MB
# Memory Efficiency: 2.18% of 1.95 GB

    def transfer_files(self,
                       files_to_transfer: Optional[list] = None,
                       connection=None,
                       folder: Optional[Union[str, Path]] = None,
                       **kwargs):
        """Transfer files."""
        files_to_transfer = files_to_transfer or []
        host = connection.host
        download = (files_to_transfer[0]['host'] == host)
        sources = list(set([str(f['path']) for f in files_to_transfer]))

        if not download:
            connection.run(f'mkdir -p {folder}', hide='stdout')

        return rsync(connection,
                     ' '.join(sources),
                     str(folder),
                     download=download, logger=getattr(self, 'logger', None))

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

    def run_ctx(self, *args, **kwargs):
        """Run context manager."""
        return connect_to_remote(self.connection)
    


    def get_cancel_cmd(self, job, **kwargs):
        """Cancel job."""
        return f'scancel {job.job_id}'
    
    def parse_cancel_output(self, output: str) -> bool:
        """Parse cancel output."""
        if 'Cancelled job' in output:
            self.logger.info(f'Job cancelled successfully: {output}')
            return True
        else:
            self.logger.error(f'Failed to cancel job: {output}')
            return False

    def get_summary_cmd(self, job, **kwargs):
        """Get job summary."""
        return f'seff {job.id}'

    def parse_summary(self, summary_output: str) -> dict:
        """Parse summary output."""
        lines = summary_output.strip().split('\n')
        if not lines:
            return {}
        headers = ['job_id', 'cluster', 'user_group', 'state', 'cores',
                   'cpu_utilized', 'cpu_efficiency', 'wall_clock_time',
                   'memory_utilized', 'memory_efficiency']
        data = [line.split(': ', 1) for line in lines if ': ' in line]
        return {headers[i]: data[i][1] for i in range(len(headers))
                if i < len(data)}

    def submit(self, job, connection, **kwargs) -> Union[str, int]:
        """Submit job."""
        # gen submit command, run it if there is an eecutor and return parsed   output
        cmd = self.get_submit_cmd(job, **kwargs)
        if executor is None:
            return cmd
        output = executor.run(cmd, hide='stdout')
        return self.parse_submit_output(output.stdout)
    
    def get_submit_cmd(self, job, **kwargs):
        """Submit job."""
        file = job.tasks[0].run_settings.get_full_file_path(
            file=job.job_script.name, dirname='submit_dir_remote'
            )
        return f'sbatch {file}'
    
    def parse_submit_output(self, output: str) -> Optional[str]:
        """Parse submit output."""
        try:
            job_id = int(output.strip().split()[-1])
            return str(job_id)
        except (ValueError, IndexError):
            self.logger.error(f'Failed to parse job ID from output: {output}')
            return None
        

    def get_job_script(self, job, **kwargs) -> str:
        """Get job script."""
        return gjs(job, **kwargs)

    def get_status_cmd(self, job, **kwargs):
        """Get job status."""
        return f'squeue -h -u {job.job_id} -o "%i %j %S %V %T %M %L"'
    
    def parse_status_output(self, status_output: str) -> dict:
        """Parse status output."""
        lines = status_output.strip().split('\n')
        if not lines:
            return {}
        headers = ['job_id', 'job_name', 'start_time', 'submission_time', 'state', 'time_used', 'time_limit']
        data = [line.split() for line in lines]
        return {headers[i]: data[0][i] for i in range(len(headers)) if i < len(data[0])}
    


