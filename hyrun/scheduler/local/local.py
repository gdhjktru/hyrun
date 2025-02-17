import os
import subprocess
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from shlex import join, split
from typing import Any, List, Optional, Union

from hyset import RunSettings
from hytools.file import File, FileManager
from hytools.logger import LoggerDummy

from hyrun.job import Job, JobMetaData

from ..abc import Scheduler
from .job_script import JobScript

# from hyrun.decorators import list_exec


class LocalScheduler(Scheduler):
    """Local scheduler."""

    def __init__(self, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', LoggerDummy())
        self.logger.debug('Local scheduler initialized')
        # self.default_data_path = 'data_path_local'
        self.name = 'local'

    def __repr__(self):
        """Represent."""
        return f'{self.__class__.__name__}()'

    def __eq__(self, other):
        """Check equality."""
        if not isinstance(other, LocalScheduler):
            return False
        return self.name == other.name

    def __hash__(self):
        """Hash."""
        return hash(self.name)

    # def check_job_params(self, job):
    #     """Check job params."""
    #     if len(job.tasks) > 1:
    #         self.logger.warning('Local scheduler only supports
    #  one task per ' +
    #                             'job, converting...')
    #         return self.separate_jobs(job)
    #     return job

    # def separate_jobs(self, job) -> list:
    #     """Separate jobs."""
    #     return [replace(job, tasks=[t]) for t in job.tasks]

    def run_ctx(self, arg: Optional[Any] = None):
        """Return context manager."""
        return nullcontext(arg)

    def gen_job_script(self, name: str, tasks: list) -> str:
        """Generate command."""
        job_script = f'# Job: {name or "$job_hash"}\n'
        for i, t in enumerate(tasks):
            wdir = getattr(t, 'work_dir_local', Path.cwd())
            job_script += f'# Run task no. {i} in {wdir}\n'
            job_script += JobScript.gen_job_script(t)
            job_script += '\n' if i < len(tasks) - 1 else ''
        return job_script

    def get_files_to_transfer(self, job):
        """Get files to transfer."""
        return {}

    def resolve_files(self, job):
        """Resolve files."""
        return {}

    def transfer_files(self, *args, **kwargs):
        """Transfer files."""
        return []

    def get_path(self,
                 file: Any,
                 folder_default: Optional[Union[str, Path]] = '') -> Path:
        """Get path from File object."""
        file = File(name=getattr(file, 'name', str(file)),
                    folder=getattr(file, 'folder', str(folder_default)))
        return Path(file.path)

    def gen_output(self,
                   result: Optional[subprocess.CompletedProcess] = None,
                   task: Optional[RunSettings] = None) -> dict:
        """Generate output."""
        try:
            output_dict = result.__dict__
        except AttributeError:
            output_dict = {}
        if result is None:
            return output_dict
        files_to_parse = [self.get_path(f, task.work_dir_local)
                          for f in task.files_to_parse]
        output_folder = task.work_dir_local if task.output_folder else None
        output_file = self.get_path(task.output_file, task.work_dir_local)
        output_dict['output_file'] = output_file
        output_dict['output_folder'] = output_folder
        output_dict['files_to_parse'] = files_to_parse
        if result.stderr:
            stderr_file = self.get_path(task.stderr_file, task.work_dir_local)
            FileManager.write_file_local(stderr_file, result.stderr)
            self.logger.debug('stderr: %s', result.stderr)
            output_dict['stderr_file'] = stderr_file
            output_dict['stderr'] = result.stderr
        if result.stdout:
            stdout_file = self.get_path(task.stdout_file, task.work_dir_local)
            FileManager.write_file_local(stdout_file, result.stdout)
            self.logger.debug('stdout: %s', result.stdout)
            output_dict['stdout_file'] = stdout_file
            output_dict['stdout'] = result.stdout

        return output_dict

    def teardown(self) -> dict:
        """Teardown."""
        # removing files
        return {'scheduler_type': self.name}

    def remove_comments_and_split_commands(self, s: str) -> list:
        """Remove comment lines and split the string into commands."""
        lines = s.split('\n')
        commands = []
        for line in lines:
            # Remove comments and strip whitespace
            line = line.split('#', 1)[0].strip()
            if line:  # Ignore empty lines
                commands.append(line)
        return commands

    def submit(self, job=None, **kwargs):
        """Submit job."""
        cmds = self.remove_comments_and_split_commands(job.job_script.content)
        if len(cmds) != len(job.tasks):
            raise ValueError('Number of commands must match number of tasks')
        outputs = []
        step_data = []
        for i, (t, cmd) in enumerate(zip(job.tasks, cmds)):
            self.logger.debug(f'RUNNING CMD #{i}: {cmd}\n')
            start_time = datetime.now()
            result = self.run_cmd(task=t, cmd=cmd, **kwargs)
            end_time = datetime.now()
            step_metadata = {'start': start_time.isoformat(),
                             'end': end_time.isoformat(),
                             'submit': start_time.isoformat(),
                             'elapsed': timedelta(seconds=(end_time - start_time).total_seconds())}
            step_data.append(JobMetaData(**step_metadata))
            outputs.append(self.gen_output(result, t))
        job.metadata = step_data



        # job.metadata.update({'start_time': start_time.isoformat(),
        #             'end_time': end_time.isoformat(),
        #             'elapsed_time': timedelta(seconds=(end_time - start_time).total_seconds())})
        print('METADATDA', job.metadata)



        job.outputs = outputs

        if sum(r.get('returncode', 0) for r in job.outputs) == 0:
            job.status = 'COMPLETED'
        else:
            job.status = 'FAILED'
        self.logger.debug(f'JOB STATUS: {job.status}\n')
        return job

    def quick_return(self,
                     cmd: str,
                     task: RunSettings
                     ) -> Optional[subprocess.CompletedProcess]:
        """Quick return."""
        files_to_check: List[Union[str, Path]] = ['output_file',
                                                  'stdout_file',
                                                  'stderr_file']
        files_to_check = [self.get_path(getattr(task, str(f)))
                          for f in files_to_check]
        if any(Path(p).exists() for p in files_to_check):
            try:
                stdout = FileManager().read_file_local(task.stdout_file)
                stderr = FileManager().read_file_local(task.stderr_file)
            except Exception:
                stdout = None
                stderr = None
            return subprocess.CompletedProcess(args=cmd,
                                               returncode=0,
                                               stdout=stdout,
                                               stderr=stderr)
        else:
            return None

    def run_cmd(self,
                task: Optional[RunSettings] = None,
                cmd: Optional[Union[str, List[str]]] = None,
                **kwargs) -> subprocess.CompletedProcess:
        """Submit job."""
        if not task.force_recompute:
            result = self.quick_return(str(cmd), task)
            if result:
                self.logger.debug('Output file exists, skipping')
                return result

        run_opt = {'input': None,
                   'env': task.env or os.environ,
                   'capture_output': True,
                   'text': True,
                   'cwd': str(task.work_dir_local),
                   'shell': False,
                   'check': True}
        run_opt.update(kwargs.get('run_opt', {}))
        if task.stdin_file:
            run_opt['input'] = FileManager().read_file_local(task.stdin_file)
        self.logger.debug(f'SETTINGS: {run_opt}\n')

        cmd = join(cmd) if run_opt['shell'] else split(cmd)  # type: ignore
        result = subprocess.run(cmd, **run_opt)  # type: ignore
        if result.returncode != 0:
            self.logger.error(f'RUNNING CMD {cmd} FAILED WITH STDERR\n ' +
                              f'{result.stderr}')
            if cmd[-1] in ['|', '&&']:
                raise ValueError('RUNNING CMD FAILED')
        return result

    def is_finished(self, job, *args, **kwargs) -> bool:
        """Check if job is finished."""
        return job.status in ['COMPLETED', 'FAILED']

    def get_status(self, job=None, **kwargs) -> Job:
        """Get status."""
        self.logger.debug(f'JOB STATUS: {job.status}\n')
        return job

    def cancel(self):
        """Cancel job."""
        self.logger.error('Cancel not implemented for local scheduler\n')

    def fetch_results(self, jobs, *args, **kwargs):
        """Fetch results."""
        pass

    # def check_finished(self, run_settings) -> bool:
    #     """Check if output file exists and return True if it does."""
    #     files_to_check = [getattr(f, 'work_path_local')
    #                       for f in [run_settings.output_file,
    #                                 run_settings.stdout_file,
    #                                 run_settings.stderr_file]
    #                       if getattr(f, 'work_path_local', None) is not None]
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
