import os
import subprocess
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from shlex import quote, split
from sys import executable as python_ex
from typing import Any, Dict, List, Optional

from hytools.logger import LoggerDummy

from ..abc import Scheduler
from .conda import get_conda_launcher
from .docker import get_docker_launcher
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
    #         self.logger.warning('Local scheduler only supports one task per ' +
    #                             'job, converting...')
    #         return self.separate_jobs(job)
    #     return job

    # def separate_jobs(self, job) -> list:
    #     """Separate jobs."""
    #     return [replace(job, tasks=[t]) for t in job.tasks]

    def run_ctx(self, arg: Optional[Any] = None):
        """Return context manager."""
        return nullcontext(arg)

    # def get_launcher(self, run_settings):
    #     """Get launcher."""
    #     # allows conda in docker but not docker in conda
    #     return get_conda_launcher(run_settings.conda_env,
    #                               [*get_docker_launcher(
    #                                   **run_settings.__dict__),
    #                                   *run_settings.launcher])

    # def _gen_running_list(self, run_settings,
    #                       cwd: Path) -> List[str]:
    #     """Generate running list."""
    #     running_list: List[str] = [
    #         *run_settings.launcher,
    #         run_settings.program,
    #         *run_settings.args  # type: ignore
    #         ]  # type: ignore #14891
    #     running_list = [str(x).strip() for x in running_list]

    #     if not all([isinstance(x, str) for x in running_list]):
    #         raise TypeError(
    #             'subprocess call command must be a list of strings ',
    #             running_list)

    #     running_list = [c.replace('python', python_ex)
    #                     if 'python' in c
    #                     else c for c in running_list]
    #     return running_list


    def gen_job_script(self, name, tasks):
        """Generate command."""
        job_script = f'# Job: {name or "$job_name"}\n'
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

    def gen_output(self, result, run_settings) -> dict:
        """Generate output."""
        output_dict: Dict[str, Any]
        # files_to_parse = run_settings.files_to_parse
        # for i, f in enumerate(files_to_parse):
        #     if isinstance(f, File):
        #         if hasattr(f, 'work_path_local'):
        #             files_to_parse[i] = f.work_path_local  # type: ignore
        output_dict = {
            # 'files_to_parse': files_to_parse,
            'output_folder': (run_settings.work_dir_local
                              if run_settings.output_folder
                              else None),
            }

        if run_settings.output_file:
            output_dict.update(
                {'output_file':
                 run_settings.output_file['path']})  # type: ignore
        if result is None:
            return output_dict

        output_dict.update({'returncode': result.returncode})

        exceptions = ['normal termination of xtb']
        if result.stderr:
            stderr_file = run_settings.stderr_file['path']
            Path(stderr_file).write_text(result.stderr)
            self.logger.debug('STDERR: %s', result.stderr) if any(
                e in result.stderr for e in exceptions
                ) else self.logger.error('STDERR: %s', result.stderr)
            # output_dict['stderr'] = result.stderr

        if result.stdout:
            stdout_file = run_settings.stdout_file['path']
            Path(stdout_file).write_text(result.stdout)
            # output_dict['stdout'] = stdout_file
        return output_dict

    def teardown(self) -> dict:
        """Teardown."""
        # removing files
        return {'name': self.name}

    def update_output(self, result=None, run_settings=None, output=None):
        """Update output."""
        output_dict = {}
        # if output.output_file:
        #     output_dict['output_file'] = output.output_file['path']
        # if output.stdout_file:
        #     output_dict['stdout_file'] = output.stdout_file['path']
        # if output.stderr_file:
        #     output_dict['stderr_file'] = output.stderr_file['path']

        output_dict['returncode'] = result.returncode

        exceptions = ['normal termination of xtb']
        if result.stderr:
            stderr_file = run_settings.stderr_file['path']
            Path(stderr_file).write_text(result.stderr)
            self.logger.debug('STDERR: %s', result.stderr) if any(
                e in result.stderr for e in exceptions
                ) else self.logger.error('STDERR: %s', result.stderr)
            output_dict['stderr'] = result.stderr

        if result.stdout:
            stdout_file = run_settings.stdout_file['path']
            Path(stdout_file).write_text(result.stdout)
            output_dict['stdout'] = stdout_file
        return replace(output, **output_dict)

    def submit(self, job=None, **kwargs):
        """Submit job."""
        if len(job.tasks) > 1:
            raise ValueError('Local scheduler only supports one task')
        rs = job.tasks[0]
        js = job.job_script['path']
        output = job.outputs[0]

        # maybe write the script to disk and run it with subprocess
        os.chmod(js, 0o755)

        # cmd = Path(js).read_text().split(' ')
        self.logger.info('Running command: %s\n',
                         Path(js).read_text().split('\n'))
        self.logger.info('Working directory: %s\n', rs.work_dir_local)
        run_opts = {'capture_output': True,
                    'text': True,
                    'cwd': str(rs.work_dir_local),
                    'env': rs.env,
                    'shell': False}
        # if ';' in cmd or '&&' in cmd:
        #     run_opts['shell'] = True
        #     cmd = ' '.join(cmd)
        result = subprocess.run(str(js), **run_opts)
        self.logger.debug('Result: %s\n', result)
        output = self.update_output(result=result,
                                    run_settings=rs,
                                    output=output)
        job.outputs = [output]
        if result.returncode == 0:
            job.status = 'COMPLETED'
        else:
            job.status = 'FAILED'
        job.finished = True
        return job

    def is_finished(self, job, *args, **kwargs) -> bool:
        """Check if job is finished."""
        success = ['COMPLETED']
        failed = ['FAILED']
        if job.status in failed:
            self.logger.error(f'Job {job.id} with id {job.db_id} in ' +
                              f'database {job.database} failed with status ' +
                              f'{job.status}')
        return job.status in success + failed

    def get_status(self, job=None, **kwargs) -> str:
        """Get status."""
        return job

    def cancel(self):
        """Cancel job."""
        self.logger.error('Cancel not implemented for local scheduler\n')

    def quick_return(self):
        """Quick return."""
        pass

    def fetch_results(self, jobs, *args, **kwargs):
        """Fetch results."""
        if isinstance(jobs, list):
            return [self.fetch_results(j) for j in jobs]
        return jobs

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
