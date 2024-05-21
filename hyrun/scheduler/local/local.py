import subprocess
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional

from hytools.file import File
from hytools.logger import LoggerDummy

from hyrun.decorators import list_exec

from ..abc import Scheduler
from .conda import get_conda_launcher
from .docker import get_docker_launcher


class LocalScheduler(Scheduler):
    """Local scheduler."""

    def __init__(self, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', LoggerDummy())
        self.logger.debug('Local scheduler initialized\n')
        self.default_data_path = 'data_path_local'

    def run_ctx(self, arg: Optional[Any] = None):
        """Return context manager."""
        return nullcontext(arg)

    def get_launcher(self, run_settings):
        """Get launcher."""
        # allows conda in docker but not docker in conda
        return get_conda_launcher(run_settings.conda_env,
                                  [*get_docker_launcher(
                                      **run_settings.__dict__),
                                      *run_settings.launcher])

    def gen_job_script(self, run_settings):
        """Generate command."""
        cmd = ''

        if run_settings.pre_cmd:
            pre_command = run_settings.pre_cmd
            if isinstance(pre_command, str):
                cmd += pre_command + '\n'
            else:
                cmd += ' && '.join(pre_command) + '\n'
            
        cmd += 'echo "job start ;"'
        cmd += ' '.join([*self.get_launcher(run_settings),
                        run_settings.program,
                        *run_settings.args])
        cmd += '; echo "job end"'
        cmd += '\n'

        if run_settings.post_cmd:
            post_command = run_settings.post_cmd
            if isinstance(post_command, str):
                cmd += post_command + '\n'
            else:
                cmd += ' && '.join(post_command) + '\n'
        job_script_name = 'job_script_' + run_settings.get_hash(cmd) + '.sh'
        job_script = File(name=job_script_name,
                          content=cmd,
                          handler=run_settings.file_handler)
        return job_script

    def copy_files(self, local_files: List[str], remote_files: List[str], ctx):
        """Copy files."""
        pass

    def gen_output(self, result, run_settings) -> dict:
        """Generate output."""
        output_dict: Dict[str, Any]
        files_to_parse = run_settings.files_to_parse
        for i, f in enumerate(files_to_parse):
            if isinstance(f, File):
                if hasattr(f, 'work_path_local'):
                    files_to_parse[i] = f.work_path_local  # type: ignore
        output_dict = {
            'files_to_parse': files_to_parse,
            'output_folder': (run_settings.work_dir_local
                              if run_settings.output_folder
                              else None),
        }

        if run_settings.output_file:
            if hasattr(run_settings.output_file, 'work_path_local'):
                f = run_settings.output_file.work_path_local  # type: ignore
            else:
                f = run_settings.output_file
            output_dict.update({'output_file': f})  # type: ignore
        if result is None:
            return output_dict

        output_dict.update({'returncode': result.returncode})

        exceptions = ['normal termination of xtb']
        if result.stderr:
            stderr_file = (
                run_settings.stderr_file.work_path_local  # type: ignore
            )
            Path(stderr_file).write_text(result.stderr)
            self.logger.debug('STDERR: %s', result.stderr) if any(
                e in result.stderr for e in exceptions
            ) else self.logger.error('STDERR: %s', result.stderr)
            output_dict['stderr'] = result.stderr

        if result.stdout:
            stdout_file = (
                run_settings.stdout_file.work_path_local  # type: ignore
            )
            Path(stdout_file).write_text(result.stdout)
            output_dict['stdout'] = stdout_file
        return output_dict

    def submit(self, job):
        """Submit job."""
        # warning might return a list of lists
        cmds = job.job_script.split('\n')
        if len(cmds) > 1:
            self.logger.warning('Multiple commands in job script, '
                                'will run sequentially\n')
        rs = job.run_settings
        results = []
        for cmd in cmds:
            self.logger.debug(f'Running command: {cmd}')
            result = subprocess.run(
                cmd.split(),  # type: ignore
                capture_output=True,
                text=True,
                cwd=rs.work_dir_local,
                env=rs.env,
                shell=False)
            # try to separate the pre and post commands
            if not all(c in result.stdout for c in ['job start', 'job end']):
                continue
            job = replace(job, **self.gen_output(result, rs))
            job.job_finished = True
            results.append(job)
        # job.finished = True
        # job.files_to_parse = [file for rs in results for file
        # in rs.files_to_parse]
        # job.output_file = [r.output_file for r in results]
        # job.stdout = [r.stdout for r in results]
        # job.stderr = [r.stderr for r in results]
        # job.returncode = sum([r.returncode for r in results])
        return results if len(results) > 1 else results[0]

    def is_finished(self, status) -> bool:
        """Check if job is finished."""
        return True

    @list_exec
    def get_status(self, job) -> str:
        """Get status."""
        return 'finished'

    def cancel(self):
        """Cancel job."""
        self.logger.error('Cancel not implemented for local scheduler\n')

    def quick_return(self):
        """Quick return."""
        pass

    def fetch_results(self, jobs):
        """Fetch results."""
        return jobs

    @list_exec
    def teardown(self, jobs):
        """Teardown."""
        for rs in jobs:
            for f in rs.scratch_dir_local + rs.files_to_remove:
                f.unlink()

    def check_finished(self, run_settings) -> bool:
        """Check if output file exists and return True if it does."""
        # compute jobs sequentially
        if isinstance(run_settings, list):
            return all(self.check_finished(rs) for rs in run_settings)
        files_to_check = [getattr(f, 'work_path_local')
                          for f in [run_settings.output_file,
                                    run_settings.stdout_file,
                                    run_settings.stderr_file]
                          if getattr(f, 'work_path_local', None) is not None]
        files_to_check = [f for f in files_to_check
                          if f.name not in ['stdout.out', 'stderr.out']]
        if not any(f.exists() for f in files_to_check if f is not None):
            return False

        self.logger.debug(f'(one of) output file(s) {files_to_check} exists')

        force_recompute = run_settings.force_recompute
        self.logger.info('force_recompute is %s, will %srecompute\n',
                         'set' if force_recompute else 'not set',
                         '' if force_recompute else 'not ')
        return not force_recompute
