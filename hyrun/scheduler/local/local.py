import os
import subprocess
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Optional, List, Union
from hytools.file import FileManager, File, get_file
from shlex import join, split
from hyset import RunSettings

from hytools.logger import LoggerDummy

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
        return {'scheduler_type': self.name}

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
            stdout_file = run_settings.stdout_file.path
            Path(stdout_file).write_text(result.stdout)
            output_dict['stdout'] = stdout_file
        return replace(output, **output_dict)
    
    def remove_comments_and_split_commands(self, s: str) -> list:
        """Remove comment lines and split the string into commands."""
        lines = s.split('\n')
        commands = []
        for line in lines:
            line = line.split('#', 1)[0].strip()  # Remove comments and strip whitespace
            if line:  # Ignore empty lines
                commands.append(line)
        return commands
    
    # def split_by_delimiters(
    #         self,
    #         commands: List[str],
    #         delimiters: List[str] = None
    #         ) -> List[List[str]]:
    #     """Split a list of commands by delimiters into different lists."""
       
    #     result = []
    #     current_list = []

    #     for item in commands:
    #         current_list.append(item)
    #         if item in delimiters:
    #             result.append(current_list)
    #             current_list = []

    #     if current_list:
    #         result.append(current_list)

    #     return result

    # def prepare_subprocess_input(
    #     self,
    #     pre_cmd: Optional[List[str]] = None,
    #     result_pre: Optional[subprocess.CompletedProcess] = None,
    #     stdin_file: Optional[Union[str, File, Path]] = None,
    # ) -> Optional[Union[bytes, str]]:
    #     """Get the input for the subprocess based on pre_cmd and result_pre."""
    #     if stdin_file is not None:
    #         self.logger.debug(f'STDIN FILE: {stdin_file}')
    #     if isinstance(stdin_file, File):
    #         return FileManager().read_file_local(stdin_file)
    #     elif isinstance(stdin_file, str):
    #         return stdin_file
    #     elif isinstance(stdin_file, Path):
    #         return stdin_file.read_text()
    #     if not pre_cmd:
    #         return None
    #     return result_pre.stdout if pre_cmd and pre_cmd[-1] == '|' else None

    def _gen_output(self, inp: RunSettings, result: Any) -> Dict[str, Any]:
        """Generate output."""
        logger = inp.logger or LoggerDummy
        output_dict: Dict[str, Any]
        files_to_parse = inp.files_to_parse
        for i, f in enumerate(files_to_parse):
            if isinstance(f, File):
                if hasattr(f, 'work_path_local'):
                    files_to_parse[i] = f.work_path_local  # type: ignore
        output_dict = {
            'files_to_parse': files_to_parse,
            'output_folder': (
                inp.work_dir_local if inp.output_folder else None
            ),
        }
        if inp.output_file:
            if hasattr(inp.output_file, 'work_path_local'):
                f = inp.output_file.work_path_local  # type: ignore
            else:
                f = inp.output_file
            output_dict.update({'output_file': f})  # type: ignore
        if result is None:
            return output_dict
        output_dict.update({'returncode': result.returncode})

        if result.returncode < 0:
            logger.warning(
                f'{inp.program} run failed, '
                + 'returncode: {result.returncode}'
            )
        if result.stderr:
            # stderr_file = inp.stderr_file.work_path_local  # type: ignore
            inp.stderr_file.content = result.stderr
            FileManager.write_file_local(inp.stderr_file)
            logger.debug('STDERR: %s', result.stderr)
            output_dict['stderr'] = result.stderr
        if result.stdout:
            # stdout_file = inp.stdout_file.work_path_local  # type: ignore
            # self.write_file_local(stdout_file, result.stdout)
            inp.stdout_file.content = result.stdout
            FileManager.write_file_local(inp.stdout_file)
            output_dict['stdout'] = inp.stdout_file
        return output_dict
    
    def submit(self, job=None, **kwargs):
        """Submit job."""
        cmds = self.remove_comments_and_split_commands(job.job_script.content)
        if len(cmds) != len(job.tasks):
            raise ValueError('Number of commands must match number of tasks')
        results = []
        for i, (t, cmd) in enumerate(zip(job.tasks, cmds)):
            run_opt={'input': None, 'env': t.env or os.environ, 'capture_output': True,
                     'text': True, 'cwd': str(t.work_dir_local),
                     'shell': False, 'check': True}
            run_opt.update(kwargs.get('run_opt', {}))
            if t.stdin_file:
                stdin = FileManager().read_file_local(t.stdin_file)
                run_opt['input'] = stdin
                self.logger.debug(f'STDIN INPUT: {stdin}')
            cmd = join(cmd) if run_opt['shell'] else split(cmd)
            print(cmd)

            self.logger.debug(f'RUNNING #{i}: {cmd}\n')
            self.logger.debug(f'WORKING DIRECTORY: {t.work_dir_local}\n')
            result = subprocess.run(
                cmd, **run_opt
            )
            self.logger.debug(f'RESULT: {result}\n')   
            results.append(self._gen_output(t, result))

        return

        print(results)
        okkokok
        # rs = job.tasks[0]
        # env = rs.env
        # wdir = rs.work_dir_local

        # results: List[subprocess.CompletedProcess] = []
        # for i, cmd in enumerate(cmds):
        #     prev_cmd = cmds[i - 1] if i > 0 else None

        #     self.logger.debug(f'RUNNING #{i}: {cmd}\n')
        #     input_subprocess = self.prepare_subprocess_input(
        #         pre_cmd=prev_cmd, result_pre=results[-1] if i > 0 else None,
        #         stdin_file=stdin_file if i == idx_final else None
        #     )
        #     if input_subprocess:
        #         logger.debug('STDIN INPUT: \n %s', input_subprocess)
        #     result = subprocess.run(
        #         self.prepare_cmd(cmd, run_opts['shell']),
        #         input=input_subprocess,
        #         **run_opts,
        #     )
        #     logger.debug('RESULT: \n %s', result)
        #     if result.returncode != 0:
        #         logger.error(f'RUNNING CMD {cmd} FAILED WITH STDERR\n ' +
        #                      f'{result.stderr}')
        #         if cmd[-1] in ['|', '&&']:
        #             raise ValueError('RUNNING CMD FAILED')
        #     results.append(result)
        #     if input_subprocess:
        #         logger.debug('RESULT PIPED TO NEXT CMD\n')

        # result = results[idx_final]vars

        print('økmmømkømkøm', cmds)
        lkmlmkmklmkl
        return
        pokokopkopk
        
        job_script = job.job_script

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
