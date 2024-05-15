from contextlib import nullcontext
from typing import Any, Optional, List
from hyrun.job import Output
from hytools.logger import LoggerDummy
from hytools.file import File
from .conda import get_conda_launcher
from .docker import get_docker_launcher
from pathlib import Path
from hyrun.decorators import list_exec, force_list


class LocalScheduler:
    
    def __init__(self, **kwargs):
        self.logger = kwargs.get('logger', LoggerDummy())
        self.logger.debug('Local scheduler initialized\n')
        self.default_data_path = 'data_path_local'

    def run_ctx(self, arg: Optional[Any] = None):
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
        if isinstance(run_settings, list):
            return [self.gen_job_script(rs) for rs in run_settings]
        launcher = self.get_launcher(run_settings)

        running_list: List[str] = [
            *launcher, run_settings.program, *run_settings.args
        ]
        running_str = ' '.join(running_list)
        job_script_name = ('job_script_' + 
                           run_settings.get_hash(running_str) + '.sh')
        job_script = File(name=job_script_name,
                            content=running_str)
        return job_script
        #         job_name = self._gen_job_name_bundle(jobs)
        # jobs = [replace(job, job_name=job_name) for job in jobs]
        # job_script_name = 'job_script_' + job_name + '.sh'
        # job_script = jobs[0].run_settings.file_handler.file(
        #     name=job_script_name,
        #     content=self._gen_job_script_bundle(jobs))
        # return running_list


    
    
    def copy_files(self, *args, **kwargs):
        #1. create directories
        #2. resolve paths
        #3. write files
        pass

    def submit(self, run_settings):
        return 0
    
    def is_finished(self, status) -> bool:
        return True
    
    @list_exec
    def get_status(self, job) -> str:
        return 'finished'   
    
    def cancel(self):   
        pass

    def quick_return(self):
        pass

    def fetch_results(self):
        pass

    def teardown(self):
        pass

    def gen_job(self, job_id, run_settings):
        return job_id
    

    def check_finished(self, run_settings) -> bool:
        """Check if output file exists and return if it does."""
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
