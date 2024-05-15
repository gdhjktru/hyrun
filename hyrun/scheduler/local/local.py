from contextlib import nullcontext
from typing import Any, Optional, List
from hyrun.job import Output
from hytools.logger import LoggerDummy
from .conda import get_conda_launcher
from .docker import get_docker_launcher

class LocalScheduler:
    
    def __init__(self, **kwargs):
        self.logger = kwargs.get('logger', LoggerDummy())
        self.logger.debug('Local scheduler initialized\n')

    def run_ctx(self, arg: Optional[Any] = None):
        return nullcontext(arg)
    
    def get_launcher(self, run_settings):
        """Get launcher."""
        # allows conda in docker but not docker in conda
        return get_conda_launcher(run_settings.conda_env,
                                  [*get_docker_launcher(
                                      **run_settings.__dict__),
                                      *run_settings.launcher])

    def gen_cmd(self, run_settings):
        """Generate command."""
        launcher = self.get_launcher(run_settings)

        running_list: List[str] = [
            *launcher, run_settings.program, *run_settings.args
        ]
        return running_list
        
    
    def send_files(self, *args, **kwargs):
        pass

    def submit(self, run_settings):
        return 0
    
    def is_finished(self, status) -> bool:
        return True
    
    def get_status(self, job):
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
        print(run_settings.__dict__.keys())
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
