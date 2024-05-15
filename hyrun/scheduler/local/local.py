from contextlib import nullcontext
from typing import Any, Optional

class LocalScheduler:
    
    def __init__(self, **kwargs):
        pass

    def run_ctx(self, arg: Optional[Any] = None):
        return nullcontext(arg)
    
    def generate_input(self, s):
        return {}
    
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