from hyrun.scheduler import get_scheduler as gs
from time import sleep
from typing import Generator, List
from hydb import Database, DatabaseDummy
from hytools.logger import LoggerDummy
from hytools.file import File
from copy import deepcopy
from dataclasses import replace
import numpy as np
from .array_job import ArrayJob
from hyrun.job import Job
from string import Template
from functools import wraps
from hyrun.decorators import list_exec, force_list
from contextlib import nullcontext, suppress
from pathlib import Path

class Runner:
    """Runner."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        
        a = ArrayJob(self.get_run_settings(*args), **kwargs)
        self.run_array, self.job_shape = a.run_settings, a.shape
        self.run_settings = self.run_array[0][0]
        self.logger = self.get_logger(**kwargs)

        self.wait_for_jobs_to_finish = self.run_settings.wait
        self.database = self.get_database(**kwargs)
        self.scheduler = self.get_scheduler(logger=self.logger, **kwargs)
        self.logger.debug("Runner initialized.")

    def get_database(self, **kwargs):
        """Get database."""
        return kwargs.get('database',
                          getattr(self.run_settings, 'database', None)) or \
                          DatabaseDummy()
        
    def get_logger(self, **kwargs):
        """Get logger."""
        return kwargs.get('logger',
                          getattr(self.run_settings, 'logger', None)) or \
                          LoggerDummy()
        
    def get_run_settings(self, *args):
        """Get run settings."""
        if len(args) > 1:
            raise ValueError("run() takes at most 1 positional argument, " +
                                "got {}".format(len(args)))
        return args[0]
    
    def get_scheduler(self, logger=None, **kwargs):
        """Get scheduler."""
        scheduler = kwargs.get('scheduler',
                               getattr(self.run_settings, 'scheduler', None))
        if scheduler is None:
            raise ValueError("Scheduler not specified in kwargs or " +
                             "run_settings")
        return gs(scheduler, logger=logger, **kwargs)
    
    def get_database(self, **kwargs):
        """Get database."""
        db = kwargs.get('database',
                        getattr(self.run_settings, 'database', None))
        return db or DatabaseDummy()

    def is_finished(self, status) -> bool:
        """Check if job is finished."""
        return self.scheduler.is_finished(status)
    
    @force_list
    def get_status(self, jobs) -> List[str]:
        """Get status."""
        return self.scheduler.get_status(jobs)
    
    def _increment_t(self, t, tmin=1, tmax=60 ) -> int:
        """Increment t."""
        return min(max(2*t, tmin), tmax)

    def _increment_and_sleep(self, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = self._increment_t(t)

    @force_list
    def wait(self, jobs, timeout=60) -> List[str]:
        """Wait for job to finish."""
        timeout = max([j.walltime for j in jobs]) or timeout
        incrementer = self._increment_and_sleep(1)
        statuses = [self.get_status(j) for j in jobs]
        for t in incrementer:
            if t >= timeout or self.is_finished(statuses):
                break
            statuses = [self.get_status(j) for j in jobs]
        return statuses
    


    @list_exec
    def write_file_local(self, file, overwrite=True):
        print(type(file))
        p = Path(file.folder) / file.name if file.folder is not None else file.work_path_local
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file))
        return str(p)

    @list_exec
    def replace_var_in_file_content(self, file):
        """Replace variables in file content."""
        with suppress(AttributeError):
            file.content = Template(file.content
                                    ).safe_substitute(**file.variables)
        return file

    
    def run_single_job(self, run_settings):
        

        # check if job has already finished
        if self.scheduler.check_finished(run_settings):
            print("Job already finished")

        job_script = self.scheduler.gen_job_script(run_settings)
        # job = Job(run_settings=run_settings, job_script=job_script)
        self.logger.debug(f"Job script: {job_script}")
        ff=[f for rs in run_settings for f in rs.files_to_write]  + [job_script]
        print('pijipj', ff) 
        local_files = self.write_file_local(
            [job_script] + [f for rs in run_settings for f in rs.files_to_write]    
        )    
        print(local_files)



        # # generate local files
        # file_local, file_remote
        # file_local.write()

        # Copy/send input
        self.scheduler.copy_files(run_settings, job_script)

        # Submit the job
        job_id = self.scheduler.submit(run_settings)
        job = self.scheduler.gen_job(job_id, run_settings)

        # Log and return job
        db_id = self.database.add(job)
        self.logger.debug(f"Job submitted: {job}")
        self.logger.debug(f"Database ID: {db_id}")
        return job
    
    def finish_single_job(self, job, status='FINISHED'):
        """Finnish single job."""
        self.logger.debug(f"Job status: {status}")
        # Update the status
        job = replace(job, status=status)
        # Fetch the results
        r = None
        try:
            r = self.scheduler.fetch_results(job)
        except Exception as e:
            self.logger.error(f"Error fetching results: {e}")
        else:
            # Update the database
            self.database.update(job.db_id, job)
        finally:
            # Teardown the scheduler
            self.scheduler.teardown(job)
        return r

    def fetch_results(self, jobs):
        """Fetch results."""
        if not isinstance(jobs, list):
            jobs = [jobs]
        return [self.finish_single_job(j) for j in jobs]

    
    def run(self, **kwargs):
        """Run."""
        self.logger.info(f"Running {len(self.run_array)} job(s).")
        with self.scheduler.run_ctx():
            jobs = [self.run_single_job(rs) for rs in self.run_array]
        if not self.wait_for_jobs_to_finish:
            return jobs
        # Wait for the job to finish
        status = self.wait(jobs)
        # Fetch the results
        return self.fetch_results(jobs)

