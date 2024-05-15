from hyrun.scheduler import get_scheduler as gs
from time import sleep
from typing import Generator, List
from hydb import Database, DatabaseDummy
from hytools.logger import LoggerDummy
from copy import deepcopy
from dataclasses import replace
import numpy as np
from .array_job import ArrayJob

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
    
    def get_status(self, jobs):
        """Get status."""
        if not isinstance(jobs, list):
            jobs = [jobs]
        return [self.scheduler.get_status(j) for j in jobs]
    
    def _increment_t(self, t, tmin=1, tmax=60 ) -> int:
        """Increment t."""
        return min(max(2*t, tmin), tmax)

    def _increment_and_sleep(self, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = self._increment_t(t)

    def wait(self, jobs, timeout=60) -> List[str]:
        """Wait for job to finish."""
        if not isinstance(jobs, list):
            jobs = [jobs]
        timeout = max([j.walltime for j in jobs]) or timeout
        incrementer = self._increment_and_sleep(1)
        statuses = [self.get_status(j) for j in jobs]
        for t in incrementer:
            if t >= timeout or self.is_finished(statuses):
                break
            statuses = [self.get_status(j) for j in jobs]
        return statuses
    
    def run_single_job(self, run_settings):
        
        # compute jobs sequentially
        if isinstance(run_settings, list):
            for rs in run_settings:
                return self.run_single_job(rs)
        # check if job has already finished
        if self.scheduler.check_finished(run_settings):
            print("Job already finished")

        # Generate input
        input = self.scheduler.gen_cmd(run_settings)
        print(input)
        åokoko
        run_settings = [replace(rss, **input) for rss in run_settings]

        # Copy/send input
        self.scheduler.send_files(run_settings)

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

