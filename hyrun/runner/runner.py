from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from string import Template
from time import sleep
from typing import Generator, List

from hydb import DatabaseDummy
from hytools.logger import LoggerDummy

from hyrun.decorators import force_list, list_exec
from hyrun.job import Job
from hyrun.scheduler import get_scheduler as gs

from .array_job import ArrayJob


class Runner:
    """Runner."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = self.get_logger(*args, **kwargs)
        self.scheduler = self.get_scheduler(*args,
                                            logger=self.logger,
                                            **kwargs)

        self.run_array = ArrayJob(self.get_run_settings(*args),
                                  logger=self.logger,
                                  scheduler=self.scheduler,
                                  **kwargs).run_settings
        self.global_settings = self.run_array[0][0]
        self.wait_for_jobs_to_finish = self.global_settings.wait
        self.database = self.get_database(**kwargs)
        self.logger.debug('Runner initialized.')
        # flatten run_array if LocalScheduler
        if self.scheduler.__class__.__name__ == 'LocalScheduler':
            self.run_array = [item for job in self.run_array for item in job]

    def get_database(self, **kwargs):
        """Get database."""
        return kwargs.get('database',
                          getattr(self.global_settings,
                                  'database', None)) or DatabaseDummy()

    def _get_attr(self, attr_name, *args, **kwargs):
        """Get attribute."""
        a = kwargs.get(attr_name, None)
        if a or not args:
            return a
        a = args[0][0] if isinstance(args[0], list) else args[0]
        return getattr(a, attr_name, None)

    def get_logger(self, *args, **kwargs):
        """Get logger."""
        if not args:
            return kwargs.get('logger', LoggerDummy())
        return self._get_attr('logger', *args, **kwargs) or LoggerDummy()

    def get_run_settings(self, *args):
        """Get run settings."""
        if len(args) > 1:
            raise ValueError('run() takes at most 1 positional argument, ' +
                             'got {}'.format(len(args)))
        return args[0]

    def get_scheduler(self, *args, logger=None, **kwargs):
        """Get scheduler."""
        scheduler = self._get_attr('scheduler', *args,  **kwargs)
        if scheduler is None:
            raise ValueError('Scheduler not specified in kwargs or ' +
                             'run_settings')
        return gs(scheduler, logger=logger, **kwargs)

    def is_finished(self, status) -> bool:
        """Check if job is finished."""
        return self.scheduler.is_finished(status)

    @force_list
    def get_status(self, jobs) -> List[str]:
        """Get status."""
        return self.scheduler.get_status(jobs)

    @list_exec
    def add_to_db(self, job):
        """Add job to database."""
        if isinstance(job, list):
            return [self.add_to_db(j) for j in job]
        purge_attrs = ['local_files', 'remote_files', 'run_settings']
        job_db = replace(job, db_id=None)
        for attr in purge_attrs:
            job_db.__dict__.pop(attr, None)
        self.logger.debug(f'adding job {job} to database {self.database}')
        id = self.database.add(job_db)
        return replace(job, db_id=id)

    @list_exec
    def copy_files(self, job, ctx):
        """Copy files."""
        return self.scheduler.copy_files(job.local_files,
                                         job.remote_files,
                                         ctx)

    def _increment_t(self, t, tmin=1, tmax=60) -> int:
        """Increment t."""
        return min(max(2*t, tmin), tmax)

    def _increment_and_sleep(self, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = self._increment_t(t)

    # def flatten_arbitrary_nested_list(self, ll):
    #     """Flatten a nested list."""
    #     return [item for sublist in ll
    #             for item in (self.flatten_arbitrary_nested_list(sublist)
    #                          if isinstance(sublist, list)
    #                          else [sublist])]

    @force_list
    def wait(self, jobs, timeout=60) -> list:
        """Wait for job to finish."""
        # flatten jobs
        # jobs = self.flatten_arbitrary_nested_list(jobs)
        timeout = max([j.walltime for j in jobs]) or timeout
        incrementer = self._increment_and_sleep(1)
        statuses = [self.get_status(j) for j in jobs]
        for t in incrementer:
            if t >= timeout or self.is_finished(statuses):
                break
            statuses = [self.get_status(j) for j in jobs]
        for j, s in zip(jobs, statuses):
            j.status = s
        return jobs

    @list_exec
    def write_file_local(self, file, overwrite=True):
        """Write file locally."""
        p = (Path(file.folder) / file.name if file.folder is not None
             else file.work_path_local)
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file).content)
        return str(p)

    @list_exec
    def replace_var_in_file_content(self, file):
        """Replace variables in file content."""
        with suppress(AttributeError):
            file.content = Template(file.content
                                    ).safe_substitute(**file.variables)
        return file

    @list_exec
    def prepare_jobs(self, job: Job):
        """Prepare jobs."""
        job_script = self.scheduler.gen_job_script(job.run_settings)
        try:
            file_list = [f for rs in job.run_settings
                         for f in rs.files_to_write] + [job_script]
        except TypeError:
            file_list = [f for f in job.run_settings.files_to_write] \
                + [job_script]
        job.local_files = self.write_file_local(file_list)
        job.remote_files = [str(f.work_path_remote)
                            for f in job.local_files
                            if hasattr(f, 'work_path_remote')]
        job.job_script = job_script.content
        return job

    @list_exec
    def check_finished(self, run_settings):
        """Check if job has finished."""
        return self.scheduler.check_finished(run_settings)

    @list_exec
    def submit_jobs(self, job: Job):
        """Submit jobs."""
        return self.scheduler.submit(job)

    def finish_single_job(self, job, status='FINISHED'):
        """Finnish single job."""
        self.logger.debug(f'Job status: {status}')
        # Update the status
        job = replace(job, status=status)
        # Fetch the results
        r = None
        try:
            r = self.scheduler.fetch_results(job)
        except Exception as e:
            self.logger.error(f'Error fetching results: {e}')
        else:
            # Update the database
            self.database.update(job.db_id, job)
        finally:
            # Teardown the scheduler
            self.scheduler.teardown(job)
        return r

    @force_list
    def fetch_results(self, jobs):
        """Fetch results."""
        return [self.finish_single_job(j) for j in jobs]

    def run(self, **kwargs):
        """Run."""
        # filter jobs that are not finished
        jobs = [Job(run_settings=rs, scheduler=rs.scheduler)
                for rs, b in zip(self.run_array,
                                 self.check_finished(self.run_array)) if not b]
        self.logger.info(f'Running {len(jobs)} job(s).')
        with self.scheduler.run_ctx() as ctx:
            jobs = self.prepare_jobs(jobs)
            if self.global_settings.dry_run:
                return jobs
            self.copy_files(jobs, ctx)
            jobs = self.submit_jobs(jobs)
            jobs = self.add_to_db(jobs)
        if not self.wait_for_jobs_to_finish:
            return jobs
        # Wait for the job to finish
        jobs = self.wait(jobs)
        # Fetch the results
        return self.fetch_results(jobs)
