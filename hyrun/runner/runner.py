import hashlib
from contextlib import suppress
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from string import Template
from time import sleep
from typing import Dict, Generator, List

from hytools.file import File
from hytools.logger import LoggerDummy

from hyrun.decorators import force_list, list_exec
from hyrun.job import Job

from .array_job import gen_jobs


class Runner:
    """Runner."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = self.get_logger(*args, **kwargs)
        # # self.scheduler = self.get_scheduler(*args,
        # #                                     logger=self.logger,
        # #                                     **kwargs)
        # self.jobs = gen_jobs(*args, **kwargs)
        # # self.jobs = self.get_scheduler(self.jobs)

        # self.logger.debug(f'Job overview: {len(self.jobs)} jobs.')
        # for i, j in enumerate(self.jobs):
        #     self.logger.debug(f'   job {i} task(s): {len(j.tasks)} tasks.')

        # # self.schedu
        # # print(self.jobs, 'self.jobs')
        # # joijioji
        # # # array_job = ArrayJob(self.get_run_settings(*args),
        # # #                      logger=self.logger,
        # # #                      **kwargs)
        # # self.run_array = array_job.run_array
        # # self.schedulers = array_job.schedulers
        # # print(self.schedulers, 'scihef')
        # # poioi

        # # self.global_settings = self.run_array[0][0]
        # # self.wait_for_jobs_to_finish = self.global_settings.wait
        # # self.database = self.get_database(**kwargs)
        # # self.logger.debug(f'Run array: {len(self.run_array)} jobs.')
        # # for i, rs in enumerate(self.run_array):
        # #     self.logger.debug(f'   job {i} task(s): {len(rs)} tasks.')

    # def check_job_params(self, jobs) -> List[Job]:
    #     """Check job parameters."""
    #     return self.flatten_2d_list(
    #         [job.scheduler.check_job_params(job) for job in jobs])

    # def flatten_2d_list(self, list_):
    #     """Flatten a 2D list."""
    #     if any(isinstance(item, list) for item in list_):
    #         return [item for sublist in list_ for item in sublist]
    #     return list_

    # @list_exec
    # def get_scheduler(self, job):
    #     """Get scheduler."""
    #     return replace(job, scheduler=gs(job.scheduler, logger=self.logger))

    # def get_database(self, **kwargs):
    #     """Get database."""
    #     return kwargs.get('database',
    #                       getattr(self.global_settings,
    #                               'database', None)) or DatabaseDummy()

    def _get_attr(self, attr_name, *args, **kwargs):
        """Get attribute."""
        a = kwargs.get(attr_name, None)
        if a or not args:
            return a
        a = args[0][0] if isinstance(args[0], list) else args[0]
        return getattr(a, attr_name, None)

    def get_logger(self, *args, **kwargs):
        """Get logger."""
        return kwargs.get('logger',
                          self._get_attr('logger', *args, **kwargs)
                          or LoggerDummy())

    # def get_run_settings(self, *args):
    #     """Get run settings."""
    #     if len(args) > 1:
    #         raise ValueError('run() takes at most 1 positional argument, ' +
    #                          'got {}'.format(len(args)))
        # return args[0]

    # def get_scheduler(self, *args, logger=None, **kwargs):
    #     """Get scheduler."""
    #     scheduler = self._get_attr('scheduler', *args,  **kwargs)
    #     if scheduler is None:
    #         raise ValueError('Scheduler not specified in kwargs or ' +
    #                          'run_settings')
    #     return gs(scheduler, logger=logger, **kwargs)

    # def is_finished(self, status) -> bool:
    #     """Check if job is finished."""
    #     return self.scheduler.is_finished(status)

    def prepare_job_for_db(self, job):
        """Prepare job for database."""
        # purge_attrs = ['local_files', 'remote_files', 'run_settings']
        purge_attrs = []
        # tasks = self.prepare_tasks_for_db(job.tasks)
        tasks = job.tasks
        job_db = replace(job, tasks=tasks, db_id=None)
        for attr in purge_attrs:
            job_db.__dict__.pop(attr, None)
        return job_db

    def resolve_files(self, jobs) -> dict:
        """Resolve files."""
        files_to_transfer: Dict[str, List[str]] = {}
        for job in jobs:
            d = job.scheduler.resolve_files(job)
            for k, v in d.items():
                if k not in files_to_transfer:
                    files_to_transfer[k] = []
                files_to_transfer[k].extend(v)
        return files_to_transfer

    # def prepare_tasks_for_db(self, tasks):
    #     """Prepare tasks for database."""
    #     purge_attrs = ['cluster_settings', 'file_handler']
    #     return [self.prepare_job_for_db(t) for t in tasks]

    # def handle_db(self, job, operation):
    #     """Handle job in database based on operation."""
    #     if isinstance(job, list):
    #         return [self.handle_db(j, operation) for j in job]
    #     job_db = self.prepare_job_for_db(job)
    #     self.logger.debug(f'{operation} job to database')
    #     if operation == 'add':
    #         id = self.database.add(job_db)
    #         if id < 0:
    #             self.logger.error('Error adding job to database')
    #             return job
    #         return replace(job, db_id=id)
    #     elif operation == 'update':
    #         self.database.update(job.db_id, job_db)
    #         return job
    @list_exec
    def add_to_db(self, job):
        """Add job to database."""
        job_db = self.prepare_job_for_db(job)
        db = job_db.database
        db_id = db.add(job_db)
        if db_id < 0:
            self.logger.error('Error adding job to database')
            return job
        else:
            self.logger.info(f'Added job to database with id {db_id}')
            self.logger.debug(f'db entry: {db.get(db_id)}')
        return replace(job, db_id=db_id)

    @list_exec
    def update_db(self, job):
        """Update job in database."""
        job_db = self.prepare_job_for_db(job)
        job_db.database.update(job.db_id, job_db)
        return job

    def _increment_t(self, t, tmin=1, tmax=60) -> int:
        """Increment t."""
        return min(max(2*t, tmin), tmax)

    def _increment_and_sleep(self, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = self._increment_t(t)

    def is_finished(self, jobs) -> bool:
        """Check if job is finished."""
        return all(job.scheduler.is_finished(job) for job in jobs)

    @list_exec
    def get_status(self, job, connection=None):
        """Get status."""
        return job.scheduler.get_status(job, connection=connection)

    def _get_timeout(self, jobs):
        """Get timeout."""
        return max(sum([t.job_time.total_seconds()
                        if isinstance(t.job_time, timedelta)
                        else t.job_time for t in j.tasks]) for j in jobs)

    @force_list
    def wait(self, jobs, connection=None, timeout=None) -> list:
        """Wait for job to finish."""
        timeout = (timeout or self._get_timeout(jobs))
        if timeout <= 0:
            return self.get_status(jobs, connection=connection)

        incrementer = self._increment_and_sleep(1)
        self.logger.info(f'Waiting for jobs to finish. Timeout: {timeout} ' +
                         'seconds.')
        for t in incrementer:
            jobs = [j.scheduler.get_status(j, connection=connection)
                    for j in jobs]
            if t >= timeout or self.is_finished(jobs):
                break
        return jobs

    @list_exec
    def replace_var_in_file_content(self, file):
        """Replace variables in file content."""
        with suppress(AttributeError):
            file.content = Template(file.content
                                    ).safe_substitute(**file.variables)
        return file

    def gen_job_script(self, job: Job) -> Job:
        """Generate job script."""
        job_script_str = job.scheduler.gen_job_script(  # type: ignore
            job)
        job_hash = hashlib.sha256(job_script_str.encode()).hexdigest()
        job_script_name = (getattr(job.tasks[0],  # type: ignore
                                   'job_script_filename', None)
                           or f'job_script_{job_hash}.sh')
        job_script = File(name=job_script_name,
                          content=job_script_str,
                          handler=job.tasks[0].file_handler)  # type: ignore
        return replace(job, job_script=job_script,
                       hash=job_hash)
        # # print(job_script)
        # p = job_script.submit_path_local
        # # p = (getattr(job_script, 'submit_path_local', None)
        # #      or job_script.work_path_local)
        # # print('pjpjp', p)
        # # pojklopiiåpoåø
        # p.parent.mkdir(parents=True, exist_ok=True)
        # p.write_text(job_script.content)
        # p_remote = getattr(job_script, 'submit_path_remote', None) or p
        # file_transfer = job.files_to_transfer or FILE_TRANSFER
        # file_transfer['pre']['from'].append(str(p))
        # file_transfer['pre']['to'].append(str(p_remote))
        # return replace(job, job_script=str(p),
        #                job_hash=job_hash)

    @list_exec
    def prepare_jobs(self, job: Job) -> Job:
        """Prepare jobs."""
        job = self.gen_job_script(job)
        self.write_file_local(job.job_script, parent='submit_path_local')
        files_to_write = [f for t in job.tasks
                          for f in t.files_to_write]
        self.write_file_local(files_to_write, parent='work_path_local')
        return job

    @list_exec
    def write_file_local(self, file, parent='work_path_local', overwrite=True):
        """Write file locally."""
        p = (Path(file.folder) / file.name if file.folder is not None
             else getattr(file, parent))
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file).content)
        return str(p)

    @list_exec
    def check_finished(self, job: Job):
        """Check if all tasks of a job have finished."""
        return [job.scheduler.check_finished(t)  # type: ignore
                for t in job.tasks]

    @list_exec
    def submit_jobs(self, job: Job, ctx):
        """Submit jobs."""
        return job.scheduler.submit(job, ctx)  # type: ignore

    @list_exec
    def fetch_results(self, job, *args, **kwargs):
        """Fetch results."""
        return job.scheduler.fetch_results(job, *args, **kwargs)

    def return_jobs(self, jobs):
        """Return jobs."""
        return jobs[0] if len(jobs) == 1 else jobs

    def get_schedulers(self, jobs):
        """Get schedulers."""
        return list(set([j.scheduler for j in jobs]))

    def run(self, *args, **kwargs):
        """Run."""
        # filter jobs that are not finished
        jobs = kwargs.get('jobs', gen_jobs(*args, **kwargs))
        jobs = [j for j in jobs
                if not any(self.check_finished(j))]
        rs = jobs[0].tasks[0]  # type: ignore
        jobs = [self.prepare_jobs(j) for j in jobs]
        jobs = self.add_to_db(jobs)
        if rs.dry_run:
            self.logger.warning('Dry run found in first task of first job, '
                                'exiting...')
            return jobs
        # potential exit point

        schedulers = list(set([j.scheduler for j in jobs]))
        jobs_scheduler = {}
        for s in schedulers:
            jobs_scheduler[s] = [i for i, j in enumerate(jobs)
                                 if j.scheduler == s]

        self.logger.info(f'Running {len(jobs)} job(s).')
        for i, j in enumerate(jobs):
            self.logger.info(f'   job {i} ({j.hash}) task(s): ' +
                             f'{len(j.tasks)} tasks')

# update database with job
        self.update_db(jobs)

        for scheduler in schedulers:
            setattr(scheduler, 'logger', self.logger)
            self.logger.debug('Using scheduler: ' +
                              f'{scheduler.name}')  # type: ignore
            with scheduler.run_ctx() as ctx:  # tpye: ignore

                self.logger.debug(f'Context manager opened, ctx: {ctx}')
                jobs_to_run = [jobs[i] for i in jobs_scheduler[scheduler]]
                files_to_transfer = self.resolve_files(jobs_to_run)
                self.logger.debug(f'Files to transfer: {files_to_transfer}')
                transfer = scheduler.transfer_files(files_to_transfer, ctx)
                for r in transfer:
                    if getattr(r, 'ok', True):
                        self.logger.info(getattr(r, 'stdout', ''))
                    else:
                        self.logger.error(getattr(r, 'stderr', ''))

                try:
                    jobs_to_run = self.submit_jobs(jobs_to_run, ctx)
                except Exception as e:
                    self.logger.error(f'Error submitting jobs: {e}')
                    continue
                # wait
                self.update_db(jobs_to_run)
                if (sum(sum([t.wait for t in j.tasks])
                        for j in jobs_to_run) == 0
                        or not kwargs.get('wait', True)):
                    for i, j in enumerate(jobs_scheduler[scheduler]):
                        jobs[j] = jobs_to_run[i]
                    self.logger.info('Not waiting for jobs to finish...')
                    continue

                jobs_to_run = self.wait(jobs_to_run, connection=ctx)
                jobs_to_run = self.update_db(jobs_to_run)

            # # Fetch the results
                transfer = self.fetch_results(jobs_to_run, connection=ctx)
                for r in transfer:
                    if getattr(r, 'ok', True):
                        self.logger.info(getattr(r, 'stdout', ''))
                    else:
                        self.logger.error(getattr(r, 'stderr', ''))

                scheduler.teardown(jobs_to_run, ctx)
            for i, j in enumerate(jobs_scheduler[scheduler]):
                jobs[j] = jobs_to_run[i]
        self.update_db(jobs)
        return self.return_jobs(jobs)
