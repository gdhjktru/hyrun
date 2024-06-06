import hashlib
from contextlib import suppress
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from string import Template
from time import sleep
from typing import Generator, List, Union

from hytools.file import File
from hytools.logger import LoggerDummy

from hyrun.decorators import force_list, list_exec
from hyrun.job import Job

from .array_job import gen_jobs

FILE_TRANSFER = {'pre': {'from': [], 'to': []},
                 'post': {'from': [], 'to': []}}

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

    @list_exec
    def get_status(self, job) -> Union[str, List[str]]:
        """Get status."""
        return job.scheduler.get_status(job)  # type: ignore

    def prepare_job_for_db(self, job):
        """Prepare job for database."""
        # purge_attrs = ['local_files', 'remote_files', 'run_settings']
        purge_attrs = []
        tasks = self.prepare_tasks_for_db(job.tasks)
        job_db = replace(job, tasks=tasks, db_id=None)
        for attr in purge_attrs:
            job_db.__dict__.pop(attr, None)
        return job_db

    @list_exec
    def prepare_tasks_for_db(self, tasks):
        """Prepare tasks for database."""
        purge_attrs = ['cluster_settings', 'file_handler']
        return [self.prepare_job_for_db(t) for t in tasks]

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
    # @list_exec
    # def add_to_db(self, job):
    #     """Add job to database."""
    #     return self.handle_db(job, 'add')

    # @list_exec
    # def update_db(self, job):
    #     """Update job in database."""
    #     return self.handle_db(job, 'update')

    # def copy_files(self, jobs, ctx):
    #     """Copy files."""
    #     return self.scheduler.copy_files(jobs, ctx)

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

    def is_finished(self, statuses) -> bool:
        """Check if job is finished."""
        return True
        # return all(self.scheduler.is_finished(s) for s in statuses)

    @force_list
    def wait(self, jobs, timeout=60) -> list:
        """Wait for job to finish."""
        timeout = max([j.job_time for j in jobs]) or timeout
        incrementer = self._increment_and_sleep(1)
        statuses = [self.get_status(j) for j in jobs]
        for t in incrementer:
            if t >= timeout or self.is_finished(statuses):
                break
            statuses = [self.get_status(j) for j in jobs]
        for j, s in zip(jobs, statuses):
            j.status = s
        return jobs

    # @list_exec
    # def resolve_files(self, job):
    #     """Resolve files."""
    #    for t in job.tasks:
    #         t.files_to_write = [self._resolve_file(f, parent='work_path_local') for f in t.files_to_write]

    # def _resolve_file(self, file, parent='work_path_local'):
    #     p = (Path(file.folder) / file.name if file.folder is not None
    #          else getattr(file, parent))
    #     return p



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
        job_script_name = (getattr(job.tasks[0],
                                  'job_script_filename', None)
                                  or f'job_script_{job_hash}.sh')
        job_script = File(name=job_script_name,
                          content=job_script_str,
                          handler=job.tasks[0].file_handler)  # type: ignore
        # print(job_script)
        p = job_script.submit_path_local
        # p = (getattr(job_script, 'submit_path_local', None)
        #      or job_script.work_path_local)
        # print('pjpjp', p)
        # pojklopiiåpoåø
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(job_script.content)
        p_remote = getattr(job_script, 'submit_path_remote', None) or p
        file_transfer = job.files_to_transfer or FILE_TRANSFER
        file_transfer['pre']['from'].append(str(p))
        file_transfer['pre']['to'].append(str(p_remote))
        return replace(job, job_script=str(p),
                       job_hash=job_hash,
                       files_to_transfer=file_transfer)

    @list_exec
    def prepare_jobs(self, job: Job) -> Job:
        """Prepare jobs."""
        job = self.gen_job_script(job)
        job = self.resolve_files(job)
        print(job)
        lklklkk
        file_list = [f for t in job.tasks
                        for f in t.files_to_write] + [job_script]
        job.local_files = self.write_file_local(file_list)
        job.remote_files = [str(f.work_path_remote)
                            for f in job.local_files
                            if hasattr(f, 'work_path_remote')]
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


    def resolve_files(self, job) -> Job:
        """Resolve files."""
        mapping = {'files_to_write': 'work_path_local'}
        files_to_transfer = job.files_to_transfer or FILE_TRANSFER
        for t in job.tasks:
            # for k,v in t.__dict__.items():
            #     if 'file' in k:
            #         print(k)
            # mlkmkmkmklmk
            for name in ['files_to_write', 'files_to_send']:
                list_ = getattr(t, name, [])
                for i, f in enumerate(list_):
                    parent_local = mapping.get(name, 'work_path_local')
                    parent_remote = parent_local.replace('local', 'remote')
                    p = self._resolve_file(f, parent=parent_local)
                    if 'files_to_write' in name:
                        self.write_file_local(f, parent=parent_local)
                    list_[i] = p

                    if name == 'files_to_transfer':
                        files_to_transfer['pre']['from'].append(p)
                        files_to_transfer['pre']['to'].append(getattr(f,
                                                                  parent_remote))
            for f in ['output_file', 'stdout_file', 'stderr_file']:
                if getattr(t, f, None) is not None:
                    p = self._resolve_file(getattr(t, f), parent='work_path_local')
                    p_remote = self._resolve_file(getattr(t, f), parent='work_path_remote')
                    files_to_transfer['post']['from'].append(str(p_remote))
                    files_to_transfer['post']['to'].append(str(p))
                    setattr(t, f, p)

        job.files_to_transfer = files_to_transfer
        return job


    def _resolve_file(self, file, parent='work_path_local'):
        p = (Path(file.folder) / file.name if file.folder is not None
             else getattr(file, parent, None))
        return p


    @list_exec
    def check_finished(self, job: Job):
        """Check if all tasks of a job have finished."""
        return [job.scheduler.check_finished(t)  # type: ignore
                for t in job.tasks]

    def submit_jobs(self, job: Job):
        """Submit jobs."""
        return job.scheduler.submit(job)  # type: ignore

    # def finish_single_job(self, job, status='FINISHED'):
    #     """Finnish single job."""
    #     self.logger.debug(f'Job status: {status}')
    #     # Update the status
    #     job = replace(job, status=status)
    #     # Fetch the results
    #     r = None
    #     try:
    #         r = self.scheduler.fetch_results(job)
    #     except Exception as e:
    #         self.logger.error(f'Error fetching results: {e}')
    #     else:
    #         # Update the database
    #         self.database.update(job.db_id, job)
    #     finally:
    #         # Teardown the scheduler
    #         self.scheduler.teardown(job)
    #     return r

    def fetch_results(self, job):
        """Fetch results."""
        return job.scheduler.fetch_results(job)

    def _return_jobs(self, jobs):
        print('returning', type(jobs))
        if not isinstance(jobs, list):
            return jobs
        if len(jobs) == 1:
            return self._return_jobs(jobs[0])
        if all(len(j) == 1 for j in jobs):
            return [j[0] for j in jobs]

    def get_schedulers(self, jobs):
        """Get schedulers."""
        return list(set([j.scheduler for j in jobs]))

    def run(self, *args, **kwargs):
        """Run."""
        # filter jobs that are not finished
        jobs = [j for j in gen_jobs(*args, **kwargs)
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

        self.logger.info(f'Running {len(jobs)} job(s).')
        for i, j in enumerate(jobs):
            self.logger.info(f'   job {i} ({j.job_hash}) task(s): {len(j.tasks)} tasks')




        for scheduler in schedulers:
            self.logger.debug('Using scheduler: ' +
                              f'{scheduler.name}')  # type: ignore
            with scheduler.run_ctx() as ctx:  # tpye: ignore
                self.logger.debug(f'Context manager opened, ctx: {ctx}')


        #     self.copy_files(jobs, ctx)
        #     jobs = self.submit_jobs(jobs)
        #     jobs = self.update_db(jobs)
        # # if not self.wait_for_jobs_to_finish:
        # #     return jobs
        # # Wait for the job to finish
        # jobs = self.wait(jobs)

        # # Fetch the results
        # jobs = self.fetch_results(jobs)
        # self.copy_files(jobs, ctx)
        # # Teardown
        # # self.scheduler.teardown(jobs)
        # return self._return_jobs(jobs)
