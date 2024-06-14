import hashlib
from contextlib import suppress
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from string import Template
from time import sleep
from typing import Dict, Generator, List, Optional, Callable
from socket import gethostname
from hytools.file import File
from hytools.logger import LoggerDummy

from hyrun.decorators import force_list, list_exec
from hyrun.job import Job, loop_update_jobs

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
        # todo: resolve objs
        tasks = job.tasks
        job_db = replace(job, tasks=tasks, db_id=None)
        for attr in purge_attrs:
            job_db.__dict__.pop(attr, None)
        return job_db

    def resolve_files(self,
                      jobs: dict, 
                      files_to_transfer: Optional[dict] = None) -> dict:
        """Resolve files."""
        files_to_transfer = files_to_transfer or {}
        for j in jobs:
            job = j['job']
            scheduler = j['scheduler']
            d = scheduler.resolve_files(job)
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
    @loop_update_jobs
    def add_to_db(self, *args,  job=None, database=None, **kwargs):
        """Add job to database."""
        job_db = self.prepare_job_for_db(job)
        db_id = database.add(job_db)
        if db_id < 0:
            self.logger.error('Error adding job to database')
            return job
        else:
            self.logger.info(f'Added job to database with id {db_id}')
            self.logger.debug(f'db entry: {database.get(db_id)}')
        return replace(job, db_id=db_id)

    @loop_update_jobs
    def update_db(self, job=None, database=None, **kwargs):
        """Update job in database."""
        job_db = self.prepare_job_for_db(job)
        database.update(job.db_id, job_db)
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
        return all(job['scheduler'].is_finished(job['job']) for job in jobs)

    @loop_update_jobs
    def get_status(self,
                   *args,
                   job=None,
                   scheduler=None,
                   connection=None, **kwargs) -> dict:
        """Get status."""
        return scheduler.get_status(job,
                                    connection=connection)

    def _get_timeout(self, jobs):
        """Get timeout."""
        return max(sum([t.job_time.total_seconds()
                        if isinstance(t.job_time, timedelta)
                        else t.job_time for t in j['job'].tasks]) for j in jobs)

    @force_list
    def wait(self, jobs, connection=None, timeout=None) -> list:
        """Wait for job to finish."""
        timeout = (timeout or self._get_timeout(jobs))
        if timeout <= 0:
            return self.get_status(jobs,
                                   connection=connection)

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

    def gen_job_script(self, job=None, scheduler=None) -> dict:
        """Generate job script."""
        job_script_str = scheduler.gen_job_script(job)  # type: ignore
        job_hash = hashlib.sha256(job_script_str.encode()).hexdigest()
        job_script_name = (getattr(job.tasks[0],  # type: ignore
                                   'job_script_filename', None)
                           or f'job_script_{job_hash}.sh')
        job_script = File(name=job_script_name,
                          content=job_script_str,
                          handler=job.tasks[0].file_handler)  # type: ignore
        return replace(job, job_script=job_script, hash=job_hash)
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

    def resolve_objs_in_tasks(self, job):
        """Resolve objects in tasks."""
        std_types = (str, int, float, bool)
        seqs = (list, tuple, set)
        print('checking resolve_objs_in_tasks')
        for t in job.tasks:
            for k, v in t.__dict__.items():
                if not isinstance(v, std_types):
                    self.logger.warning(f'Found non-standard type in task: {k}')
                    print('resolve', k, type(v), v)
                if isinstance(v, seqs):
                    for i, e in enumerate(v):
                        if not isinstance(e, std_types):
                            self.logger.warning(f'Found non-standard type in task: {k}/{e}')
                            print('resolve', k, i, type(e), e)
        print('checking done')
        kklløklklo
        return job

    @loop_update_jobs
    def prepare_jobs(self, *args, job=None, scheduler=None, **kwargs) -> Job:
        """Prepare jobs."""
        job = self.gen_job_script(job=job, scheduler=scheduler)
        host_remote = [getattr(t, 'host', None) for t in job.tasks]
        print(list(set(host_remote)))
        settings = {'parent_local': 'work_path_local',
                        'parent_remote': 'work_path_remote',
                        'host_remote': host_remote[0]}
        job.job_script = self.write_file_local(job.job_script,
                                               parent_local='submit_path_local',
                                               parent_remote='submit_path_remote')
        for t in job.tasks:
            
            t.files_to_write = self.write_file_local(t.files_to_write,
                                                     **settings)
            
            t.files_for_restarting = [self.resolve_file_name(f, 
                                                             **settings) for f in
                                      t.files_for_restarting]
            t.files_to_parse = [self.resolve_file_name(f, **settings) for f in
                                      t.files_to_parse]
            for f in ['output_file', 'stdout_file', 'stderr_file']:
                if hasattr(t, f):
                    setattr(t, f, self.resolve_file_name(getattr(t, f),
                                                         file_name=f, 
                                                         **settings))
            for f in ['file_handler', 'cluster_settings']:
                if hasattr(t, f):
                    setattr(t, f, None)

        job = self.resolve_objs_in_tasks(job)

        return job
    
    def resolve_file_name(self,
                          file,
                          parent_local: Optional[str] = 'work_path_local',
                          parent_remote: Optional[str] = 'work_path_remote',
                          file_name: Optional[str] = None,
                          host_local: Optional[str] = None,
                          host_remote: Optional[str] = None,) -> dict:
        """Resolve file name."""
        file_local = (Path(file.folder) / file.name
                          if file.folder is not None
                          else getattr(file, parent_local))
        
        file_name = file_name or file_local.name
        host_local = host_local or gethostname()
        file = {'name': str(file_name),
                'local': {'host': str(host_local),
                          'path': str(file_local)}}
        if not host_remote:
            return file
    
        file_remote = (Path(file.folder) / file.name
                        if file.folder is not None
                        else getattr(file, parent_remote))
        file['remote'] = {'host': str(host_remote), 
                          'path': str(file_remote)}
        return file


    @list_exec
    def write_file_local(self, file,  overwrite=True, **kwargs):
        """Write file locally."""
        p = Path(self.resolve_file_name(
            file, **kwargs).get('local',{}).get('path',''))
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file).content)
        return str(p)


    def check_finished(self, job=None, scheduler=None):
        """Check if all tasks of a job have finished."""
        return [scheduler.check_finished(t)  # type: ignore
                for t in job.tasks]

    @loop_update_jobs
    def submit_jobs(self, *args, job=None, scheduler=None, context=None, **kwargs):
        """Submit jobs."""
        return scheduler.submit(job, context)  # type: ignore

    @loop_update_jobs
    def fetch_results(self,
                      *args,
                      job=None,
                      scheduler:Optional[Callable]=None, **kwargs):
        """Fetch results."""
        return scheduler.fetch_results(job, *args, **kwargs)

    def return_jobs(self, jobs):
        """Return jobs."""
        return jobs[0] if len(jobs) == 1 else jobs
    
    def check_all_finished(self, jobs: dict):
        """Check if all jobs are finished."""
        for j in jobs.values():
            job, scheduler = j['job'], j['scheduler']
            job.finished = all(self.check_finished(job, scheduler))
        return jobs

    # def get_schedulers(self, jobs):
    #     """Get schedulers."""
    #     return list(set([j.scheduler for j in jobs]))

    def run(self, *args, **kwargs):
        """Run."""
        jobs = gen_jobs(*args, logger=self.logger, **kwargs)
        jobs = self.check_all_finished(jobs)
  
        # "global" run settings                
        rs = jobs[0]['job'].tasks[0]  # type: ignore
        jobs = self.prepare_jobs(jobs)
        print(jobs)
        kklklklkl
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

        if len(schedulers) > 1 :
            print('branch out')
        #     .... first submit all then wait for all then fetch all
        # database save_minimal
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

                # what if wait is not set

                jobs_to_run = self.wait(jobs_to_run, connection=ctx)
                jobs_to_run = self.update_db(jobs_to_run)

            # # Fetch the results
                # check status first
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
