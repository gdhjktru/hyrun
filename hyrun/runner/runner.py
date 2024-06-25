import hashlib
from contextlib import suppress
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from socket import gethostname
from string import Template
from time import sleep
from typing import Generator, Optional

from hydb import DatabaseDummy
from hytools.logger import Logger, LoggerDummy

from hyrun.decorators import list_exec
from hyrun.job import Job, loop_update_jobs

from .gen_jobs import gen_jobs

try:
    from hytools.file import File
except ImportError:
    from hyset import File


class Runner:
    """Runner."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = self.get_logger(*args, **kwargs)
        # self.parser = kwargs.get('parser', kwargs.get('method', None))

    def _get_attr(self, attr_name, *args, **kwargs):
        """Get attribute."""
        a = kwargs.get(attr_name, None)
        if a or not args:
            return a
        a = args[0][0] if isinstance(args[0], list) else args[0]
        return getattr(a, attr_name, None)

    @loop_update_jobs
    def get_jobs_from_db(self, *args, job=None, database=None, **kwargs):
        """Resolve db_id."""
        db = database or DatabaseDummy()
        db_id = job.db_id
        entry = db.get(key='db_id', value=db_id)[0]
        job = db.dict_to_obj(entry)
        job.db_id = db_id
        return {'job': job, 'scheduler': job.scheduler, 'db_id': db_id,
                'database': db}

    def get_logger(self, *args, **kwargs):
        """Get logger."""
        return kwargs.get('logger',
                          self._get_attr('logger', *args, **kwargs)
                          or LoggerDummy())

    def get_files_to_transfer(self,
                              jobs,
                              files_to_transfer=None,
                              job_keys=None,
                              task_keys=None):
        """Get files to transfer."""
        files_to_transfer = files_to_transfer or []
        job_keys = job_keys or []
        task_keys = task_keys or []
        for j in jobs.values():
            for k in job_keys:
                _list = getattr(j['job'], k, [])
                _list = [_list] if not isinstance(_list, list) else _list
                for f in _list:
                    files_to_transfer.append(f)
            for t in j['job'].tasks:
                for k in task_keys:
                    ll = getattr(t, k, [])
                    ll = [ll] if not isinstance(ll, list) else ll
                    for f in ll:
                        files_to_transfer.append(f)
        return [f for f in files_to_transfer if f is not None]

    # @loop_update_jobs
    # def get_from_db(self, *args, job=None, database=None, **kwargs):
    #     """Get job from database."""
    #     job_db = database.get(job.db_id)
    #     return replace(job, **job_db)

    @loop_update_jobs
    def add_to_db(self, *args, job=None, database=None, **kwargs):
        """Add job to database."""
        if job.db_id is not None:
            self.logger.debug('Job already in database, updating...')
            return self.update_db(job=job, database=database)
        db_id = database.add(job)
        if db_id < 0:
            self.logger.error('Error adding job to database')
            return job
        else:
            self.logger.info(f'Added job to database with id {db_id}')
            self.logger.debug(f'db entry: {database.get(db_id)}')
        return replace(job, db_id=db_id)

    @loop_update_jobs
    def update_db(self, *args, job=None, database=None, **kwargs):
        """Update job in database."""
        db_id = getattr(job, 'db_id', None)
        if db_id is None:
            raise ValueError('db_id must be set to update job in database')
        database.update(db_id, job)
        self.logger.info(f'Updated job in database with id {db_id}')
        self.logger.debug(f'db entry: {database.get(db_id)}')
        return job

    def _increment_t(self, t, tmin=1, tmax=60) -> int:
        """Increment t."""
        return min(max(2 * t, tmin), tmax)

    def _increment_and_sleep(self, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = self._increment_t(t)

    def is_finished(self, jobs) -> bool:
        """Check if job is finished."""
        return all(job['scheduler'].is_finished(job['job'])
                   for job in jobs.values())

    @loop_update_jobs
    def get_status_run(self,
                       *args,
                       job=None,
                       scheduler=None,
                       connection=None, **kwargs) -> dict:
        """Get status."""
        return scheduler.get_status(job,  # type: ignore
                                    connection=connection)

    def _get_timeout(self, jobs):
        """Get timeout."""
        return max(sum([t.job_time.total_seconds()
                        if isinstance(t.job_time, timedelta)
                        else t.job_time for t in j['job'].tasks])
                   for j in jobs.values())

    def wait(self, jobs, connection=None, timeout=None) -> dict:
        """Wait for job to finish."""
        timeout = (timeout or self._get_timeout(jobs))
        if timeout <= 0:
            return self.get_status_run(jobs, connection=connection)

        incrementer = self._increment_and_sleep(1)
        self.logger.info(f'Waiting for jobs to finish. Timeout: {timeout} ' +
                         'seconds.')
        for t in incrementer:
            jobs = self.get_status_run(jobs, connection=connection)
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

    def gen_job_script(self, job=None, scheduler=None):
        """Generate job script."""
        job_script_str = scheduler.gen_job_script(job)  # type: ignore
        job_hash = hashlib.sha256(job_script_str.encode()).hexdigest()
        job_script_name = (getattr(job.tasks[0],  # type: ignore
                                   'job_script_filename', None)
                           or f'job_script_{job_hash}.sh')
        job_script = File(name=job_script_name,
                          content=job_script_str)  # type: ignore
        return replace(job,
                       job_script=job_script,
                       hash=job_hash)

    def check_types(self, obj):
        """Check types."""
        # move to hydb?
        self.logger.debug(f'Checking types in {obj} ({type(obj)}) ' +
                          'for storage in database')
        ignore_types = (Logger, timedelta, Path)
        std_types = (str, int, float, bool)
        seqs = (list, tuple, set)
        if isinstance(obj, ignore_types + std_types) or not obj:
            return
        if isinstance(obj, seqs):
            for e in obj:
                self.check_types(e)
        else:
            d = obj if isinstance(obj, dict) else obj.__dict__
            for k, v in d.items():
                if v is None or isinstance(v, ignore_types + std_types):
                    continue
                elif isinstance(v, dict) or isinstance(v, seqs):
                    self.check_types(v)
                else:
                    self.logger.error(f'Found non-standard type {type(v)} ' +
                                      f'in task: {k}')

    def resolve_file_names(self, files, parent, host):
        """Resolve file names."""
        return [self.resolve_file_name(f, parent=parent, host=host)
                for f in files]

    @loop_update_jobs
    def prepare_jobs(self,
                     *args,
                     job=None,
                     scheduler=None,
                     **kwargs) -> Job:
        """Prepare jobs."""
        parent = getattr(job.tasks[0], 'submit_dir_local', None)
        host = gethostname()
        job = self.gen_job_script(job=job, scheduler=scheduler)
        self.write_file_local(job.job_script, parent=parent, host=host)

        job.job_script = self.resolve_file_name(
            job.job_script, parent=parent, host=host)
        for t in job.tasks:
            parent = getattr(t, 'work_dir_local', None)
            host = gethostname()
            if not all([parent, host]):
                self.logger.error('work_dir_local and host must be set')
                continue

            self.write_file_local(t.files_to_write, parent=parent, host=host)
            t.files_to_write = self.resolve_file_names(
                t.files_to_write, parent, host)
            t.files_for_restarting = self.resolve_file_names(
                t.files_for_restarting, parent, host)
            t.files_to_parse = self.resolve_file_names(
                t.files_to_parse, parent, host)

            host = getattr(
                t,
                'host',
                None) or getattr(
                t,
                'connection',
                {}).get(
                'host',
                gethostname())
            parent = getattr(t, 'work_dir_remote', None)
            if host == gethostname():
                parent = getattr(t, 'work_dir_local', None)

            for f in ['output_file', 'stdout_file', 'stderr_file']:
                if hasattr(t, f):
                    setattr(t, f, self.resolve_file_name(getattr(t, f),
                                                         parent=parent,
                                                         host=host))
            for f in ['file_handler', 'cluster_settings', 'logger']:
                if hasattr(t, f):
                    setattr(t, f, None)
            self.check_types(t)

        for o in job.outputs:
            parent = getattr(o, 'work_dir_local', None)
            host = gethostname()
            for f in ['output_file', 'stdout_file', 'stderr_file']:
                if hasattr(o, f):
                    setattr(o, f, self.resolve_file_name(getattr(o, f),
                                                         parent=parent,
                                                         host=host))

            o.files_to_parse = self.resolve_file_names(
                o.files_to_parse, parent, host)
        return job

    def resolve_file_name(self,
                          file,
                          parent: Optional[str] = None,
                          host: Optional[str] = None) -> dict:
        """Resolve file name."""
        if not file:
            return {}
        parent = parent or str(Path('.'))
        file = (Path(file.folder) / file.name
                if file.folder is not None
                else Path(parent) / file.name)
        return {'path': str(file), 'host': host or gethostname()}

    @list_exec
    def write_file_local(self, file, overwrite=True, **kwargs):
        """Write file locally."""
        p = Path(self.resolve_file_name(file, **kwargs).get('path'))
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file).content)
        return str(p)

    def check_finished_jobs(self, jobs: dict) -> dict:
        """Check if all jobs are finished."""
        for j in jobs.values():
            job = j['job']
            job.finished = all(
                self.check_finished_single(t)
                for t in job.tasks)  # type: ignore
        return jobs

    def check_finished_single(self, run_settings) -> bool:
        """Check if output file exists and return True if it does."""
        # NOTE: for remotely parsed add parsed file !!
        work_dir_local = getattr(run_settings, 'work_dir_local', Path('.'))
        files_to_check = []
        for n in ['output_file', 'stdout_file', 'stderr_file']:
            f = getattr(run_settings, n, None)
            if f is None:
                continue
            if hasattr(f, 'name'):
                files_to_check.append(Path(work_dir_local) / Path(f.name).name)
            else:
                files_to_check.append(Path(work_dir_local) / Path(f['path']))

        files_to_check = [f for f in files_to_check
                          if f.name not in ['stdout.out', 'stderr.out']]

        if any(f.exists() for f in files_to_check if f is not None):
            self.logger.debug(f'(one of) output file(s) {files_to_check} ' +
                              'exists')
        else:
            return False

        force_recompute = run_settings.force_recompute
        self.logger.info('force_recompute is %s, will %srecompute\n',
                         'set' if force_recompute else 'not set',
                         '' if force_recompute else 'not ')
        return not force_recompute

    @loop_update_jobs
    def submit_jobs(self, *args, job=None, scheduler=None, **kwargs):
        """Submit jobs."""
        return scheduler.submit(job=job, **kwargs)  # type: ignore

    # def get_outputs(self, jobs):
    #     """Get outputs."""
    #     for j in jobs.values():
    #         job = j['job']
    #         job.outputs = []
    #         for t in job.tasks:
    #             output = Output().from_dict(t.__dict__)
    #             print('output get', output)
    #             for f in ['output_file', 'stdout_file', 'stderr_file']:
    #                 if hasattr(t, f):
    #                     setattr(output, f, getattr(t, f)['local']['path'])
    #             print('output mod', output)

    #             job.outputs.append(output)
    #     return jobs

    def get_remote_folder(self, jobs) -> str:
        """Get remote folder."""
        remote_folders = [getattr(t, 'submit_dir_remote', None)
                          or getattr(t, 'work_dir_remote', None)
                          for j in jobs.values()
                          for t in j['job'].tasks
                          if getattr(t, 'submit_dir_remote', None)
                          or getattr(t, 'work_dir_remote', None)]
        if len(list(set(remote_folders))) > 1:
            raise ValueError('All tasks in a job must have the same ' +
                             'remote folder')
        return str(remote_folders[0]) if len(remote_folders) == 1 else ''

    def transfer_from_cluster(self,
                              jobs=None,
                              scheduler=None,
                              connection=None,
                              **kwargs):
        """Transfer files from cluster."""
        transfer_all = kwargs.get('transfer_all', True)
        output_files = ['output_file', 'stdout_file', 'stderr_file']
        dfiles_to_transfer = {}
        for j in jobs.values():
            finished = all(self.check_finished_single(t)
                           for t in j['job'].tasks)
            if not finished:
                self.logger.warning(f'Job {j["job"].id} not finished, not ' +
                                    'transferring files')
                continue
            for t, o in zip(j['job'].tasks, j['job'].outputs):
                wdir_local = str(getattr(t, 'work_dir_local'))
                if wdir_local not in dfiles_to_transfer:
                    dfiles_to_transfer[wdir_local] = []

                if transfer_all:
                    for f in output_files:
                        output_file = getattr(o, f, None)
                        remote_dir = Path(output_file['path']).parent
                        alls = str(Path(remote_dir) / '*')
                        dfiles_to_transfer[wdir_local].append(
                            {'path': alls,
                             'host': getattr(connection,
                                             'host',
                                             gethostname())})
                        continue

                for f in output_files:
                    remote_file = getattr(o, f, None)
                    if remote_file:
                        dfiles_to_transfer[wdir_local].append(
                            remote_file)

        for wdir_local, files in dfiles_to_transfer.items():
            transfer = scheduler.transfer_files(  # type: ignore
                files_to_transfer=files,
                connection=connection,
                folder=wdir_local)
            log_method = (self.logger.info
                          if getattr(transfer, 'ok', True)
                          else self.logger.error)
            log_method(getattr(transfer, 'stdout'
                               if getattr(transfer, 'ok', True)
                               else 'stderr', ''))

    def transfer_to_cluster(self,
                            jobs=None,
                            scheduler=None,
                            connection=None):
        """Transfer files to cluster."""
        files_to_transfer = [j['job'].job_script for j in jobs.values()]

        files_to_transfer.extend([f for j in jobs.values()
                                  for t in j['job'].tasks
                                  for f in t.files_to_write])

        remote_folder = self.get_remote_folder(jobs)

        loc = f'{getattr(connection, "host", gethostname())}'
        loc += f':{remote_folder}' if remote_folder else ''
        self.logger.debug(f'Files to transfer: {files_to_transfer} to {loc}')

        transfer = scheduler.transfer_files(  # type: ignore
            files_to_transfer=files_to_transfer,
            connection=connection,
            folder=remote_folder)
        log_method = (self.logger.info
                      if getattr(transfer, 'ok', True)
                      else self.logger.error)
        log_method(getattr(transfer, 'stdout'
                           if getattr(transfer, 'ok', True) else 'stderr', ''))

    def run(self, *args, **kwargs):
        """Run."""
        jobs = gen_jobs(*args, logger=self.logger, **kwargs)
        jobs = self.check_finished_jobs(jobs)
        self.logger.debug('The following jobs are finished: ' +
                          ', '.join([str(i) for i, j in jobs.items()
                                     if j['job'].finished]))
        # remove jobs that are finished
        jobs = {i: j for i, j in jobs.items() if not j['job'].finished}

        jobs = self.prepare_jobs(jobs)
        jobs = self.add_to_db(jobs)

        if any(t.dry_run for j in jobs.values() for t in j['job'].tasks):
            self.logger.warning('Performing dry run, exiting...')
            return [j['job'] for j in jobs.values()]

        schedulers = list(set([j['scheduler'] for j in jobs.values()]))

        self.logger.info(f'Running {len(jobs)} job(s).')
        for i, j in jobs.items():
            self.logger.info(f'   job {i} ({j["scheduler"].name}) task(s): ' +
                             f'{len(j["job"].tasks)} tasks')
        wait = any([t.wait for j in jobs.values() for t in j['job'].tasks])
        self.logger.info(f'Waiting for jobs to finish: {wait}')

        if len(schedulers) > 1 and wait:
            self.logger.warning('Multiple schedulers found, ' +
                                'waiting for all jobs of each scheduler' +
                                'to finish consecutively...')
        #     .... first submit all then wait for all then fetch all
        for scheduler in schedulers:
            setattr(scheduler, 'logger', self.logger)
            self.logger.debug('Using scheduler: ' +
                              f'{scheduler.name}')  # type: ignore
            with scheduler.run_ctx() as ctx:  # tpye: ignore

                self.logger.debug(f'Context manager opened, ctx: {ctx}')
                jobs_to_run = {i: j for i, j in jobs.items()
                               if j['scheduler'] == scheduler}

                self.transfer_to_cluster(jobs=jobs_to_run,
                                         scheduler=scheduler,
                                         connection=ctx)

                remote_folder = self.get_remote_folder(jobs_to_run)

                try:
                    jobs_to_run = self.submit_jobs(
                        jobs_to_run, connection=ctx,
                        remote_folder=remote_folder)
                except Exception as e:
                    self.logger.error(f'Error submitting jobs: {e}')
                    raise e
                    # continue
                # wait
                jobs_to_run = self.update_db(jobs_to_run)
                if not wait:
                    self.logger.info('Not waiting for jobs to finish...')
                    continue

                jobs_to_run = self.wait(jobs_to_run, connection=ctx)
                jobs_to_run = self.update_db(jobs_to_run)

                self.transfer_from_cluster(jobs=jobs_to_run,
                                           scheduler=scheduler,
                                           connection=ctx, **kwargs)

                scheduler.teardown(jobs_to_run, ctx)
        if not wait:
            return [j['job'].db_id for j in jobs.values()]
        return [j['job'].outputs for j in jobs.values()]

    def get_status(self, *args, fetch_results=False, **kwargs):
        """Get status/Fetch results."""
        jobs = gen_jobs(*args, logger=self.logger, **kwargs)
        jobs = self.check_finished_jobs(jobs)
        self.logger.debug('The following jobs are finished: ' +
                          ', '.join([str(i) for i, j in jobs.items()
                                     if j['job'].finished]))
        # remove jobs that are finished
        jobs = {i: j for i, j in jobs.items() if not j['job'].finished}

        schedulers = list(set([j['scheduler'] for j in jobs.values()]))

        self.logger.info(f'Checking {len(jobs)} job(s).')

        for scheduler in schedulers:
            setattr(scheduler, 'logger', self.logger)
            self.logger.debug('Using scheduler: ' +
                              f'{scheduler.name}')  # type: ignore
            with scheduler.run_ctx() as ctx:  # tpye: ignore

                self.logger.debug(f'Context manager opened, ctx: {ctx}')
                jobs_to_run = {i: j for i, j in jobs.items()
                               if j['scheduler'] == scheduler}

                jobs_to_run = self.get_status_run(
                    jobs_to_run, connection=ctx)

                if not fetch_results:
                    scheduler.teardown(jobs_to_run, ctx)
                    continue

                self.transfer_from_cluster(jobs=jobs_to_run,
                                           scheduler=scheduler,
                                           connection=ctx, **kwargs)

                scheduler.teardown(jobs_to_run, ctx)
        return [j['job'].outputs
                if fetch_results
                else j['job'].status for j in jobs.values()]

    def fetch_results(self, *args, **kwargs):
        """Fetch results."""
        return self.get_status(*args, fetch_results=True, **kwargs)
