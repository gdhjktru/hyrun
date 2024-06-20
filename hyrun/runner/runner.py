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
from hytools.logger import LoggerDummy, Logger
import inspect
import subprocess
from hyrun.decorators import force_list, list_exec
from hyrun.job import Job, loop_update_jobs, Output

from .array_job import gen_jobs


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

    def get_logger(self, *args, **kwargs):
        """Get logger."""
        return kwargs.get('logger',
                          self._get_attr('logger', *args, **kwargs)
                          or LoggerDummy())

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
    
    def get_files_to_transfer(self,
                              jobs,
                              files_to_transfer=None,
                              job_keys = None,
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
                
            # d = scheduler.get_files_to_transfer(job)
            # for k, v in d.items():
            #     if k not in files_to_transfer:
            #         files_to_transfer[k] = []
            #     files_to_transfer[k].extend(v)
        # remove Nones from list
        return [f for f in files_to_transfer if f is not None]


    # def resolve_files(self,
    #                   jobs: dict, 
    #                   files_to_transfer: Optional[dict] = None) -> dict:
    #     """Resolve files."""
    #     files_to_transfer = files_to_transfer or {}
    #     for j in jobs.values():
    #         job = j['job']
    #         scheduler = j['scheduler']
    #         d = scheduler.resolve_files(job)
    #         for k, v in d.items():
    #             if k not in files_to_transfer:
    #                 files_to_transfer[k] = []
    #             files_to_transfer[k].extend(v)
    #     return files_to_transfer


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
    def get_from_db(self, *args, job=None, database=None, **kwargs):
        """Get job from database."""
        job_db = database.get(job.db_id)
        return replace(job, **job_db)

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
        return all(job['scheduler'].is_finished(job['job'])
                   for job in jobs.values())

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
                        else t.job_time for t in j['job'].tasks])
                        for j in jobs.values())

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
            jobs = self.get_status(jobs, connection=connection)
            # statuses = [j['scheduler'].get_status(j['job'], connection=connection)
            #             for j in jobs.values()]
            # for i, j in jobs.items():
            #     j['job'].status = statuses[i]
            # jobs = {i: j for i, j in jobs.items()}
            
            
            # [j['scheduler'].get_status(j['job'], connection=connection)
            #         for i, j in jobs)]
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
                          content=job_script_str)  # type: ignore
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
        for t in job.tasks:
            self.check_types(t)
        # for t in job.tasks:

        #     for k, v in t.__dict__.items():
        #         if not isinstance(v, std_types):
        #             self.logger.warning(f'Found non-standard type in task: {k}')
        #             print('resolve', k, type(v), v)
        #         if isinstance(v, seqs):
        #             for i, e in enumerate(v):
        #                 if not isinstance(e, std_types):
        #                     self.logger.warning(f'Found non-standard type in task: {k}/{e}')
        #                     print('resolve', k, i, type(e), e)
        return job
    
    def check_types(self, obj):
        """Check types."""
        self.logger.debug(f'Checking types in {obj} of type {type(obj)} for storage in database')
        ignore_types= (Logger, timedelta, Path)
        std_types = (str, int, float, bool)
        seqs = (list, tuple, set)
        if isinstance(obj, ignore_types + std_types):
            return
        if not obj:
            return  
        if isinstance(obj, seqs):
            for e in obj:
                return self.check_types(e)
    
        d = obj if isinstance(obj, dict) else obj.__dict__
        for k, v in d.items():
            if v is None or isinstance(v, ignore_types + std_types):
                continue
            if isinstance(v, dict):
                return self.check_types(v)             
            if isinstance(v, seqs):
                for e in v:
                    return self.check_types(e)
            self.logger.error(f'Found non-standard type {type(v)} in task: {k}')



    @loop_update_jobs
    def prepare_jobs(self, *args, job=None, scheduler=None, **kwargs) -> Job:
        """Prepare jobs."""
        job = self.gen_job_script(job=job, scheduler=scheduler)
        # hosts = [getattr(t, 'host', None)
        #                or getattr(t, 'connection', {}).get('host', None)
        #                for t in job.tasks]
        # wdirs = [getattr(t, 'work_dir_local', None)
        #          for t in job.tasks if hasattr(t, 'work_dir_local')]
        # sdirs = [getattr(t, 'submit_dir_local', None)
        #             for t in job.tasks if hasattr(t, 'submit_dir_local')]
        parent= getattr(job.tasks[0], 'submit_dir_local', None)
        host = gethostname()
        self.write_file_local(job.job_script, parent= parent, host=host)
        job.job_script = self.resolve_file_name(job.job_script,parent= parent, host=host)
        for i, t in enumerate(job.tasks):
            parent = getattr(t, 'work_dir_local', None)
            host =  gethostname()
            if not all([parent, host]):
                self.logger.error('work_dir_local and host must be set')
                continue

            self.write_file_local(t.files_to_write, parent=parent, host=host)
            t.files_to_write = [self.resolve_file_name(f, parent=parent, host=host) for f in
                                      t.files_to_write]
            
    
            t.files_for_restarting = [self.resolve_file_name(f, 
                                                             parent=parent, host=host) for f in
                                      t.files_for_restarting]
            t.files_to_parse = [self.resolve_file_name(f, parent=parent, host=host) for f in
                                      t.files_to_parse]
            
            parent = getattr(t, 'work_dir_remote', None)
            host = getattr(t, 'host', None) or getattr(t, 'connection', {}).get('host', gethostname())

            for f in ['output_file', 'stdout_file', 'stderr_file']:
                if hasattr(t, f):
                    setattr(t, f, self.resolve_file_name(getattr(t, f),
                                                         file_name=f, 
                                                         parent=parent, host=host))
            for f in ['file_handler', 'cluster_settings']:
                if hasattr(t, f):
                      setattr(t, f, None)


       
        job = self.resolve_objs_in_tasks(job)

        return job
    
    def resolve_file_name(self,
                          file,
                          parent: Optional[str] = '',
                          file_name: Optional[str] = None,
                          host: Optional[str] = None) -> dict:
        """Resolve file name."""
        file = (Path(file.folder) / file.name
                          if file.folder is not None
                          else Path(parent) / file.name)
        return {'path': str(file), 'host': host or gethostname()}
        # file_name = file_name or file_local.name
        # host_local = host_local or gethostname()
        # dfile = {'name': str(file_name),
        #         'local': {'host': str(host_local),
        #                   'path': str(file_local)}}
        # if not host_remote:
        #     return dfile
    
        # file_remote = (Path(file.folder) / file.name
        #                 if file.folder is not None
        #                 else getattr(file, parent_remote))
        # dfile['remote'] = {'host': str(host_remote), 
        #                   'path': str(file_remote)}
        # print('ijilwjeflwef', dfile)
        # return dfile

    @list_exec
    def write_file_local(self, file,  overwrite=True, **kwargs):
        """Write file locally."""
        p = Path(self.resolve_file_name(file, **kwargs).get('path'))
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file).content)
        return str(p)


    def check_finished(self, job=None, **kwargs):
        """Check if all tasks of a job have finished."""
        return [self.check_finished_single(t)  # type: ignore
                for t in job.tasks]
    
    def check_finished_single(self, run_settings) -> bool:
        """Check if output file exists and return True if it does."""
        work_dir_local = getattr(run_settings, 'work_dir_local', Path('.'))
        files_to_check = [Path(work_dir_local)/Path(f.name).name
                          for f in [run_settings.output_file,
                                    run_settings.stdout_file,
                                    run_settings.stderr_file]]
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
    def submit_jobs(self, *args, job=None, scheduler=None, context=None, **kwargs):
        """Submit jobs."""
        return scheduler.submit(job, context, **kwargs)  # type: ignore

    @loop_update_jobs
    def fetch_results(self,
                      *args,
                      job=None,
                      scheduler:Optional[Callable]=None, **kwargs):
        """Fetch results."""
        # copy also job_script  to work_dir_remote/local
        if not scheduler.is_finished(job):
            self.logger.error(f'Job {job.id} not finished, cannot fetch results')
            return []
        return scheduler.fetch_results(job, *args, **kwargs)

    def return_jobs(self, jobs):
        """Return jobs."""
        return jobs[0] if len(jobs) == 1 else jobs
    
    def check_all_finished(self, jobs: dict):
        """Check if all jobs are finished."""
        for j in jobs.values():
            job = j['job']
            job.finished = all(self.check_finished(job))
        return jobs
    


    def get_outputs(self, jobs):
        """Get outputs."""
        for j in jobs.values():
            job = j['job']
            job.outputs = []
            for t in job.tasks:
                output = Output().from_dict(t.__dict__)
                print('output get', output)
                for f in ['output_file', 'stdout_file', 'stderr_file']:
                    if hasattr(t, f):
                        setattr(output, f, getattr(t, f)['local']['path'])
                print('output mod', output)

                job.outputs.append(output)

                    
            okok
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
        jobs = self.add_to_db(jobs)

        if rs.dry_run:
            self.logger.warning('Dry run found in first task of first job, '
                                'exiting...')
            return jobs

        schedulers = list(set([j['scheduler'] for j in jobs.values()]))
       
        self.logger.info(f'Running {len(jobs)} job(s).')
        for i, j in jobs.items():
            self.logger.info(f'   job {i} ({j["job"].hash}) task(s): ' +
                             f'{len(j["job"].tasks)} tasks')
        wait = any([t.wait for j in jobs.values() for t in j['job'].tasks])
        self.logger.info(f'Waiting for jobs to finish: {wait}')
# update database with job
        # self.update_db(jobs)

        if len(schedulers) > 1 and wait:
            self.logger.warning('Multiple schedulers found, ' +
                                'waiting for all jobs of each scheduler' +
                                'to finish consecutively...')
        #     .... first submit all then wait for all then fetch all
        # database save_minimal
        for scheduler in schedulers:
            setattr(scheduler, 'logger', self.logger)
            self.logger.debug('Using scheduler: ' +
                              f'{scheduler.name}')  # type: ignore
            with scheduler.run_ctx() as ctx:  # tpye: ignore

                self.logger.debug(f'Context manager opened, ctx: {ctx}')
                jobs_to_run = {i: j for i, j in jobs.items()
                               if j['scheduler'] == scheduler}
                
                files_to_transfer = [j['job'].job_script
                                     for j in jobs_to_run.values()]
                
                files_to_transfer.extend([f
                                          for j in jobs_to_run.values()
                                          for t in j['job'].tasks
                                          for f in t.files_to_write])

                remote_folder = [getattr(t, 'submit_dir_remote', None) or getattr(t, 'work_dir_remote', None)
                                 for j in jobs_to_run.values() for t in j['job'].tasks]
                remote_folder = list(set(remote_folder))

                if len(remote_folder) > 1:
                    self.logger.error('Multiple remote folders found, ' +
                                      'cannot continue')
                
                self.logger.debug(f'Files to transfer: {files_to_transfer}'
                                  + f' to {getattr(ctx, "host", gethostname())}:{remote_folder[0]}')

                transfer = scheduler.transfer_files(files_to_transfer=files_to_transfer,
                                                    connection=ctx, 
                                                    folder=remote_folder[0])
                log_method = self.logger.info if getattr(transfer, 'ok', True) else self.logger.error
                log_method(getattr(transfer, 'stdout' if getattr(transfer, 'ok', True) else 'stderr', ''))

                try:
                    jobs_to_run = self.submit_jobs(jobs_to_run, ctx,remote_folder=remote_folder[0])
                except Exception as e:
                    self.logger.error(f'Error submitting jobs: {e}')
                    continue
                # wait
                self.update_db(jobs_to_run)
                if not wait:
                    self.logger.info('Not waiting for jobs to finish...')
                    continue

                # what if wait is not set

                jobs_to_run = self.wait(jobs_to_run, connection=ctx)
                jobs_to_run = self.update_db(jobs_to_run)

            # # Fetch the results
                print('REMEMBER; EVERY JOB task needs anoutput oobject')
                # check status first
                # first resolve the files below
                
                remote_wdirs = []
                for job in jobs_to_run.values():
                    files_to_transfer = []
                    job_id = job['job'].id
                    d = job['job'].tasks[0].work_dir_remote
                    d = str(d).replace('job_id', str(job_id))
                    remote_wdirs.append(d)
                    d_local = job['job'].tasks[0].work_dir_local
                    job['job'].outputs = []
                    for t in job['job'].tasks:
                        output = Output().from_dict(t.__dict__) 
                        if not output:
                            raise ValueError('could not create output object')
                        job['job'].outputs.append(output)
                        for f in ['output_file', 'stdout_file', 'stderr_file']:
                            remote_file = getattr(t, f, None)
                            if remote_file:
                                remote_file['path'] = Path(d) / Path(remote_file['path']).name
                                remote_file['host'] = getattr(ctx, 'host', gethostname())
                                files_to_transfer.append(remote_file)
                    transfer = scheduler.transfer_files(files_to_transfer=files_to_transfer,
                                                    connection=ctx, 
                                                    folder=d_local)
                    log_method = self.logger.info if getattr(transfer, 'ok', True) else self.logger.error
                    log_method(getattr(transfer, 'stdout' if getattr(transfer, 'ok', True) else 'stderr', ''))


                print(jobs)
                kokkkk
                    

                
                # for f in ['output_file', 'stdout_file', 'stderr_file']:
                    # remote_wdir

                    # file = self.resolve_file_name(getattr(j['job'], f, None),parent= parent, host=host)
                    # files_to_transfer.append[getattr(j['job'], f, None)
                    #                  for j in jobs_to_run.values()]
                # local_folder = [getattr(t, 'work_dir_local', None) for j in jobs_to_run.values() for t in j['job'].tasks]
                # local_folder = list(set(local_folder))
                # if len(local_folder) > 1:
                #     self.logger.error('Multiple local folders found, ' +
                #                       'cannot continue')
                # transfer = scheduler.transfer_files(files_to_transfer=files_to_transfer,
                #                                     connection=ctx, 
                #                                     folder=local_folder[0])
                    # for j in jobs_to_run.values():
                    #     id = j['job'].id
                    #     for t in j['job'].tasks:
                    #         f = getattr(t, f, None)
                    #         if f is None:
                    #             continue
                    #         # move up
                    #         p = f['remote']['path']
                    #         ff = Path(p).parent / f'{id}' / Path(p).name
                    #         f['remote']['path'] = str(ff)
                    #         files_to_transfer.append(f)
            
                # fetch_all= any([t.copy_all for j in jobs_to_run.values()
                #         for t in j['job'].tasks])
                # print('oijiji', files_to_transfer)
                # transfer = scheduler.transfer_files(files_to_transfer,
                #                                     ctx,
                #                                     to_remote=False,
                #                                     from_remote=True,
                #                                 )
                # # transfer = self.fetch_results(jobs_to_run, connection=ctx, fetch_all=fetch_all)
                # for r in transfer:
                #     if getattr(r, 'ok', True):
                #         self.logger.info(getattr(r, 'stdout', ''))
                #     else:
                #         self.logger.error(getattr(r, 'stderr', ''))

                # jobs_to_run = self.get_outputs(jobs_to_run)

                scheduler.teardown(jobs_to_run, ctx)
        self.update_db(jobs)
        return self.return_jobs(jobs)
