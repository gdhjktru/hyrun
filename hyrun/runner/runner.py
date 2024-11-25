from datetime import timedelta
from pathlib import Path
from time import sleep
from typing import Generator

from hytools.logger import LoggerDummy

from hyrun.job import loop_update_jobs

from .db import JobDatabaseManager
from .gen_jobs import gen_jobs
from .job_prep import JobPrep
from .transfer import FileTransferManager

# try:
#     from hytools.file import File
# except ImportError:
#     from hyset import File


class Runner(FileTransferManager, JobDatabaseManager, JobPrep):
    """Runner."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger',
                                 self._get_attr('logger', *args, **kwargs)
                                 or LoggerDummy())
        # self.parser = kwargs.get('parser', kwargs.get('method', None))

    def _get_attr(self, attr_name, *args, **kwargs):
        """Get attribute."""
        a = kwargs.get(attr_name, None)
        if a or not args:
            return a
        a = args[0][0] if isinstance(args[0], list) else args[0]
        return getattr(a, attr_name, None)

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

    @loop_update_jobs
    def get_status_run(self,
                       *args,
                       job=None,
                       scheduler=None,
                       connection=None, **kwargs) -> dict:
        """Get status."""
        return scheduler.get_status(job,  # type: ignore
                                    connection=connection)

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

    @loop_update_jobs
    def initiate_job(self, *args, job=None, scheduler=None, database=None,
                     **kwargs):
        """Initiate job."""
        if job.db_id is not None:
            self.logger.debug('Job already in database, updating...')
            return self.update_db(job=job, database=database)
        if job.job_hash is not None:
            self.logger.debug('Job hash found, checking database...')
            job_db = self.get_jobs_from_db(job=job,
                                           scheduler=scheduler,
                                           database=database,
                                           key='job_hash',
                                           val=job.job_hash)
            if job_db is not None:
                self.logger.debug('Job found in database, updating...')
                return job_db
        self.logger.debug('Job not found in database, adding...')
        return self.add_to_db(job=job, database=database)

    def get_finished_jobs(self,
                          jobs: dict,
                          connection=None,
                          scheduler=None,
                          **kwargs) -> dict:
        """Get finished jobs."""
        jobs_to_fetch = {}
        for i, j in list(jobs.items()):
            if j['job'].id is not None:
                jobs_to_fetch[i] = j
                del jobs[i]

        self.logger.info(f'Jobs to fetch: {len(jobs_to_fetch)}')
        jobs_to_fetch = self.get_status_run(jobs_to_fetch,
                                            connection=connection)
        jobs_to_do_fetch = {i: j for i, j in jobs_to_fetch.items()
                            if j['job'].status in ['COMPLETED']}
        jobs_to_not_fetch = {i: j for i, j in jobs_to_fetch.items()
                             if j['job'].status not in ['COMPLETED']}
        if jobs_to_not_fetch:
            self.logger.warning('Jobs not finished: ' +
                                f'{jobs_to_not_fetch}')

        self.transfer_from_cluster(jobs=jobs_to_do_fetch,
                                   scheduler=scheduler,
                                   connection=connection, **kwargs)
        return jobs

    async def arun(self, *args, **kwargs):
        """Run."""
        return self.run(*args, wait=False, **kwargs)

    def run(self, *args, **kwargs):
        """Run."""
        jobs = gen_jobs(*args, logger=self.logger, **kwargs)
        # check if jobs has an id

        jobs = self.check_finished_jobs(jobs)
        self.logger.debug('The following jobs are finished: ' +
                          ', '.join([str(i) for i, j in jobs.items()
                                     if j['job'].finished]))
        # remove jobs that are finished
        jobs = {i: j for i, j in jobs.items() if not j['job'].finished}

        schedulers = list(set([j['scheduler'] for j in jobs.values()]))

        self.logger.info(f'Running {len(jobs)} job(s).')
        for i, j in jobs.items():
            self.logger.info(f'   job {i} ({j["scheduler"].name}) task(s): ' +
                             f'{len(j["job"].tasks)} tasks')
        wait = kwargs.get('wait') or any([t.wait for j in jobs.values()
                                          for t in j['job'].tasks])
        dry_run = any([t.dry_run for j in jobs.values()
                       for t in j['job'].tasks])
        rerun = any([t.rerun for j in jobs.values() for t in j['job'].tasks])
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
            jobs_to_fetch = {}
            with scheduler.run_ctx() as ctx:  # tpye: ignore

                self.logger.debug(f'Context manager opened, ctx: {ctx}')
                jobs_to_run = {i: j for i, j in jobs.items()
                               if j['scheduler'] == scheduler}

                jobs_to_run = self.prepare_jobs(jobs_to_run)
                jobs_to_run = self.initiate_job(jobs_to_run)
                if dry_run:
                    self.logger.warning('Performing dry run...')
                    continue

                if not rerun:
                    jobs_to_run = self.get_finished_jobs(jobs_to_run,
                                                         scheduler=scheduler,
                                                         connection=ctx,
                                                         **kwargs)

                if len(jobs_to_run) == 0:
                    self.logger.info('No more jobs to run with ' +
                                     'this scheduler ...')
                    continue

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

                jobs_to_run = self.get_status_run(jobs_to_run, connection=ctx)
                jobs_to_fetch = {i: j for i, j in jobs_to_run.items()
                                 if j['job'].status in ['COMPLETED']}
                jobs_to_not_fetch = {i: j for i, j in jobs_to_run.items()
                                     if j['job'].status not in ['COMPLETED']}
                if jobs_to_not_fetch:
                    self.logger.warning('Jobs not finished: ' +
                                        f'{jobs_to_not_fetch}')

                self.transfer_from_cluster(jobs=jobs_to_fetch,
                                           scheduler=scheduler,
                                           connection=ctx, **kwargs)

                scheduler.teardown(jobs_to_run, ctx)
        if not wait:
            return self.gen_db_info(jobs)
        if dry_run:
            return self.gen_db_info(jobs)
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

                jobs_to_fetch = {i: j for i, j in jobs_to_run.items()
                                 if j['job'].status in ['COMPLETED']}
                jobs_to_not_fetch = {i: j for i, j in jobs_to_run.items()
                                     if j['job'].status not in ['COMPLETED']}
                if jobs_to_not_fetch:
                    self.logger.warning('Jobs not finished: ' +
                                        f'{jobs_to_not_fetch}')

                if not fetch_results:
                    scheduler.teardown(jobs_to_run, ctx)
                    continue

                self.transfer_from_cluster(jobs=jobs_to_fetch,
                                           scheduler=scheduler,
                                           connection=ctx, **kwargs)

                scheduler.teardown(jobs_to_run, ctx)
        return [j['job'].outputs
                if fetch_results
                else j['job'].status for j in jobs.values()]

    def fetch_results(self, *args, **kwargs):
        """Fetch results."""
        return self.get_status(*args, fetch_results=True, **kwargs)
