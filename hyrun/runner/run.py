# from hytools.logger import get_logger

from hyrun.job import ArrayJob

from .runner import Runner

from hyrun.scheduler import get_scheduler
from hytools.connection import get_connection
from hydb import get_database
from hytools.logger import Logger
from typing import Optional


def scheduler_exec(connection, scheduler_func, *args, **kwargs):
    """Execute scheduler function."""
    return connection.execute(scheduler_func(*args, **kwargs))

def _get_logger(*args, print_level='ERROR', **kwargs):
    """Get logger."""
    logger = next(
        (a.logger for arg in args 
         for a in (arg if isinstance(arg, list) else [arg])
         if hasattr(a, 'logger')),
        None
    )
    if logger:
        return logger
    if 'logger' in kwargs:
        return kwargs['logger']

    from hytools.logger import get_logger, Logger
    return get_logger(print_level=print_level)


def prepare_jobs(aj, logger: Optional[Logger] = None):
    """Prepare jobs."""
    logger = logger or  get_logger()
    for i, job in enumerate(aj.jobs):
        logger.debug(f'...prepping job {i}')
        job.scheduler = get_scheduler(job.scheduler,
                                      logger=logger,
                                      **job.scheduler_opt)
        job.job_script = job.scheduler.gen_job_script(job.tasks)
        logger.debug(f'job script: \n{job.job_script}')
        job.job_hash = job._gen_hash()
        logger.debug(f'job hash: {job.job_hash}')
        job.database = get_database(job.database, **job.database_opt)
        logger.debug(f'database: {job.database}')
        job.database.open()
        entry = job.database.search_one(job_hash=job.job_hash, resolve=True)
        if entry:
            logger.info(f'Job {job.job_hash} found in database')
            job.id = entry.id
            job.db_id = entry.id
            job.status = entry.status
            job.finished = entry.finished
            job.metadata = entry.metadata
        job.database.close()
    aj.update()
    return aj


def run(*args, **kwargs):
    """Run hsp job."""
    # # return Runner(*args, **kwargs).run(*args, **kwargs)
    logger = _get_logger(*args, print_level='DEBUG', **kwargs)
    aj = ArrayJob(*args, logger=logger, **kwargs)
    aj = prepare_jobs(aj, logger)
    
    if kwargs.get('dryrun', False):
        for job in aj.jobs:
            job.scheduler = getattr(job.scheduler, 'name', str(job.scheduler))
            job.database = getattr(job.database, 'name', str(job.database))
        return aj




        print('checking if job is in database')
        # check if job is finished
        print('checking if job is finished')
        # generate job scripts
        print('generating job script')
        # write all files to disk and add to job.files
        

    for job_group in aj.job_grouped:
        print('writing all files to disk')
        # transfer files to cluster
        print('transferring files to cluster if job.db_id is None')
        for job in job_group:
            if not job.finished:
                continue
            # submit job
            print('submitting job')
            # add job to database
            print('adding job to database')
            # exit point: if not wait return database entry
            print('exiting')
            # check if job is running
            print('checking if job is running')
            # check if job is finished
            print('checking if job is finished')
            # fetch results
            print('fetching results')
            # transfer files from cluster
            print('transferring files from cluster')
    # loop jobs by scheduler
    #
    # get all files to transfer
    # for jobs in aj.job_groups:
    # print(files_to_transfer     )
    # wait, dryrun, rerun
    # db connections
    # loop jobs
    #

# def rerun(*args, **kwargs):
#     """Run."""
#     return Runner(*args, rerun=True, **kwargs).run()


def get_status(*args, **kwargs):
    """Get status."""
    return Runner(*args, **kwargs).get_status(*args, **kwargs)


def fetch_results(*args, **kwargs):
    """Get results."""
    return Runner(*args, **kwargs).fetch_results(*args, **kwargs)


async def arun(*args, **kwargs):
    """Run."""
    return run(*args, wait=True, **kwargs)
