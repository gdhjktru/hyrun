# from hytools.logger import get_logger

from hyrun.job import ArrayJob

from .runner import Runner

from hyrun.scheduler import get_scheduler
from hydb import get_database


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

    from hytools.logger import get_logger
    return get_logger(print_level=print_level)



def run(*args, **kwargs):
    """Run hsp job."""
    # # return Runner(*args, **kwargs).run(*args, **kwargs)
    logger = _get_logger(*args, print_level='DEBUG', **kwargs)
    aj = ArrayJob(*args, logger=logger, **kwargs)
    #     # check if jobs has an id in database
    # jobs = self.check_finished_jobs(jobs)

    # prep jobs
    for i, job in enumerate(aj.jobs):
        logger.debug(f'prepping job {i}')
        job.scheduler = get_scheduler(job.scheduler,
                                      logger=logger,
                                      **job.scheduler_opt)
        job.job_script = job.scheduler.gen_job_script(job.tasks)
        logger.debug(f'job script: \n{job.job_script}')
        job.job_hash = job._gen_hash()
        logger.debug(f'job hash: {job.job_hash}')
        job.database = get_database(job.database, **job.database_opt)
        logger.debug(f'database: {job.database}')
        # init scheduler

        # job.scheduler = get_scheduler(job.scheduler, **job.scheduler_opt)
        # print(job)

        # init database
        # init all classes
        # check if job is in database
        print('checking if job is in database')
        # check if job is finished
        print('checking if job is finished')
        # generate job scripts
        print('generating job script')
        # write all files to disk and add to job.files
        print('writing all files to disk')

    for job_group in aj.job_grouped:
        # initiate scheduler
        print('initiating scheduler')
        # transfer files to cluster
        print('transferring files to cluster')
        for job in job_group:
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
