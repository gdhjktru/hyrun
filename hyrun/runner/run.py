# from hytools.logger import get_logger

from hashlib import sha256
from pathlib import Path
from string import Template
from typing import Optional

from hydb import get_database
from hytools.connection import get_connection
from hytools.file import File, get_file
from hytools.logger import Logger, get_logger

from hyrun.job import ArrayJob
from hyrun.scheduler import get_scheduler

from .runner import Runner


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

    from hytools.logger import Logger, get_logger
    return get_logger(print_level=print_level)


def prepare_jobs(aj, logger: Optional[Logger] = None):
    """Prepare jobs."""
    logger = logger or get_logger()
    for i, job in enumerate(aj.jobs):
        logger.debug(f'...prepping job {i}')
        job.scheduler = get_scheduler(job.scheduler,
                                      logger=logger,
                                      **job.scheduler_opt)
        job.job_script = job.scheduler.gen_job_script(job.name,
                                                      job.tasks)

        job.job_hash = _gen_hash([job.name or '',
                                  job.connection_opt.get('user', ''),
                                  job.connection_opt.get('host', ''),
                                  getattr(job.scheduler, 'name',
                                          str(job.scheduler)),
                                  job.job_script or '',
                                  ])

        logger.debug(f'job hash: {job.job_hash}')
        print('check if job hash is in database. if yes, update db id -> for this one one only gets the status updated')
        print('problem: if alrady updated with the latest we dont want to overwrite it with potentital Nones, \
              that meens when updating we need to make sure we add information/have more than before')
        print('use the slurm status and create a mapping')


        # job.status = status_mapping.get(job.status, 0)
        # if job.db_id is not None:
        #     print('if job is in the database, update the status')
        #     print('if job is not




        job.job_script = Template(job.job_script).safe_substitute(
            job_name=job.job_hash)
        logger.debug(f'job script: \n{job.job_script}')

        # job.job_script = File(f'job_{job.job_hash}.sh',
        #                       content=job.job_script,
        #                       host='localhost')

        print('add job script to files to write and sanitize all files')
        print('check that all objects can go into a database)')



        job.database = get_database(job.database, **job.database_opt)
        logger.debug(f'database: {job.database}')
        job.database.open()
        entry = job.database.search_one(job_hash=job.job_hash, resolve=True)
        if entry:
            logger.info(f'Job {job.job_hash} found in database')
            print('here we need to be careful!!!')
            job.__dict__.update(entry)
        job.database.close()
        logger.debug('\n')
        # add job script to files_to_write and then sanitize files_to_write,
        #files_to_remote, files_for_restarting
        for task in job.tasks:
            task.files_to_write.append(File(name=f'job_{job.job_hash}.sh',
                                            content=job.job_script,
                                            host='localhost'))
            task.files_to_write = sanitize_files(task.files_to_write)
            task.files_to_remote = sanitize_files(task.files_to_remote)
            task.files_for_restarting = sanitize_files(task.files_for_restarting)
    aj.update()
    return aj

def sanitize_files(files):
    """Sanitize files."""
    for i, f in enumerate(files):
        files[i] = get_file(f)
    return [get_file(f) for f in files]

def _gen_hash(hashlist: list) -> str:
    """Generate hash."""
    return sha256(
        ''.join(hashlist).encode()).hexdigest()




def run(*args, **kwargs):
    """Run hsp job."""
    # # return Runner(*args, **kwargs).run(*args, **kwargs)
    logger = _get_logger(*args, print_level='DEBUG', **kwargs)
    aj = ArrayJob(*args, logger=logger, **kwargs)
    aj = prepare_jobs(aj, logger=logger)

    if kwargs.get('dryrun', False):
        print('teardown')
        for job in aj.jobs:
            job.scheduler = getattr(job.scheduler, 'name', str(job.scheduler))
            job.database = getattr(job.database, 'name', str(job.database))
        return aj

    for job_group in aj.jobs_grouped:

        for job in job_group:
            files = []
            for task in job.tasks:
                print('files should already be sanitized')
            #     # ths should alreadu been done
            #     work_dir_local = getattr(task, 'work_dir_local', Path.cwd())
            #     for f in task.files_to_write:
            #         files.append(File(name=getattr(f, 'name', f),
            #                           parent=getattr(f, 'folder', work_dir_local),
            #                           content=getattr(f, 'content', None),
            #                           host='localhost'))

            # job_script_file = File(name=f'job_{job.job_hash}.sh',
            #                        parent=Path.cwd(),
            #                        content=job.job_script,
            #                        host='localhost')
            # files.append(job_script_file)
            for f in files:
                print(f'writing {f.path} to disk')
                Path(f.path).write_text(f.content)




        # print('write files to disk, jump to next group if job.db_id is not None')





        # connection_type = list(set(job.connection_type for job in job_group))
        # if len(connection_type) > 1:
        #     raise ValueError('Connection type must be identical in every group')


        # with get_connection(connection_type[0],
        #                     **job_group[0].connection_opt) as connection:
        #     print('connection established')




# hcheck that the connection type is identical in every group


        print('checking if job is in database')
        # check if job is finished
        print('checking if job is finished')
        # generate job scripts
        print('generating job script')
        # write all files to disk and add to job.files


    for job_group in aj.jobs_grouped:
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
