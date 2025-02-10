from typing import Optional
from hytools.logger import get_logger, Logger
from hyrun.job import ArrayJob, Job
from hyrun.scheduler import get_scheduler, Scheduler
from hydb import get_database
from hytools.file import File
from string import Template
from hashlib import sha256
from pathlib import Path
from copy import deepcopy


def prepare_jobs(aj: ArrayJob,
                 **kwargs):
    """Prepare jobs."""
    logger = kwargs.pop('logger', get_logger(print_level='DEBUG'))
    prepper = JobPrep(logger=logger, **kwargs)
    for i, job in enumerate(aj.jobs):
        logger.debug(f'Preparing job #{i}')
        # print(job)
        job.scheduler = prepper.get_scheduler(job)
        # print(job.tasks)
        job.job_script = job.scheduler.gen_job_script(
             job.metadata.get('name', ''), job.tasks)
        job.job_hash = job.gen_hash()
        
        # print(job.job_script)
        # print(job.job_hash)


    oihjoijhoiioi




class JobPrep:
        """Class to prepare"""
        
        def __init__(self,
                     logger: Optional[Logger] = None,
                     **kwargs):
             """Initialize."""
             self.logger = logger or get_logger(print_level='ERROR')

        def get_scheduler(self, job: Job):
            """Set scheduler."""
            scheduler = getattr(job, 'scheduler') or getattr(job.tasks[0],
                                                             'scheduler')                
            if isinstance(scheduler, Scheduler):
                 return scheduler
            elif isinstance(scheduler, str):
                 scheduler = {'scheduler_type': scheduler}
            elif not isinstance(scheduler, dict):
                 raise TypeError('scheduler must be of type str, dict or ' +
                                 'Scheduler')
            return get_scheduler(logger=self.logger, **scheduler)


    #     job.scheduler = get_scheduler(job.scheduler,
    #                                   logger=logger,
    #                                   **job.scheduler_opt)
    #     job.job_script = job.scheduler.gen_job_script(job.name,
    #                                                   job.tasks)

    #     job.job_hash = _gen_hash([job.name or '',
    #                               job.connection_opt.get('user', ''),
    #                               job.connection_opt.get('host', ''),
    #                               getattr(job.scheduler, 'name',
    #                                       str(job.scheduler)),
    #                               job.job_script or '',
    #                               ])

    #     logger.debug(f'job hash: {job.job_hash}')
    #     print('check if job hash is in database. if yes, update db id -> for this one one only gets the status updated')
    #     print('problem: if alrady updated with the latest we dont want to overwrite it with potentital Nones, \
    #           that meens when updating we need to make sure we add information/have more than before')
    #     print('use the slurm status and create a mapping')


    #     # job.status = status_mapping.get(job.status, 0)
    #     # if job.db_id is not None:
    #     #     print('if job is in the database, update the status')
    #     #     print('if job is not




    #     job.job_script = Template(job.job_script).safe_substitute(
    #         job_name=job.job_hash)
    #     logger.debug(f'job script: \n{job.job_script}')

    #     # job.job_script = File(f'job_{job.job_hash}.sh',
    #     #                       content=job.job_script,
    #     #                       host='localhost')

    #     print('add job script to files to write and sanitize all files')
    #     print('check that all objects can go into a database)')



    #     job.database = get_database(job.database, **job.database_opt)
    #     logger.debug(f'database: {job.database}')
    #     job.database.open()
    #     entry = job.database.search_one(job_hash=job.job_hash, resolve=True)
    #     if entry:
    #         logger.info(f'Job {job.job_hash} found in database')
    #         print('here we need to be careful!!!')
    #         job.__dict__.update(entry)
    #     job.database.close()
    #     logger.debug('\n')
    #     # add job script to files_to_write and then sanitize files_to_write,
    #     #files_to_remote, files_for_restarting
    #     for task in job.tasks:
    #         task.files_to_write.append(File(name=f'job_{job.job_hash}.sh',
    #                                         content=job.job_script,
    #                                         host='localhost'))
    #         task.files_to_write = sanitize_files(task.files_to_write)
    #         task.files_to_remote = sanitize_files(task.files_to_remote)
    #         task.files_for_restarting = sanitize_files(task.files_for_restarting)
    # aj.update()
    # return aj
