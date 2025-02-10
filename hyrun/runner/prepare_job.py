from string import Template
from typing import Optional

from hydb import Database, get_database
from hytools.file import File
from hytools.logger import Logger, get_logger

from hyrun.job import ArrayJob, Job
from hyrun.scheduler import Scheduler, get_scheduler


def prepare_jobs(aj: ArrayJob,
                 **kwargs):
    """Prepare jobs."""
    logger = kwargs.pop('logger', get_logger(print_level='DEBUG'))
    prepper = JobPrep(logger=logger, **kwargs)
    # init databases
    databases = {}
    for job in aj.jobs:
        db_name = prepper.get_database_name(job)
        if db_name in databases:
            continue
        db = prepper.get_database(job)
        db.open()
        databases[db.name] = db
    logger.debug(f'Opened databases: {list(databases.values())}\n')
    # init jobs
    for i, job in enumerate(aj.jobs):
        job_name = job.metadata.get('name', '')
        logger.debug(f'-- Preparing job #{i} {job_name} --')
        # generating job_script and hash
        init_job_script = prepper.get_scheduler(job).gen_job_script(job_name,
                                                                    job.tasks)
        job.job_hash = job.gen_hash(job_script=init_job_script)
        logger.debug(f'job hash: {job.job_hash}')

        # update job_script with job_hash
        job.job_script = File(name=f'job_{job.job_hash}.sh',
                              content=prepper.update_job_script(
                                   init_job_script, job.job_hash),
                              host='localhost')
        logger.debug(f'job script: \n{job.job_script}')
        # lookup hash in database
        job.database_id = prepper.lookup_database(job, databases)
        if job.database_id:
            logger.info(f'Job {job.job_hash} found in database\n')
        else:
            logger.debug(f'Job {job.job_hash} *not* found in database\n')
    # close database connections
    for db in databases.values():
        db.close()
    aj.update()
    logger.debug('-- Preparing jobs done --\n')
    return aj


class JobPrep:
    """Class to prepare Jobs."""

    def __init__(self,
                 logger: Optional[Logger] = None,
                 **kwargs):
        """Initialize."""
        self.logger = logger or get_logger(print_level='ERROR')

    def get_scheduler(self, job: Job) -> Scheduler:
        """Set scheduler."""
        scheduler = (getattr(job, 'scheduler')
                     or getattr(job.tasks[0], 'scheduler'))
        return get_scheduler(scheduler, logger=self.logger)

    def get_database_name(self, job: Job) -> str:
        """Get database name."""
        database_name = (getattr(job, 'database')
                         or getattr(job.tasks[0], 'database'))
        if isinstance(database_name, dict):
            return database_name.get('name', str(database_name))
        return getattr(database_name, 'name', str(database_name))

    def get_database(self, job: Job) -> Database:
        """Get database."""
        database = (getattr(job, 'database')
                    or getattr(job.tasks[0], 'database'))
        return get_database(database,
                            logger=self.logger)

    def update_job_script(self, job_script, job_hash) -> str:
        """Update job script."""
        return Template(job_script).safe_substitute(job_hash=job_hash)

    def lookup_database(self, job: Job,
                        databases: Optional[dict] = {}) -> Optional[int]:
        """Lookup job in database."""
        database_name = self.get_database_name(job)
        database = databases.get(database_name) or self.get_database(job)
        database.open()

        entry = database.search_one(job_hash=job.job_hash,
                                    resolve=False) or {}

        return entry.get('id')
