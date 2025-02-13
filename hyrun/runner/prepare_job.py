from dataclasses import asdict
from pathlib import Path
from shlex import split
from string import Template
from subprocess import run
from typing import Optional

from hydb import Database, get_database
from hyset.base.partial_settings import FOLDER_FILE_MAP
from hytools.connection import get_connection
from hytools.connection.rsync import send_multiple
from hytools.file import File, FileManager
from hytools.logger import Logger, get_logger

from hyrun.job import ArrayJob, Job
from hyrun.scheduler import Scheduler, get_scheduler


def prepare_connection(jobs: list, logger=None, **kwargs):
    """Prepare connection."""
    logger = kwargs.pop('logger', get_logger(print_level='DEBUG'))
    connection = jobs[0].tasks[0].connection
    logger.debug(f'Connection options: {connection}')
    return get_connection(**connection)


def send_files(files_remote: Optional[list] = None,
               folders_remote: Optional[list] = None,
               host: Optional[str] = None,
               user: Optional[int] = None,
               port: Optional[int] = None,
               connection_type: Optional[str] = None,
               logger=None,
               **kwargs):
    """Send files."""
    if connection_type == 'local':
        logger.debug('Local connection, skipping file transfer')
        return
    rsync_cmd = send_multiple(src=files_remote,
                              dest=folders_remote,
                              host=host,
                              user=user,
                              port=port)
    logger.debug(f'rsync command: {rsync_cmd}')
    transfer_result = run(split(rsync_cmd),
                          shell=False,
                          check=True,
                          capture_output=True)
    logger.debug(f'Transfer result: \n{transfer_result.stdout.decode()}')
    if transfer_result.returncode != 0:
        logger.error('Transfer failed:')
        logger.error(f'STDOUT: {transfer_result.stdout.decode()}')
        logger.error(f'STDERR: {transfer_result.stderr.decode()}')
        raise ValueError('Transfer failed')
    logger.debug('-- Transfer successful --\n')
    return


def prepare_transfer(jobs: list, host=None, logger=None, **kwargs):
    """Prepare transfer."""
    logger = kwargs.pop('logger', get_logger(print_level='DEBUG'))
    logger.debug(f'-- Preparing transfer to {host}--')
    prepper = JobPrep(logger=logger, **kwargs)
    files_remote = []
    folders_remote = []
    for job in jobs:
        force_recompute = prepper.get_attr_from_tasks(job, 'force_recompute',
                                                      is_bool=True)
        if job.database_id and not force_recompute:
            logger.debug(f'Job {job.job_hash} already exists in ' +
                         f'database with id {job.job_id}')
            continue
        prepper.add_job_script_to_files(job)
        prepper.dump_files(job, **kwargs)
        files_local, folders_local = prepper.get_files_to_transfer(job)
        files_remote.extend(files_local)
        folders_remote.extend(folders_local)

    logger.debug('-- Preparing transfer done --\n')
    return files_remote, folders_remote


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

    def get_attr_from_tasks(self,
                            job: Job,
                            attr: str,
                            is_bool: Optional[bool] = False):
        """Get attribute from tasks."""
        if is_bool:
            return any([getattr(job.tasks[i], attr)
                        for i in range(len(job.tasks))])
        return job.tasks[0].get(attr, None)

    def get_files_to_transfer(self, job: Job):
        """Get files to transfer."""
        files_remote = []
        folders_remote = []
        for task in job.tasks:
            for folder_local, files in FOLDER_FILE_MAP.items():
                folder_remote = getattr(task,
                                        folder_local.replace(
                                            'local', 'remote'),
                                        None)
                folder_remote = str(folder_remote).replace('job_id', '')
                for file in files:
                    f = getattr(task, file, [])
                    if f is None:
                        continue
                    f = [f] if not isinstance(f, list) else f
                    for ff in f:
                        if not Path(ff.path).exists():
                            continue
                        files_remote.append(ff)
                        folders_remote.append(folder_remote)
        return files_remote, folders_remote

    def dump_files(self, job: Job, **kwargs):
        """Dump files."""
        for task in job.tasks:
            names = [task.files_to_write[i].name
                     for i in range(len(task.files_to_write))]
            # folders = [task.files_to_write[i].folder
            #               for i in range(len(task.files_to_write))]
            # hosts = [task.files_to_write[i].host
            #          for i in range(len(task.files_to_write))]
            self.logger.debug(f'Writing files to disk: {names}')
            FileManager.write_file_local(task.files_to_write)

    def add_job_script_to_files(self, job: Job):
        """Add job script to files."""
        job.tasks[0].files_to_write.append(
            File(
                **{
                    **asdict(job.job_script),  # type: ignore
                    'folder': str(job.tasks[0].submit_dir_local),
                    'host': 'localhost'
                    }
                )
            )

    def get_scheduler(self, job: Job, **kwargs) -> Scheduler:
        """Set scheduler."""
        logger = kwargs.pop('logger', self.logger)
        scheduler = (getattr(job, 'scheduler')
                     or getattr(job.tasks[0], 'scheduler'))
        return get_scheduler(scheduler, logger=logger, **kwargs)

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
