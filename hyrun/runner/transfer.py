from .prepare_job import JobPrep
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

def prepare_transfer(jobs: list, host=None, logger=None, **kwargs):
    """Prepare transfer."""
    logger = kwargs.pop('logger', get_logger(print_level='Error'))
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

    return files_remote, folders_remote


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
    logger.debug(f'Sending files with rsync command: {rsync_cmd}')
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
    return




# from pathlib import Path
# from socket import gethostname


# class FileTransferManager:
#     """File transfer manager."""

#     def get_files_to_transfer(self, jobs, files_to_transfer=None,
#                               job_keys=None, task_keys=None):
#         """Get files to transfer."""
#         files_to_transfer = files_to_transfer or []
#         job_keys = job_keys or []
#         task_keys = task_keys or []
#         for j in jobs.values():
#             for k in job_keys:
#                 _list = getattr(j['job'], k, [])
#                 _list = [_list] if not isinstance(_list, list) else _list
#                 for f in _list:
#                     files_to_transfer.append(f)
#             for t in j['job'].tasks:
#                 for k in task_keys:
#                     ll = getattr(t, k, [])
#                     ll = [ll] if not isinstance(ll, list) else ll
#                     for f in ll:
#                         files_to_transfer.append(f)
#         return [f for f in files_to_transfer if f is not None]

#     def get_remote_folder(self, jobs) -> str:
#         """Get remote folder."""
#         remote_folders = [getattr(t, 'submit_dir_remote', None)
#                           or getattr(t, 'work_dir_remote', None)
#                           for j in jobs.values()
#                           for t in j['job'].tasks
#                           if getattr(t, 'submit_dir_remote', None)
#                           or getattr(t, 'work_dir_remote', None)]
#         if len(list(set(remote_folders))) > 1:
#             raise ValueError('All tasks in a job must have the same ' +
#                              'remote folder')
#         return str(remote_folders[0]) if len(remote_folders) == 1 else ''

#     def transfer_from_cluster(self,
#                               jobs=None,
#                               scheduler=None,
#                               connection=None,
#                               **kwargs):
#         """Transfer files from cluster."""
#         transfer_all = kwargs.get('transfer_all', True)
#         output_files = ['output_file', 'stdout_file', 'stderr_file']
#         dfiles_to_transfer = {}
#         for j in jobs.values():
#             # finished = all(self.check_finished_single(t)
#             #                for t in j['job'].tasks)
#             # if not finished:
#             #     self.logger.warning(f'Job {j["job"].id} not finished, not ' +
#             #                         'transferring files')
#             #     continue
#             for t, o in zip(j['job'].tasks, j['job'].outputs):
#                 wdir_local = str(getattr(t, 'work_dir_local'))
#                 if wdir_local not in dfiles_to_transfer:
#                     dfiles_to_transfer[wdir_local] = []

#                 if transfer_all:
#                     for f in output_files:
#                         output_file = getattr(o, f, None)
#                         remote_dir = Path(output_file['path']).parent
#                         alls = str(Path(remote_dir) / '*')
#                         dfiles_to_transfer[wdir_local].append(
#                             {'path': alls,
#                              'host': getattr(connection,
#                                              'host',
#                                              gethostname())})
#                         continue

#                 for f in output_files:
#                     remote_file = getattr(o, f, None)
#                     if remote_file:
#                         dfiles_to_transfer[wdir_local].append(
#                             remote_file)

#         for wdir_local, files in dfiles_to_transfer.items():
#             transfer = scheduler.transfer_files(  # type: ignore
#                 files_to_transfer=files,
#                 connection=connection,
#                 folder=wdir_local)
#             log_method = (self.logger.info
#                           if getattr(transfer, 'ok', True)
#                           else self.logger.error)
#             log_method(getattr(transfer, 'stdout'
#                                if getattr(transfer, 'ok', True)
#                                else 'stderr', ''))

#     def transfer_to_cluster(self,
#                             jobs=None,
#                             scheduler=None,
#                             connection=None):
#         """Transfer files to cluster."""
#         files_to_transfer = [j['job'].job_script for j in jobs.values()]

#         files_to_transfer.extend([f for j in jobs.values()
#                                   for t in j['job'].tasks
#                                   for f in t.files_to_write])

#         remote_folder = self.get_remote_folder(jobs)

#         loc = f'{getattr(connection, "host", gethostname())}'
#         loc += f':{remote_folder}' if remote_folder else ''
#         self.logger.debug(f'Files to transfer: {files_to_transfer} to {loc}')

#         transfer = scheduler.transfer_files(  # type: ignore
#             files_to_transfer=files_to_transfer,
#             connection=connection,
#             folder=remote_folder)
#         log_method = (self.logger.info
#                       if getattr(transfer, 'ok', True)
#                       else self.logger.error)
#         log_method(getattr(transfer, 'stdout'
#                            if getattr(transfer, 'ok', True) else 'stderr', ''))
