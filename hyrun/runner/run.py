from hyrun.job import ArrayJob

from .prepare_job import prepare_jobs


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


def run(*args, **kwargs):
    """Run hsp job."""
    # # return Runner(*args, **kwargs).run(*args, **kwargs)
    logger = _get_logger(*args, print_level='DEBUG', **kwargs)
    aj = ArrayJob(*args, logger=logger, **kwargs)
    aj = prepare_jobs(aj, logger=logger)
    if kwargs.get('dryrun', False):
        logger.info('Dryrun')
        return aj


#     for job_group in aj.jobs_grouped:

#         for job in job_group:
#             files = []
#             for task in job.tasks:
#                 print('files should already be sanitized')
#             #     # ths should alreadu been done
#             #     work_dir_local = getattr(task, 'work_dir_local',
# Path.cwd())
#             #     for f in task.files_to_write:
#             #         files.append(File(name=getattr(f, 'name', f),
#             #                           parent=getattr(f, 'folder',
# work_dir_local),
#             #                           content=getattr(f, 'content', None),
#             #                           host='localhost'))

#             # job_script_file = File(name=f'job_{job.job_hash}.sh',
#             #                        parent=Path.cwd(),
#             #                        content=job.job_script,
#             #                        host='localhost')
#             # files.append(job_script_file)
#             for f in files:
#                 print(f'writing {f.path} to disk')
#                 Path(f.path).write_text(f.content)

#         # print('write files to disk, jump to next group if job.db_id
# is not None')

#         # connection_type = list(set(job.connection_type for job in
# job_group))
#         # if len(connection_type) > 1:
#         #     raise ValueError('Connection type must be identical in
#  every group')

#         # with get_connection(connection_type[0],
#         #                     **job_group[0].connection_opt) as connection:
#         #     print('connection established')


# # hcheck that the connection type is identical in every group

#         print('checking if job is in database')
#         # check if job is finished
#         print('checking if job is finished')
#         # generate job scripts
#         print('generating job script')
#         # write all files to disk and add to job.files

#     for job_group in aj.jobs_grouped:
#         print('writing all files to disk')
#         # transfer files to cluster
#         print('transferring files to cluster if job.db_id is None')
#         for job in job_group:
#             if not job.finished:
#                 continue
#             # submit job
#             print('submitting job')
#             # add job to database
#             print('adding job to database')
#             # exit point: if not wait return database entry
#             print('exiting')
#             # check if job is running
#             print('checking if job is running')
#             # check if job is finished
#             print('checking if job is finished')
#             # fetch results
#             print('fetching results')
#             # transfer files from cluster
#             print('transferring files from cluster')
#     # loop jobs by scheduler
#     #
#     # get all files to transfer
#     # for jobs in aj.job_groups:
#     # print(files_to_transfer     )
#     # wait, dryrun, rerun
#     # db connections
#     # loop jobs
#     #

# # def rerun(*args, **kwargs):
# #     """Run."""
# #     return Runner(*args, rerun=True, **kwargs).run()


def get_status(*args, **kwargs):
    """Get status."""
    pass


def fetch_results(*args, **kwargs):
    """Get results."""
    pass


async def arun(*args, **kwargs):
    """Run."""
    pass
