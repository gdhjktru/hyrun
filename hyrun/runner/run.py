# from hytools.logger import get_logger

# from hyrun.job import ArrayJob

# from .runner import Runner


# def run(*args, dependencies=None, **kwargs):
#     """Run hsp job."""
#     # # return Runner(*args, **kwargs).run(*args, **kwargs)
#     # aj = ArrayJob(*args, logger=get_logger(print_level='DEBUG'), **kwargs)
#     # dependency_graph  = kwargs...
#     #     # check if jobs has an id in database
#     # jobs = self.check_finished_jobs(jobs)

#     # prep jobs
#     for job in aj.jobs:
#         # init all classes
#         # check if job is in database
#         print('checking if job is in database')
#         print('checking if all dependencies are met, use networkx.all_simple_paths#')
#         # check if job is finished
#         print('checking if job is finished')
#         # generate job scripts
#         print('generating job script')
#         # write all files to disk and add to job.files
#         print('writing all files to disk')

#     for job_group in aj.job_groups:
#         # initiate scheduler
#         print('initiating scheduler')
#         # transfer files to cluster
#         print('transferring files to cluster')
#         for job in job_group:
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


# def get_status(*args, **kwargs):
#     """Get status."""
#     return Runner(*args, **kwargs).get_status(*args, **kwargs)


# def fetch_results(*args, **kwargs):
#     """Get results."""
#     return Runner(*args, **kwargs).fetch_results(*args, **kwargs)


# async def arun(*args, **kwargs):
#     """Run."""
#     return run(*args, wait=True, **kwargs)
