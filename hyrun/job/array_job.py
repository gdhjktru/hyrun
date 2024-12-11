from dataclasses import dataclass, field, fields
from functools import singledispatchmethod, wraps
from typing import Any, Dict, List, Union

from hytools.logger import get_logger

from .job import Job  # noqa: F401


def update_arrayjob(func):
    """Decorate to loop over jobs."""
    @wraps(func)
    def wrapper(self, arrayjob: ArrayJob, *args, **kwargs) -> ArrayJob:
        """Loop over jobs."""
        if len(arrayjob) < 1:
            raise ValueError('No jobs provided')
        for i, job in enumerate(arrayjob.jobs):
            arrayjob[i] = func(self, job, *args, **kwargs)
        return arrayjob
    return wrapper


@dataclass
class ArrayJob:
    """Array job tools."""

    jobs: List[Job] = field(default_factory=list)
    logger: Any = None

    def __post_init__(self):
        """Post init."""
        self.logger = self.logger or get_logger()
        self.logger.debug(f'ArrayJob initialized with {len(self.jobs)} jobs')
        # convert jobs to a list of lists of jobs
        self.jobs = self._normalize_input(self.jobs)
        self.logger.debug(f'ArrayJob normalized to {len(self.jobs)} jobs')

    def __getitem__(self, job_index: int) -> Union[Job, int, Dict[str, Any]]:
        """Get job."""
        return self.jobs[job_index]

    def __setitem__(self, job_index: int,
                    job: Union[Job, int, Dict[str, Any]]):
        """Set job."""
        self.jobs[job_index] = self._convert_to_job(job)
        self.logger.debug(f'ArrayJob set job {job_index} to {job}')
        self.jobs = sorted(self.jobs,
                           key=lambda job: (job.scheduler, job.database))
        self.logger.debug('ArrayJob re-sorted jobs by scheduler and database')

    def __len__(self) -> int:
        """Get length."""
        return len(self.jobs)

    def _normalize_input(self,
                         jobs_input: Union[Job,
                                           List[Union[Job, List[Any]]]]
                         ) -> List[Job]:
        """Convert input into a list of lists."""
        if not isinstance(jobs_input, list):
            return self._normalize_input([jobs_input])
        return sorted([self._convert_to_job(job) for job in jobs_input],
                      key=lambda job: (job.scheduler, job.database))

    @singledispatchmethod
    def _convert_to_job(self, job: Any) -> Job:
        """Convert input to Job."""
        # assume we have a task or a list of tasks
        self.logger.debug(f'job normalization detected task {type(job)}')
        if not isinstance(job, list):
            job = [job]
        check = self._check_common_attributes(job,
                                              ['database', 'database_opt',
                                               'scheduler', 'scheduler_opt'])
        if not check:
            msg = 'All tasks in a job must have the same ' + \
                    'database, database_opt, scheduler, and scheduler_opt'
            self.logger.error(msg)
            raise ValueError(msg)

        job_dict = {f.name: getattr(job[0], f.name, None) for f in fields(Job)}
        job_dict['tasks'] = job
        return self._convert_to_job(job_dict)

    @_convert_to_job.register(Job)
    def _(self, job: Job) -> Job:
        """Convert Job to Job."""
        self.logger.debug(f'job normalization detected Job {type(job)}')
        return job

    @_convert_to_job.register(dict)
    def _(self, job: dict) -> Job:
        """Convert dictionary to Job."""
        self.logger.debug(f'job normalization detected dictionary {type(job)}')
        try:
            return Job(**job)
        except AttributeError as e:
            self.logger.error(f'Could not convert dictionary to Job: {job}')
            raise e

    def _check_common_attributes(self,
                                 jobs: List[Any],
                                 common_attributes: List[str]) -> bool:
        """Check that jobs have the same values for a list of attributes."""
        for attr in common_attributes:
            values = []
            for run_settings in jobs:
                value = getattr(run_settings, attr)
                if isinstance(value, dict):
                    # Convert dictionary to a tuple of sorted items
                    value = tuple(sorted(value.items()))
                values.append(value)
            if len(set(values)) > 1:
                return False
        return True


#

# def gen_array_job(arg, **kwargs) -> dict:
#     """Generate jobs."""
#     aj = ArrayJobOld()
#     # case only one job/task
#     if not isinstance(arg, list):
#         arg = [[arg]]
#     # dont allow more than 2D list
#     aj._check_nested_levels(arg)
#     # case 1d list
#     if not any(isinstance(item, list) for item in arg):
#         arg = [arg]
#     # make sure tasks is a list of lists
#     tasks = [aj._flatten_list(a) for a in arg]
#     # check if all tasks are list of run settings, or job or db_id
#     jobs = aj.set_jobs(tasks, **kwargs)
#     jobs = aj.add_databases(jobs, **kwargs)
#     # jobs = aj.add_connections(jobs)
#     jobs = aj.add_schedulers(jobs)
#     # check that some parameters are identical among tasks
#     jobs = aj._check_job_params(jobs)
#     return jobs

# class ArrayJobOld:
#     """Array job tools."""

#     def __init__(self, *args, **kwargs):
#         """Initialize."""
#         self.logger = kwargs.get('logger', get_logger())


#     def _check_all_types_of_nested_list(self, list_, type_to_check):
#         """Check all types of nested list."""
#         if not isinstance(list_, list):
#             return isinstance(list_, type_to_check)
#         else:
#             return all(self._check_all_types_of_nested_list(item,
#                                                             type_to_check)
#                        for item in list_)

#     def set_jobs(self, job, **kwargs):
#         """Check tasks."""
#         jobs = {}
#         ijob = 0
#         for j in job:
#             if self._check_all_types_of_nested_list(j, (int, Job)):
#                 if isinstance(j[0], list):
#                     _list = self._flatten_2d_list(j)
#                 else:
#                     _list = j
#                 for item in _list:
#                     if isinstance(item, int):
#                         jobs[ijob] = self.resolve_db_id(item, **kwargs)
#                     else:  # Assuming item is of type Job
#                         jobs[ijob] = {'job': item}
#                     ijob += 1
#             else:
#                 outputs = [Output().from_dict(t.__dict__) for t in j]
#                 jobs[ijob] = {'job': Job(tasks=j, outputs=outputs)}
#                 ijob += 1
#         return jobs

    # def resolve_db_id(self, db_id: int, **kwargs) -> dict:
    #     """Resolve db_id."""
    #     db = (get_database(kwargs['database'], logger=self.logger)
    #           if kwargs.get('database')
    #           else DatabaseDummy())
    #     db_id = db._db_id(db_id)
    #     entry = db.get(key='db_id', value=db_id)
    #     # if isinstance(entry, list):
    #     #     entry = entry[0]
    #     job = db.dict_to_obj(entry)
    #     job.db_id = db_id
    #     return {'job': job}

    # def get_connections(self, jobs) -> list[dict]:
    #     """Extract connections."""
    #     c = []
    #     for job in jobs.values():
    #         connections = []
    #         if getattr(job['job'].tasks[0], 'connection', None):
    #             connections.append(job['job'].tasks[0].connection)
    #         elif getattr(job['job'].tasks[0], 'extract_connection', None):
    #             connections.append(job['job'].tasks[0].extract_connection())
    #         else:
    #             connections.append({})
    #         if len(set(json.dumps(d, sort_keys=True)
    #                for d in connections)) > 1:
    #             self.logger.error('All tasks in a job must have the same ' +
    #                               'connection')
    #         c.append(connections[0])
    #     return c

    # def _check_nested_levels(self, list_, level=0):
    #     """Check nested levels with recursion."""
    #     if level > 1:
    #         raise ValueError('Run settings must be at most 2D')
    #     for item in list_:
    #         if isinstance(item, list):
    #             self._check_nested_levels(item, level + 1)

    # def _flatten_list(self, list_):
    #     """Flatten a nested list."""
    #     if not isinstance(list_, list):
    #         return [list_]
    #     else:
    #         return [item
    #                 for sublist in list_
    #                 for item in self._flatten_list(sublist)]

    # def add_databases(self, jobs, **kwargs) -> dict:
    #     """Get databases."""
    #     for job in jobs.values():
    #         if job.get('database', None) is not None:
    #             continue
    #         db = list(set([rs.database for rs in job['job'].tasks]))
    #         if len(db) > 1:
    #             self.logger.error('All tasks in a job must have the same ' +
    #                               'database')
    #         job['database'] = (get_database(db[0], logger=self.logger)
    #                            if db[0]
    #                            else kwargs.get('database', DatabaseDummy()))
    #     return jobs

    # def add_schedulers(self, jobs):
    #     """Add schedulers."""
    #     connections = self.get_connections(jobs)
    #     for i, job in jobs.items():
    #         if job.get('scheduler', None):
    #             continue
    #         s = list(set([rs.scheduler for rs in job['job'].tasks]))
    #         if len(s) > 1:
    #             self.logger.error('All tasks in a job must have the same ' +
    #                               'scheduler')
    #         job['scheduler'] = get_scheduler(s[0],
    #                                          connection=connections[i],
    #                                          logger=self.logger)
    #     return jobs

    # def _flatten_2d_list(self, list_):
    #     """Flatten a 2D list."""
    #     if any(isinstance(item, list) for item in list_):
    #         return [item for sublist in list_ for item in sublist]
    #     return list_

    # def change_key(self, dict_obj, old_key, new_key):
    #     """Change key."""
    #     dict_obj[new_key] = dict_obj.pop(old_key)

    # def _check_job_params(self, jobs) -> dict:
    #     """Check job parameters."""
    #     new_jobs: dict = {}
    #     i = 0
    #     for job in jobs.values():
    #         new_job = job['scheduler'].check_job_params(job['job'])
    #         if not isinstance(new_job, list):
    #             new_job = [new_job]
    #         for j in new_job:
    #             new_jobs[i] = {}
    #             new_jobs[i]['job'] = j
    #             new_jobs[i]['scheduler'] = job['scheduler']
    #             new_jobs[i]['database'] = job['database']
    #             i += 1
    #     return new_jobs

    # def _sort_jobs(self, jobs, key):
    #     """Sort jobs."""
    # return {i: j for i, j in sorted(jobs.items(), key=lambda x: x[1][key])}

    # def _sort_jobs_by_scheduler(self, jobs):
    #     """Sort jobs by scheduler."""
    #     return self._sort_jobs(jobs, 'scheduler')

    # # sort the sorted jobs by database
    # def _sort_jobs_by_database(self, jobs):
    #     """Sort jobs by database."""
    #     return self._sort_jobs(jobs, 'database')
