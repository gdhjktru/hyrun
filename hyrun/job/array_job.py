import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union
from functools import singledispatchmethod
from .output import Output


from hydb import Database, DatabaseDummy, get_database  # noqa: F401
from hytools.logger import get_logger

from hyrun.job import Job, Output  # noqa: F401
from hyrun.scheduler import get_scheduler



@dataclass
class ArrayJob:

    jobs: List[Union[Any, Job, int, Dict[str, Any]]] = field(default_factory=list)
    logger: Any = None

    def __post_init__(self):
        self.logger = self.logger or get_logger()
        self.logger.debug(f'ArrayJob initialized with {len(self.jobs)} jobs')
        # convert jobs to a list of lists of jobs
        self.jobs = self._normalize_input(self.jobs)
        
    def __getitem__(self, job_index: int) -> Union[Job, int, Dict[str, Any]]:
        return self.jobs[job_index]
    
    def __setitem__(self, job_index: int, job: Union[Job, int, Dict[str, Any]]):
        self.jobs[job_index] = job

    def __len__(self) -> int:
        return len(self.jobs)
    
    def _normalize_input(self,
                         jobs_input: Union[Job,
                                           List[Union[Job, List[Any]]]]
                        ) -> List[Job]:
        """Convert input into a list of lists."""
        if not isinstance(jobs_input, list):
            return self._normalize_input([jobs_input])
        return [self._convert_to_job(job) for job in jobs_input]
    

    @singledispatchmethod
    def _convert_to_job(self, job: Any) -> Job:
        # assume we have a task or a list of tasks
        self.logger.debug(f'job normalization detected task {type(job)}')
        if isinstance(job, list):
            return Job(tasks=job)
        return Job(tasks=[job])
    
    @_convert_to_job.register(Job)
    def _(self, job: Job) -> Job:
        self.logger.debug(f'job normalization detected Job {type(job)}')
        return job
    
    @_convert_to_job.register(dict)
    def _(self, job: dict) -> Job:
        self.logger.debug(f'job normalization detected dictionary {type(job)}')
        return Job(**job)
    
    # @_convert_to_job.register(int)
    # def _(self, job: int) -> Job:
    #     self.logger.debug(f'job normalization detected database id {type(job)}')
    #     return self.resolve_db_id(job)
    
    #     def resolve_db_id(self, db_id: int, **kwargs) -> dict:
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
    

    

    #     tasks: Optional[List[Any]] = None
    # outputs: Optional[List[Output]] = None
    # job_script: Optional[str] = None
    # database: Optional[Union[str, Path, Database]] = None
    # scheduler: Optional[Union[str, Scheduler]] = None
    # metadata: Optional[dict] = field(default_factory=dict)


           

        # if isinstance(jobs_input, Job):
        #     return [[jobs_input]]
        # elif isinstance(jobs_input, list):
        #     return [[job] if isinstance(job, Job) else job
        #             for job in jobs_input]






    # def add_task_to_job(self, job_index: int, task: Task):
    #     job = self.jobs[job_index]
    #     if isinstance(job, Job):
    #         job.tasks.append(task)

    # def add_job(self, job: Union[Job, int, Dict[str, Any]]):
    #     self.jobs.append(job)

    # def remove_job(self, job_index: int):
    #     self.jobs.pop(job_index)



# job in jobs.values():
#             connections = []
#             if getattr(job['job'].tasks[0], 'connection', None):
#                 connections.append(job['job'].tasks[0].connection)
#

def gen_array_job(arg, **kwargs) -> dict:
    """Generate jobs."""
    aj = ArrayJobOld()
    # case only one job/task
    if not isinstance(arg, list):
        arg = [[arg]]
    # dont allow more than 2D list
    aj._check_nested_levels(arg)
    # case 1d list
    if not any(isinstance(item, list) for item in arg):
        arg = [arg]
    # make sure tasks is a list of lists
    tasks = [aj._flatten_list(a) for a in arg]
    # check if all tasks are list of run settings, or job or db_id
    jobs = aj.set_jobs(tasks, **kwargs)
    jobs = aj.add_databases(jobs, **kwargs)
    # jobs = aj.add_connections(jobs)
    jobs = aj.add_schedulers(jobs)
    # check that some parameters are identical among tasks
    jobs = aj._check_job_params(jobs)
    return jobs

# def normalize_to_list_of_lists(jobs_input: Union[Job, List[Union[Job, List[Any]]]], current_level: int = 0) -> List[List[Job]]:
#     """
#     Recursively normalize input to a list of lists of jobs while ensuring the nesting level is within limits.
#     """
#     MAX_NESTING_LEVEL = 2

#     # Check if the current level exceeds the maximum allowed nesting level
#     if current_level > MAX_NESTING_LEVEL:
#         raise ValueError("Nesting level too large. Only up to list of lists is allowed.")
    
#     if isinstance(jobs_input, Job):
#         return [[jobs_input]]

#     # If it's a list, determine if it contains jobs or nested lists with jobs
#     if isinstance(jobs_input, list):
#         # If the list contains jobs, wrap it in another list
#         if all(isinstance(item, Job) for item in jobs_input):
#             if current_level == 0:
#                 return [jobs_input]  # Top-level list of jobs
#             else:
#                 return jobs_input  # Sub-level list of jobs (one more level will be added in the parent call)

#         # If the list contains lists, recursively normalize each sub-list
#         elif all(isinstance(item, list) for item in jobs_input):
#             normalized = [normalize_to_list_of_lists(item, current_level + 1) for item in jobs_input]
#             # Flatten the list of lists by one level
#             if current_level + 1 == MAX_NESTING_LEVEL:
#                 return [sublist for innerlist in normalized for sublist in innerlist]
#             else:
#                 return normalized

#     # If input format is not recognized, raise an error
#     raise ValueError("Invalid input format. Expected Job, list of Jobs, or list of lists of Jobs.")


class ArrayJobOld:
    """Array job tools."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', get_logger())


    def _check_all_types_of_nested_list(self, list_, type_to_check):
        """Check all types of nested list."""
        if not isinstance(list_, list):
            return isinstance(list_, type_to_check)
        else:
            return all(self._check_all_types_of_nested_list(item,
                                                            type_to_check)
                       for item in list_)

    def set_jobs(self, job, **kwargs):
        """Check tasks."""
        jobs = {}
        ijob = 0
        for j in job:
            if self._check_all_types_of_nested_list(j, (int, Job)):
                if isinstance(j[0], list):
                    _list = self._flatten_2d_list(j)
                else:
                    _list = j
                for item in _list:
                    if isinstance(item, int):
                        jobs[ijob] = self.resolve_db_id(item, **kwargs)
                    else:  # Assuming item is of type Job
                        jobs[ijob] = {'job': item}
                    ijob += 1
            else:
                outputs = [Output().from_dict(t.__dict__) for t in j]
                jobs[ijob] = {'job': Job(tasks=j, outputs=outputs)}
                ijob += 1
        return jobs

    def resolve_db_id(self, db_id: int, **kwargs) -> dict:
        """Resolve db_id."""
        db = (get_database(kwargs['database'], logger=self.logger)
              if kwargs.get('database')
              else DatabaseDummy())
        db_id = db._db_id(db_id)
        entry = db.get(key='db_id', value=db_id)
        # if isinstance(entry, list):
        #     entry = entry[0]
        job = db.dict_to_obj(entry)
        job.db_id = db_id
        return {'job': job}

    def get_connections(self, jobs) -> list[dict]:
        """Extract connections."""
        c = []
        for job in jobs.values():
            connections = []
            if getattr(job['job'].tasks[0], 'connection', None):
                connections.append(job['job'].tasks[0].connection)
            elif getattr(job['job'].tasks[0], 'extract_connection', None):
                connections.append(job['job'].tasks[0].extract_connection())
            else:
                connections.append({})
            if len(set(json.dumps(d, sort_keys=True)
                   for d in connections)) > 1:
                self.logger.error('All tasks in a job must have the same ' +
                                  'connection')
            c.append(connections[0])
        return c

    def _check_nested_levels(self, list_, level=0):
        """Check nested levels with recursion."""
        if level > 1:
            raise ValueError('Run settings must be at most 2D')
        for item in list_:
            if isinstance(item, list):
                self._check_nested_levels(item, level + 1)

    def _flatten_list(self, list_):
        """Flatten a nested list."""
        if not isinstance(list_, list):
            return [list_]
        else:
            return [item
                    for sublist in list_
                    for item in self._flatten_list(sublist)]

    def add_databases(self, jobs, **kwargs) -> dict:
        """Get databases."""
        for job in jobs.values():
            if job.get('database', None) is not None:
                continue
            db = list(set([rs.database for rs in job['job'].tasks]))
            if len(db) > 1:
                self.logger.error('All tasks in a job must have the same ' +
                                  'database')
            job['database'] = (get_database(db[0], logger=self.logger)
                               if db[0]
                               else kwargs.get('database', DatabaseDummy()))
        return jobs

    def add_schedulers(self, jobs):
        """Add schedulers."""
        connections = self.get_connections(jobs)
        for i, job in jobs.items():
            if job.get('scheduler', None):
                continue
            s = list(set([rs.scheduler for rs in job['job'].tasks]))
            if len(s) > 1:
                self.logger.error('All tasks in a job must have the same ' +
                                  'scheduler')
            job['scheduler'] = get_scheduler(s[0],
                                             connection=connections[i],
                                             logger=self.logger)
        return jobs

    def _flatten_2d_list(self, list_):
        """Flatten a 2D list."""
        if any(isinstance(item, list) for item in list_):
            return [item for sublist in list_ for item in sublist]
        return list_

    def change_key(self, dict_obj, old_key, new_key):
        """Change key."""
        dict_obj[new_key] = dict_obj.pop(old_key)

    def _check_job_params(self, jobs) -> dict:
        """Check job parameters."""
        new_jobs: dict = {}
        i = 0
        for job in jobs.values():
            new_job = job['scheduler'].check_job_params(job['job'])
            if not isinstance(new_job, list):
                new_job = [new_job]
            for j in new_job:
                new_jobs[i] = {}
                new_jobs[i]['job'] = j
                new_jobs[i]['scheduler'] = job['scheduler']
                new_jobs[i]['database'] = job['database']
                i += 1
        return new_jobs
    
    def _sort_jobs(self, jobs, key):
        """Sort jobs."""
        return {i: j for i, j in sorted(jobs.items(), key=lambda x: x[1][key])}
    
    def _sort_jobs_by_scheduler(self, jobs):
        """Sort jobs by scheduler."""
        return self._sort_jobs(jobs, 'scheduler')
    
    # sort the sorted jobs by database
    def _sort_jobs_by_database(self, jobs):
        """Sort jobs by database."""
        return self._sort_jobs(jobs, 'database')
    
