import itertools
import json
from dataclasses import replace
from typing import List

from hydb import Database, DatabaseDummy  # noqa: F401
from hytools.logger import LoggerDummy

from hyrun.decorators import force_list
from hyrun.job import Job, Output  # noqa: F401
from hyrun.scheduler import get_scheduler


def gen_jobs(arg, **kwargs) -> dict:
    """Generate jobs."""
    aj = ArrayJob()
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


# @force_list
# def update_jobs(jobs, **kwargs) -> list[Job]:
#     """Update jobs."""
#     return [replace(job, **kwargs) for job in jobs]


class ArrayJob:
    """Array job tools."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', LoggerDummy())

    def set_jobs(self, tasks, **kwargs):
        """Check tasks."""
        jobs = {i: {} for i in range(len(tasks))}
        for i, t in enumerate(tasks):
            if len(t) == 1:
                if isinstance(t[0], int):
                    jobs[i] = self.resolve_db_id(t[0], **kwargs)
                elif isinstance(t[0], Job):
                    jobs[i]['job'] = {'job': t[0]}
                else:
                    outputs = [Output().from_dict(rs.__dict__) for rs in t]
                    jobs[i] = {'job': Job(tasks=t, outputs=outputs)}
        return jobs

    def resolve_db_id(self, db_id, **kwargs):
        """Resolve db_id."""
        db = (Database(kwargs['database'])
              if kwargs.get('database')
              else DatabaseDummy())
        entry = db.get(key='db_id', value=db_id)
        return {'job': Job(**entry, db_id=db_id, database=db.name),
                'database': db,
                'db_id': db_id,}

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
            if len(set(json.dumps(d, sort_keys=True) for d in connections)) > 1:
                self.logger.error('All tasks in a job must have the same ' +
                                    'connection')
            c.append(connections[0])
        return c

    def _check_nested_levels(self, list_: list):
        """Check nested levels."""
        for rs in list_:
            if isinstance(rs, list):
                for item in rs:
                    if isinstance(item, list):
                        raise ValueError('Run settings must be at most 2D')

    def _flatten_list(self, list_: list) -> list:
        """Flatten any list."""
        if not isinstance(list_, list):
            return [list_]
        while any(isinstance(item, list) for item in list_):
            list_ = [item if isinstance(item, list)
                     else [item] for item in list_]
            return list(itertools.chain.from_iterable(list_))
        return list_ if isinstance(list_, list) else [list_]

    def add_databases(self, jobs, **kwargs) -> dict:
        """Get databases."""
        for job in jobs.values():
            if job.get('database', None):
                continue
            db = list(set([rs.database for rs in job['job'].tasks]))
            if len(db) > 1:
                self.logger.error('All tasks in a job must have the same ' +
                                  'database')
            job['database'] = (Database(db[0])
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
                                             connection=connections[i])
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
        # extract all scheduler names
        # scheduler_names = set([job['scheduler'].name for job in jobs.values()])
        # # extract all jobs with same scheduler.name:
        # jobs_grouped = {name: {k: v for k, v in jobs.items()
        #                        if v['scheduler'].name == name}
        #                 for name in scheduler_names}
        # # check that some parameters are identical among tasks
        # k=0
        # for name, jobs in jobs_grouped.items():
        #     new_jobs = jobs['scheduler'].check_job_params(jobs)
        #     # len(new_jobs) might be bigger than len(jobs) if some jobs
        #     # were separated. therefore, re-assign the keys, starting from 0
        #     for i, job in new_jobs.items():
        #         self.change_key(jobs, i, k)
        #         k += 1
        new_jobs = {}
        i = 0
        for job in jobs.values():
            new_job = job['scheduler'].check_job_params(job)
            if not isinstance(new_job, list):
                new_job = [new_job]
            for j in new_job:
                new_jobs[i] = j
                i += 1
        return new_jobs





        return jobs






        # jobs_flattened = self._flatten_2d_list(
        #     [job['scheduler'].check_job_params(job['job']) for job in jobs.values()])

        # for job in jobs.values:
        #     jobs_flattened
        # return self._flatten_2d_list(
        #     [job['scheduler'].check_job_params(job['job']) for job in jobs.values()])
