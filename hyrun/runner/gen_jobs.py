import json

from hydb import Database, DatabaseDummy  # noqa: F401
from hytools.logger import LoggerDummy

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


class ArrayJob:
    """Array job tools."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.logger = kwargs.get('logger', LoggerDummy())

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
        db = (Database(kwargs['database'], logger=self.logger)
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
            job['database'] = (Database(db[0], logger=self.logger)
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
