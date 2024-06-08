import itertools
from dataclasses import replace
from typing import List

from hydb import Database, DatabaseDummy  # noqa: F401

from hyrun.decorators import force_list
from hyrun.job import Job  # noqa: F401
from hyrun.scheduler import get_scheduler


def gen_jobs(arg, **kwargs) -> list[Job]:
    """Generate jobs."""
    if isinstance(arg, list):
        if all(isinstance(item, Job) for item in arg):
            return arg
    aj = ArrayJob()
    # case only one job/task
    if not isinstance(arg, list):
        arg = [[arg]]
    # dont allow more than 2D list
    aj._check_nested_levels(arg)
    # case 1d list
    if not any(isinstance(item, list) for item in arg):
        arg = [arg]
    # make sure arg is a list of lists
    arg = [aj._flatten_list(a) for a in arg]
    # allow for multiple databases
    databases = aj._get_databases(dim=len(arg), **kwargs)
    # generate job
    jobs = [aj._get_job(a, d) for a, d in zip(arg, databases)]
    connections = aj.extract_connections(jobs)
    # extract and convert scheduler
    jobs = [replace(job,
                    scheduler=get_scheduler(aj._check_schedulers(job),
                                            connection=c))
            for job, c in zip(jobs, connections)]
    # check that some parameters are identical among tasks
    jobs = aj._check_job_params(jobs)
    return jobs


@force_list
def update_jobs(jobs, **kwargs) -> list[Job]:
    """Update jobs."""
    return [replace(job, **kwargs) for job in jobs]


class ArrayJob:
    """Array job tools."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        pass

    def extract_connections(self, jobs):
        """Extract connections."""
        connections = []
        for job in jobs:
            if hasattr(job.tasks[0], 'connection'):
                connections.append(job.tasks[0].connection)
            elif hasattr(job.tasks[0], 'extract_connection'):
                connections.append(job.tasks[0].extract_connection())
            else:
                connections.append(None)
        return connections

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

    def _get_databases(self, dim=1, **kwargs) -> list[Database]:
        """Get databases."""
        db = kwargs.pop('database', [])
        if not db:
            return [DatabaseDummy() for _ in range(dim)]
        if not isinstance(db, list):
            return [Database(db) for _ in range(dim)]
        return [Database(d) for d in db]

    def _get_job(self, arg: list, db) -> Job:
        """Get job."""
        if any(isinstance(item, int) for item in arg):
            if len(arg) > 1:
                raise ValueError('Only one db_id per job allowed')
            return self._get_job_from_db(arg[0], db)
        return self._get_job_from_rs(arg, db)

    def _get_job_from_rs(self, arg, db) -> Job:
        """Get job."""
        db = [Database(t.database).name for t in arg]
        if len(set(db)) > 1:
            raise ValueError('All run settings in a job must have the ' +
                             'same database')
        return Job(tasks=arg, database=Database(db[0]))

    def _get_job_from_db(self, db_id, db) -> Job:
        """Get job."""
        d = Database(db).get(key='db_id', value=db_id)
        d['db_id'] = db_id
        d['database'] = db.name
        return Job(**d)

    def _check_schedulers(self, job):
        schedulers = [rs.scheduler for rs in job.tasks]
        if len(set(schedulers)) > 1:
            raise ValueError('All run settings in a job must have the ' +
                             'same scheduler')
        return job.tasks[0].scheduler

    def _flatten_2d_list(self, list_):
        """Flatten a 2D list."""
        if any(isinstance(item, list) for item in list_):
            return [item for sublist in list_ for item in sublist]
        return list_

    def _check_job_params(self, jobs) -> List[Job]:
        """Check job parameters."""
        # flatten because local might return a list of jobs
        return self._flatten_2d_list(
            [job.scheduler.check_job_params(job) for job in jobs])
