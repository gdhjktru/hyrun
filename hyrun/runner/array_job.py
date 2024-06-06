import itertools
from dataclasses import replace
from functools import singledispatch

from hydb import Database, DatabaseDummy  # noqa: F401

from hyrun.decorators import force_list
from hyrun.job import Job  # noqa: F401


def _check_nested_levels(list_: list):
    """Check nested levels."""
    for rs in list_:
        if isinstance(rs, list):
            for item in rs:
                if isinstance(item, list):
                    raise ValueError('Run settings must be at most 2D')


def _flatten_list(list_: list) -> list:
    """Flatten any list."""
    if not isinstance(list_, list):
        return [list_]
    while any(isinstance(item, list) for item in list_):
        list_ = [item if isinstance(item, list)
                 else [item] for item in list_]
        return list(itertools.chain.from_iterable(list_))
    return list_ if isinstance(list_, list) else [list_]


def _get_databases(dim=1, **kwargs) -> list[Database]:
    """Get databases."""
    db = kwargs.pop('database', [])
    if not db:
        return [DatabaseDummy() for _ in range(dim)]
    if not isinstance(db, list):
        return [Database(db) for _ in range(dim)]
    return [Database(d) for d in db]


@singledispatch
def _get_job(arg, db) -> Job:
    """Get job."""
    return Job(tasks=arg)


@_get_job.register(int)
def _(arg, db) -> Job:
    d = db.get(key='db_id', value=int(arg))
    d['db_id'] = int(arg)
    d['database'] = db.name
    return Job(**d)


def _check_schedulers(job):
    schedulers = [rs.scheduler for rs in job.tasks]
    if len(set(schedulers)) > 1:
        raise ValueError('All run settings in a job must have the ' +
                         'same scheduler')
    return job.tasks[0].scheduler


def gen_jobs(arg, **kwargs) -> list[Job]:
    """Generate jobs."""
    if not isinstance(arg, list):
        arg = [[arg]]
    _check_nested_levels(arg)
    if not any(isinstance(item, list) for item in arg):
        arg = [arg]
    arg = [_flatten_list(a) for a in arg]
    databases = _get_databases(dim=len(arg), **kwargs)
    jobs = [_get_job(a, d) for a, d in zip(arg, databases)]
    return [replace(job, scheduler=_check_schedulers(job)) for job in jobs]


@force_list
def update_jobs(jobs, **kwargs) -> list[Job]:
    """Update jobs."""
    return [replace(job, **kwargs) for job in jobs]

# class ArrayJob:
#     """Array job."""

#     def __init__(self,
#                  run_settings,
#                  **kwargs):
#         """Initialize."""
#         self.run_array = self.get_settings(run_settings)
#         # self.schedulers = self.get_schedulers(self.run_array)
#         # self.common_keys = self.check_common_keys(**kwargs)

#     def flatten_any_list(self, ll):
#         """Flatten any list."""
#         if not isinstance(ll, list):
#             return [ll]
#         while any(isinstance(item, list) for item in ll):
#             ll = [item if isinstance(item, list) else [item] for item in ll]
#             return list(itertools.chain.from_iterable(ll))
#         return ll if isinstance(ll, list) else [ll]

#     def _check_nested_levels(self, run_settings):
#         """Check nested levels."""
#         for rs in run_settings:
#             if isinstance(rs, list):
#                 for item in rs:
#                     if isinstance(item, list):
#                         raise ValueError('Run settings must be at most 2D')

#     # def get_schedulers(self, run_array):
#     #     """Get schedulers."""
#     #     if not run_array or run_array == [[]]:
#     #         return []
#     #     schedulers = []
#     #     for job in run_array:
#     #         if not all(rs.scheduler == job[0].scheduler for rs in job):
#     #             raise ValueError('All run settings in a
# job must have the ' +
#     #                              'same scheduler')
#     #         schedulers.append(job[0].scheduler)
#     #     return schedulers

#     def get_settings(self, run_settings) -> list:
#         """Get settings."""
#         if not isinstance(run_settings, list):
#             return [[run_settings]]
#         self._check_nested_levels(run_settings)
#         if not any(isinstance(item, list) for item in run_settings):
#             return [run_settings]
#         return [self.flatten_any_list(rs) for rs in run_settings]

#         # if 'local' in scheduler.__class__.__name__.lower():
#         #     return [[item] for item in self.flatten_any_list(run_settings)]
#         # else:
#         #     if not any(isinstance(item, list) for item in run_settings):
#         #         return [run_settings]
#         #     return [self.flatten_any_list(item) for item in run_settings]

#     # def check_common_keys(self, keys: Optional[list] =
# None, **kwargs) -> bool:
#     #     """Prepare array job."""
#     #     keys = keys or []
#     #     return all(
#     #         all(getattr(rrs, k, None) == getattr(rs[0], k,
#  None) for rrs in rs)
#     #         for k in keys for rs in self.run_settings
#     #     )

# if __name__ == '__main__':
#     from dataclasses import dataclass
#     @dataclass
#     class RS:
#         """Run settings."""

#         run_settings: list

#     print(gen_jobs(RS(1)))
#     print(gen_jobs([RS(1), RS(2), RS(3)]))
#     print(gen_jobs([RS(1), [RS(2), RS(3)], [RS(4), RS(5), RS(6)]]))
