from .runner import Runner


def run(*args, **kwargs):
    """Run."""

    # # return Runner(*args, **kwargs).run(*args, **kwargs)
    # jobs = gen_jobs(*args, logger=self.logger, **kwargs)
    #     # check if jobs has an id
    # jobs = self.check_finished_jobs(jobs)
    # jobs = {i: j for i, j in jobs.items() if not j['job'].finished}
    #         schedulers = list(set([j['scheduler'] for j in jobs.values()]))

    #     databases = list(set([j['scheduler'] for j in jobs.values()]))
    # jobs = sort_jobs_by('scheduler', *args, **kwargs)
    # wait, dryrun, rerun


# def rerun(*args, **kwargs):
#     """Run."""
#     return Runner(*args, rerun=True, **kwargs).run()


def get_status(*args, **kwargs):
    """Get status."""
    return Runner(*args, **kwargs).get_status(*args, **kwargs)


def fetch_results(*args, **kwargs):
    """Get results."""
    return Runner(*args, **kwargs).fetch_results(*args, **kwargs)


async def arun(*args, **kwargs):
    """Run."""
    return run(*args, wait=True, **kwargs)
