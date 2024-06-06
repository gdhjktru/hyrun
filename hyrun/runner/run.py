from .array_job import gen_jobs  # noqa
from .runner import Runner


def run(*args, **kwargs):
    """Run."""
    return Runner(*kwargs).run(*args, **kwargs)


def rerun(*args, **kwargs):
    """Run."""
    return Runner(*args, rerun=True, **kwargs).run()


def get_status(*args, **kwargs):
    """Get status."""
    pass


def get_results(*args, **kwargs):
    """Get results."""
    pass


async def arun(*args, **kwargs):
    """Run."""
    return await Runner(*args, **kwargs).arun()
