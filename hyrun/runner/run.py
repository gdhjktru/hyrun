
from .runner import Runner


def run(*args, **kwargs):
    """Run."""
    return Runner(*args, **kwargs).run()

async def arun(*args, **kwargs):
    """Run."""
    return await Runner(*args, **kwargs).arun()
