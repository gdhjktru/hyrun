from pathlib import Path
from typing import Any

from .job import get_job
from .graph import JobGraph


def get_workflow(*args, **kwargs) -> JobGraph:
    """Create a JobGraph from the provided jobs.

    This function accepts a variable number of job definitions and returns
    a JobGraph object containing those jobs.

    Parameters
    ----------
        *args: Variable number of job definitions.
        **kwargs: Additional keyword arguments to pass to JobGraph.

    Returns
    -------
        JobGraph: An instance of JobGraph containing the provided jobs.

    Raises
    ------
        ValueError: If no arguments are provided.

    """
    if len(args) == 0:
        raise ValueError('No arguments provided')

    if isinstance(args[0], JobGraph):
        # If the first argument is already a JobGraph, return it directly
        return args[0]

    if isinstance(args[0], (str, Path)):
        try:
            if Path(args[0]).exists():
                workflow = JobGraph()
                workflow.read(filename=args[0])
                return workflow
        except Exception as e:
            raise ValueError(f'Error reading workflow from {args[0]}: {e}')

    jobs = normalize_input(*args)
    return JobGraph(jobs=jobs, **kwargs)


def normalize_input(jobs: Any) -> list:
    """Normalize input to a list of lists."""
    jobs = [jobs] if not isinstance(jobs, list) else jobs
    return [get_job(job) for job in jobs]
