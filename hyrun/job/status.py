from dataclasses import dataclass


@dataclass
class JobStatus:

    """Dataclass containing information about the job status."""


    job_id: str
    job_name: str
    submission_time: str
    state: str
    quality_of_service: str
    elapsed_time: str
    time_left: str