from dataclasses import dataclass


job_status_map = {'UNKNOWN': 0, 'PENDING': 10, 'RUNNING': 20, 'COMPLETED' : 30,
                'BOOT_FAIL': 40, 'CANCELLED': 40, 'DEADLINE': 40, 'FAILED': 40,
                'NODE_FAIL': 40, 'OUT_OF_MEMORY': 40, 'PREEMPTED': 40,
                'TIMEOUT': 40}
    
@dataclass
class JobStatus:
    """Dataclass containing information about the job status."""

    job_id: str
    job_name: str
    submission_time: str
    status: str
    quality_of_service: str
    elapsed_time: str
    time_left: str

