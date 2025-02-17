from time import sleep
from typing import List, Optional, Tuple

from hytools.connection import Connection
from hytools.logger import Logger
from hytools.memory import get_memory
from hytools.time import slurmtime_to_timedelta

from hyrun.job import Job, JobMetaData

cmd_map = {
    'get_status': (
        'sacct -j {job_scheduler_id} --units=K -p -o '
        'JobId%20,State%20,Submit,Start,End,Elapsed,TimeLimit,CPUTime,'
        'MaxRSS,MaxVMSize,MaxDiskRead,MaxDiskWrite,ReqCPUS,AllocCPUS,'
        'AllocNodes,ReqMem,NodeList'
    )
}


def get_status(job: Job,
               connection: Optional[Connection] = None,
               logger: Optional[Logger] = None,
               **kwargs) -> Job:
    """Get status."""
    if not connection:
        raise ValueError('Connection is required')
    max_attempts = kwargs.get('max_attempts', 5)
    sleep_seconds = kwargs.get('sleep_seconds', 3)

    cmd = cmd_map['get_status'].format(job_scheduler_id=job.scheduler_id)
    attempts = 0
    while attempts < max_attempts:
        result = connection.execute(cmd)  # type: ignore
        if f'{job.scheduler_id}' in result.stdout:
            break
        logger.debug(f'Job {job.scheduler_id} not found in {result.stdout}, ' +
                     f'retrying... {attempts + 1}/{max_attempts}')
        sleep(sleep_seconds)
        attempts += 1
    job_status, task_data = SlurmStatus.parse_output(result.stdout,
                                                     logger=logger,
                                                     nsteps=len(job.tasks),
                                                     job_id=job.scheduler_id)
    job.status = job_status
    job.metadata = task_data
    return job


class SlurmStatus:
    """Slurm status parser."""

    @classmethod
    def parse_output(cls,
                     output: Optional[str] = None,
                     nsteps: Optional[int] = None,
                     logger: Optional[Logger] = None,
                     job_id: Optional[int] = None) -> Tuple[str, list]:
        """Parse output."""
        if not output or not job_id:
            return 'UNKNOWN', []

        nsteps = nsteps or 1
        lines = output.split('\n')
        step_data = []
        job_status = 'UNKNOWN'
        time_limit = None
        req_mem = None

        for line in lines:
            if f'{job_id}|' in line:
                logger.debug(f'Parsing job line: {line}')
                job_status = line.split('|')[1]
                try:
                    time_limit = slurmtime_to_timedelta(line.split('|')[6])
                except ValueError:
                    time_limit = None
                req_mem = get_memory(line.split('|')[15])
                break

        for i in range(nsteps):
            for line in lines:
                if f'{job_id}.{i}' in line:
                    logger.debug(f'Parsing job step line: {line}')
                    metadata = cls.parse_job_data(line.split('|'))
                    metadata.time_limit = time_limit
                    metadata.req_mem = req_mem
                    step_data.append(metadata)
                    break
        return job_status, step_data

    @classmethod
    def parse_job_data(cls, data: List[str]) -> JobMetaData:
        """Parse job data."""
        step_fields = [
            'submit', 'start', 'end', 'elapsed', 'cpu_time',
            'max_rss', 'max_vm_size', 'max_disk_read', 'max_disk_write',
            'req_cpus', 'alloc_cpus', 'alloc_nodes', 'node_list'
        ]
        step_values = [
            data[2], data[3], data[4], slurmtime_to_timedelta(data[5]),
            slurmtime_to_timedelta(data[7]), get_memory(data[8]),
            get_memory(data[9]), get_memory(data[10]), get_memory(data[11]),
            data[12], data[13], data[14], data[16]
        ]
        step = dict(zip(step_fields, step_values))
        return JobMetaData(**step)
