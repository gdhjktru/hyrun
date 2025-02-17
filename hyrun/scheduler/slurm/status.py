from hytools.connection import Connection
from hyrun.job import Job, JobMetadata
from hytools.logger import Logger
from hytools.time import slurmtime_to_timedelta
from hytools.memory import get_memory
from time import sleep
from typing import Optional, List



cmd_map = {'get_status': 'sacct -j {job_scheduler_id} --units=K -p -o JobId%40,State%20,Submit,Start,End,Elapsed,TimeLimit,CPUTime,MaxRSS,MaxVMSize,MaxDiskRead,MaxDiskWrite,ReqCPUS,AllocCPUS,AllocNodes,ReqMem,NodeList'}


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
                                                     nsteps = len(job.tasks),
                                                     job_id = job.scheduler_id)
    job.status = job_status
    job.metadata = task_data
    print('job_status', job_status)
    print('stepdata', task_data)

    ijioijioio
    return Job


class SlurmStatus:

    @classmethod
    def parse_output(cls,
                     output: Optional[str] = None,
                     nsteps: Optional[int] = None,
                     logger: Optional[Logger] = None,
                     job_id: Optional[int] = None) -> List[dict]:
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
                    data = line.split('|')
                    step = {
                        'submit': data[2],
                        'start': data[3],
                        'end': data[4],
                        'elapsed': slurmtime_to_timedelta(data[5]),
                        'time_limit': time_limit,
                        'cpu_time': slurmtime_to_timedelta(data[7]),
                        'max_rss': get_memory(data[8]),
                        'max_vm_size': get_memory(data[9]),
                        'max_disk_read': get_memory(data[10]),
                        'max_disk_write': get_memory(data[11]),
                        'req_cpus': data[12],
                        'alloc_cpus': data[13],
                        'alloc_nodes': data[14],
                        'req_mem': req_mem,
                        'node_list': data[16]
                    }
                    step_data.append(JobMetadata(**step))
                    break
    
        return job_status, step_data