from abc import ABC, abstractmethod

from hashlib import sha256
from string import Template
from hytools.file import File
# from contextlib import contextmanager
# from typing import Any, Optional


class Scheduler(ABC):
    """Base class for remote Schedulers."""

    @abstractmethod
    def __hash__(self) -> int:
        """Hash."""

    @abstractmethod
    def __eq__(self, value: object) -> bool:
        """Equal."""
        pass

    def set_job_script(self, job, **kwargs):
        """Get job script."""
        job_script_content = self.get_job_script(job, **kwargs)
        job_hash = sha256(job_script_content.encode()).hexdigest()
        job_script_content = job_script_content.replace('__JOB_HASH__',
                                                        job_hash)
        job_script_name = f'job_script_{job_hash}.sh'
        job_script = File(name=job_script_name,
                          content=job_script_content)
        job.job_script = job_script
        job.job_hash = job_hash
        return job
    
    @abstractmethod
    def get_submit_cmd(self, job, **kwargs) -> str:
        """Get command to submit job."""
        pass

    def parse_submit_output(self, output: str) -> int:
        """Parse output of job submission."""
        pass

    def parse_status_output(self, output: str) -> str:
        """Parse output of job status."""
        pass


    def get_status(self, job, connection=None, **kwargs) -> str:
        """Get job status."""
        cmd = self.get_status_cmd(job, **kwargs)
        if connection is None:
            return cmd
        output = connection.execute(cmd)
        if output.returncode != 0:
            raise RuntimeError(f'Failed to get job status: {output.stderr}')
        return self.parse_status_output(output.stdout)
    
    def submit(self, job, connection=None, **kwargs):
        """Submit job."""
        cmd = self.get_submit_cmd(job, **kwargs)
        if connection is None:
            return cmd
        output = connection.execute(cmd)
        if output.returncode != 0:
            raise RuntimeError(f'Job submission failed: {output.stderr}')
        return self.parse_submit_output(output.stdout))



    @abstractmethod
    def get_job_script(self, job, **kwargs) -> str:
        """Cancel job."""



    @abstractmethod
    def get_cancel_cmd(self):
        """Cancel job."""

    @abstractmethod
    def get_summary_cmd(self, job, **kwargs):
        """Get job summary."""
        pass

    @abstractmethod
    def get_submit_cmd(self, job, **kwargs):
        """Submit job."""
        pass

    @abstractmethod
    def get_status_cmd(self, job, **kwargs):
        """Get job status."""
        pass

    # @abstractmethod
    # def fetch_results(self, *args, **kwargs):
    #     """Fetch results."""

    # @contextmanager
    # @abstractmethod
    # def run_ctx(self, arg: Optional[Any] = None):
    #     """Run context manager."""

    @abstractmethod
    def teardown(self):
        """Teardown job."""
