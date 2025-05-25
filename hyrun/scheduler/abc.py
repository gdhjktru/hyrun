from abc import ABC, abstractmethod

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

    # @abstractmethod
    # def __enter__(self):
    #     """Enter."""
    #     pass

    # @abstractmethod
    # def __exit__(self, *args):
    #     """Exit."""
    #     pass

    @abstractmethod
    def get_cancel_cmd(self):
        """Cancel job."""

    # @abstractmethod
    # def is_finished(self, *args, **kwargs):
    #     """Check if job finishes."""

    @abstractmethod
    def get_summary_cmd(self, *args, **kwargs):
        """Get job summary."""
        pass

    @abstractmethod
    def get_submit_cmd(self, *args, **kwargs):
        """Submit job."""
        pass

    @abstractmethod
    def get_job_script(self, *args, **kwargs):
        """Get job script."""
        pass


    @abstractmethod
    def get_status_cmd(self, *args, **kwargs):
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
