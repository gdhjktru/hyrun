from abc import ABC, abstractmethod

# from contextlib import contextmanager
# from typing import Any, Optional


class Scheduler(ABC):
    """Base class for Schedulers."""

    @abstractmethod
    def __hash__(self) -> int:
        """Hash."""

    @abstractmethod
    def __eq__(self, value: object) -> bool:
        """Equal."""
        pass

    @abstractmethod
    def cancel(self):
        """Cancel job."""

    @abstractmethod
    def is_finished(self, *args, **kwargs):
        """Check if job finishes."""

    @abstractmethod
    def job_script(self, *args, **kwargs):
        """Generate job script."""

    @abstractmethod
    def submit(self, *args, **kwargs):
        """Generate command for job submission."""

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
