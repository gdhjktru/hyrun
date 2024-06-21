from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Optional


class Scheduler(ABC):
    """Base class for remote Schedulers."""

    @abstractmethod
    def cancel(self):
        """Cancel job."""

    @abstractmethod
    def is_finished(self, *args, **kwargs):
        """Check if job finishes."""

    # @abstractmethod
    # def fetch_results(self, *args, **kwargs):
    #     """Fetch results."""

    @contextmanager
    @abstractmethod
    def run_ctx(self, arg: Optional[Any] = None):
        """Run context manager."""

    @abstractmethod
    def teardown(self):
        """Teardown job."""
