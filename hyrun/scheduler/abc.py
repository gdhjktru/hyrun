from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Optional


class Scheduler(ABC):
    """Base class for remote Schedulers."""

    @abstractmethod
    def cancel(self):
        """Cancel job."""

    @abstractmethod
    def run(self):
        """Run job."""

    @abstractmethod
    def quick_return(self):
        """Quick return job."""

    @abstractmethod
    def fetch_results(self):
        """Fetch results."""

    @contextmanager
    @abstractmethod
    def run_ctx(self, arg: Optional[Any] = None):
        """Run context manager."""

    @abstractmethod
    def teardown(self):
        """Teardown job."""
