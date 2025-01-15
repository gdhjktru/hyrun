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

    @abstractmethod
    def __enter__(self):
        """Enter."""
        pass

    @abstractmethod
    def __exit__(self, *args):
        """Exit."""
        pass

    @abstractmethod
    def cancel(self):
        """Cancel job."""

    @abstractmethod
    def is_finished(self, *args, **kwargs):
        """Check if job finishes."""

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
