import unittest
from dataclasses import dataclass
from unittest.mock import Mock

from hytools.logger import LoggerDummy

from hyrun.runner import ArrayJob


@dataclass
class RS:
    """Run settings."""

    run_settings: list


class TestArrayJob(unittest.TestCase):
    """Test array job."""

    def setUp(self):
        """Set up."""
        self.array_job = ArrayJob(run_settings=[], logger=LoggerDummy())
        self.mock_scheduler_local = Mock()
        self.mock_scheduler_local.__class__.__name__ = 'LocalScheduler'
        self.mock_scheduler_remote = Mock()
        self.mock_scheduler_remote.__class__.__name__ = 'SlurmScheduler'

    def test_get_settings_1d(self):
        """Test that 1D run_settings are correctly flattened."""
        run_settings = [RS(1), RS(2), RS(3)]
        result = self.array_job.get_settings(run_settings,
                                             self.mock_scheduler_local)
        self.assertEqual(result, [[RS(1)], [RS(2)], [RS(3)]])
        result = self.array_job.get_settings(run_settings,
                                             self.mock_scheduler_remote)
        self.assertEqual(result, [[RS(1), RS(2), RS(3)]])

    def test_get_settings_2d(self):
        """Test that 2D run_settings are correctly flattened."""
        run_settings = [RS(1), [RS(2), RS(3)], [RS(4), RS(5), RS(6)]]
        result = self.array_job.get_settings(run_settings,
                                             self.mock_scheduler_local)
        self.assertEqual(result, [[RS(1)], [RS(2)], [RS(3)], [RS(4)], [RS(5)],
                                  [RS(6)]])
        result = self.array_job.get_settings(run_settings,
                                             self.mock_scheduler_remote)
        self.assertEqual(result, [[RS(1)], [RS(2), RS(3)],
                                  [RS(4), RS(5), RS(6)]])

    def test_get_settings_3d(self):
        """Test that ValueError is raised when run_settings is 3D."""
        run_settings = [RS(1), [RS(2), [RS(3), RS(4)], [RS(5), RS(6)]]]
        with (ValueError):
            self.array_job.get_settings(run_settings,
                                        self.mock_scheduler_local)
        with self.assertRaises(ValueError):
            self.array_job.get_settings(run_settings,
                                        self.mock_scheduler_remote)


if __name__ == '__main__':
    unittest.main()
