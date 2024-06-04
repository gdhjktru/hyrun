import unittest
from unittest.mock import Mock
from hyrun.runner import ArrayJob
from hytools.logger import LoggerDummy
from dataclasses import dataclass

@dataclass
class RS:
    run_settings: list

class TestArrayJob(unittest.TestCase):
    def setUp(self):
        self.array_job = ArrayJob(run_settings=[], logger=LoggerDummy())
        self.mock_scheduler_local = Mock()
        self.mock_scheduler_local.__class__.__name__ = 'LocalScheduler'
        self.mock_scheduler_remote = Mock()
        self.mock_scheduler_remote.__class__.__name__ = 'SlurmScheduler'


    def test_get_settings_1d(self):
        run_settings = [RS(1), RS(2), RS(3)]
        result = self.array_job.get_settings(run_settings, self.mock_scheduler_local)
        self.assertEqual(result, [[RS(1)], [RS(2)], [RS(3)]])
        result = self.array_job.get_settings(run_settings, self.mock_scheduler_remote)
        self.assertEqual(result, [[RS(1), RS(2), RS(3)]])

    def test_get_settings_2d(self):
        run_settings = [RS(1), [RS(2), RS(3)], [RS(4), RS(5), RS(6)]]
        result = self.array_job.get_settings(run_settings, self.mock_scheduler_local)
        self.assertEqual(result, [[RS(1)], [RS(2)], [RS(3)], [RS(4)], [RS(5)], [RS(6)]])
        result = self.array_job.get_settings(run_settings, self.mock_scheduler_remote)
        self.assertEqual(result, [[RS(1)], [RS(2), RS(3)], [RS(4), RS(5), RS(6)]])

    def test_get_settings_3d(self):
        run_settings = [RS(1), [RS(2), [RS(3), RS(4)], [RS(5), RS(6)]]]
        with self.assertRaises(ValueError):
            self.array_job.get_settings(run_settings, self.mock_scheduler_local)
        with self.assertRaises(ValueError):
            self.array_job.get_settings(run_settings, self.mock_scheduler_remote)
    # def test_get_settings_2d(self):
    #     run_settings = [['setting1', 'setting2'], ['setting3', 'setting4']]
    #     result = self.array_job.get_settings(run_settings, 'local')
    #     self.assertEqual(result, [['setting1'], ['setting2'], ['setting3', 'setting4']])

    # def test_get_settings_3d(self):
    #     run_settings = [[['setting1', 'setting2'], ['setting3', 'setting4']], [['setting5', 'setting6']]]
    #     result = self.array_job.get_settings(run_settings, 'local')
    #     self.assertEqual(result, [['setting1', 'setting2'], ['setting3', 'setting4'], ['setting5', 'setting6']])

if __name__ == '__main__':
    unittest.main()
