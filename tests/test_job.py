import unittest
from dataclasses import dataclass

from hyrun.job.job import Job, check_common_dataclass, get_job


@dataclass
class DummyDatabase:
    """Dummy database class for testing."""

    name: str


@dataclass
class DummyScheduler:
    """Dummy scheduler class for testing."""

    name: str


@dataclass
class DummyTask:
    """Dummy task class for testing."""

    database: dict
    scheduler: dict
    connection: dict


class TestJob(unittest.TestCase):
    """Test Job and JobInfo classes."""

    def test_job_from_dict(self):
        """Test that get_job returns a Job from a dictionary."""
        d = {'job_id': 1, 'status': 'pending', 'hash': 'abc'}
        job = get_job(d)
        self.assertIsInstance(job, Job)
        self.assertEqual(job.job_id, 1)
        self.assertEqual(job.status, 'pending')
        self.assertEqual(job.hash, 'abc')

    def test_job_from_list(self):
        """Test that get_job returns a Job from a list."""
        tasks = [{'database': {'name': 'db'}, 'scheduler': {'name': 'slurm'},
                  'connection': {}}]
        job = get_job(tasks)
        self.assertIsInstance(job, Job)
        self.assertEqual(len(job.tasks), 1)
        self.assertEqual(job.tasks[0]['database']['name'], 'db')

    def test_job_from_job(self):
        """Test that get_job returns the same job."""
        job1 = Job(job_id=2, status='done')
        job2 = get_job(job1)
        self.assertIs(job1, job2)

    def test_check_common_dataclass_success(self):
        """Test that check_common_dataclass works correctly."""
        tasks = [
            DummyTask(database={'name': 'db'}, scheduler={'name': 'slurm'},
                      connection={}),
            DummyTask(database={'name': 'db'}, scheduler={'name': 'slurm'},
                      connection={})
            ]
        # Should not raise
        check_common_dataclass(tasks, keys=['database', 'scheduler',
                                            'connection'])

    def test_check_common_dataclass_fail(self):
        """Test that check_common_dataclass raises ValueError."""
        tasks = [
            DummyTask(database={'name': 'db1'}, scheduler={'name': 'slurm'},
                      connection={}),
            DummyTask(database={'name': 'db2'}, scheduler={'name': 'slurm'},
                      connection={})
            ]
        with self.assertRaises(ValueError):
            check_common_dataclass(tasks, keys=['database'])

    def test_job_hash_set(self):
        """Test that the job hash is set correctly."""
        job = Job(job_script='echo hello')
        # hash should be set in __post_init__ if job_script is present
        self.assertIsNotNone(job.hash)


if __name__ == '__main__':
    unittest.main()

# import unittest
# from dataclasses import asdict

# from hyrun.job import Job, JobInfo, Output


# class TestJob(unittest.TestCase):
#     """Test Job and JobInfo classes."""

#     def setUp(self):
#         """Set up."""
#         self.job_info = JobInfo(id=1, name='test_job')
#         self.outputs = [Output(output_file='/path/to/output')]
#         self.job = Job(
#             id=self.job_info.id,
#             name=self.job_info.name,
#             tasks=['task1', 'task2'],
#             outputs=self.outputs,
#             job_script='script.sh',
#             database='test_db',
#             scheduler='test_scheduler',
#             db_id=123,
#             database_opt={'opt1': 'value1'},
#             scheduler_opt={'opt2': 'value2'}
#             )

#     def test_job_info_initialization(self):
#         """Test that the JobInfo is initialized correctly."""
#         self.assertEqual(self.job_info.id, 1)
#         self.assertEqual(self.job_info.name, 'test_job')
#         self.assertFalse(self.job_info.finished)
#         self.assertIsNone(self.job_info.status)
#         self.assertIsNone(self.job_info.job_hash)
#         self.assertEqual(self.job_info.metadata, {})

#     def test_job_initialization(self):
#         """Test that the Job is initialized correctly."""
#         self.assertEqual(self.job.id, 1)
#         self.assertEqual(self.job.name, 'test_job')
#         self.assertEqual(self.job.tasks, ['task1', 'task2'])
#         self.assertEqual(self.job.outputs, self.outputs)
#         self.assertEqual(self.job.job_script, 'script.sh')
#         self.assertEqual(self.job.database, 'test_db')
#         self.assertEqual(self.job.scheduler, 'test_scheduler')
#         self.assertEqual(self.job.db_id, 123)
#         self.assertEqual(self.job.database_opt, {'opt1': 'value1'})
#         self.assertEqual(self.job.scheduler_opt, {'opt2': 'value2'})

#     def test_job_defaults(self):
#         """Test that the Job defaults are set correctly."""
#         job = Job(id=2, name='default_job')
#         self.assertEqual(job.database, 'dummy')
#         self.assertEqual(job.scheduler, 'local')
#         self.assertEqual(job.database_opt, {})
#         self.assertEqual(job.scheduler_opt, {})

#     def test_job_asdict(self):
#         """Test that the Job can be converted to a dictionary."""
#         job_dict = asdict(self.job)
#         expected_dict = {
#             'id': 1,
#             'name': 'test_job',
#             'finished': False,
#             'status': None,
#             'job_hash': None,
#             'metadata': {},
#             'tasks': ['task1', 'task2'],
#             'outputs': [{'files_to_parse': None,
#                          'output_file': '/path/to/output',
#                          'output_folder': None,
#                          'stdout': None,
#                          'stderr': None,
#                          'stdout_file': None,
#                          'stderr_file': None,
#                          'returncode': None,
#                          'error': None}],
#             'job_script': 'script.sh',
#             'database': 'test_db',
#             'db_id': 123,
#             'database_opt': {'opt1': 'value1'},
#             'scheduler': 'test_scheduler',
#             'scheduler_opt': {'opt2': 'value2'},
#             'connection_type': None,
#             'connection_opt': {},
#             'files': None
#             }
#         self.assertEqual(job_dict, expected_dict)


# if __name__ == '__main__':
#     unittest.main()
