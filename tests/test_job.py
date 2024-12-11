import unittest
from dataclasses import asdict

from hyrun.job import Job, JobInfo, Output


class TestJob(unittest.TestCase):
    """Test Job and JobInfo classes."""

    def setUp(self):
        """Set up."""
        self.job_info = JobInfo(id=1, name='test_job')
        self.outputs = [Output(output_file='/path/to/output')]
        self.job = Job(
            id=self.job_info.id,
            name=self.job_info.name,
            tasks=['task1', 'task2'],
            outputs=self.outputs,
            job_script='script.sh',
            database='test_db',
            scheduler='test_scheduler',
            db_id=123,
            database_opt={'opt1': 'value1'},
            scheduler_opt={'opt2': 'value2'}
            )

    def test_job_info_initialization(self):
        """Test that the JobInfo is initialized correctly."""
        self.assertEqual(self.job_info.id, 1)
        self.assertEqual(self.job_info.name, 'test_job')
        self.assertFalse(self.job_info.finished)
        self.assertIsNone(self.job_info.status)
        self.assertIsNone(self.job_info.job_hash)
        self.assertEqual(self.job_info.metadata, {})

    def test_job_initialization(self):
        """Test that the Job is initialized correctly."""
        self.assertEqual(self.job.id, 1)
        self.assertEqual(self.job.name, 'test_job')
        self.assertEqual(self.job.tasks, ['task1', 'task2'])
        self.assertEqual(self.job.outputs, self.outputs)
        self.assertEqual(self.job.job_script, 'script.sh')
        self.assertEqual(self.job.database, 'test_db')
        self.assertEqual(self.job.scheduler, 'test_scheduler')
        self.assertEqual(self.job.db_id, 123)
        self.assertEqual(self.job.database_opt, {'opt1': 'value1'})
        self.assertEqual(self.job.scheduler_opt, {'opt2': 'value2'})

    def test_job_defaults(self):
        """Test that the Job defaults are set correctly."""
        job = Job(id=2, name='default_job')
        self.assertEqual(job.database, 'dummy')
        self.assertEqual(job.scheduler, 'local')
        self.assertEqual(job.database_opt, {})
        self.assertEqual(job.scheduler_opt, {})

    def test_job_asdict(self):
        """Test that the Job can be converted to a dictionary."""
        job_dict = asdict(self.job)
        expected_dict = {
            'id': 1,
            'name': 'test_job',
            'finished': False,
            'status': None,
            'job_hash': None,
            'metadata': {},
            'tasks': ['task1', 'task2'],
            'outputs': [{'files_to_parse': None,
                         'output_file': '/path/to/output',
                         'output_folder': None,
                         'stdout': None,
                         'stderr': None,
                         'stdout_file': None,
                         'stderr_file': None,
                         'returncode': None,
                         'error': None}],
            'job_script': 'script.sh',
            'database': 'test_db',
            'db_id': 123,
            'database_opt': {'opt1': 'value1'},
            'scheduler': 'test_scheduler',
            'scheduler_opt': {'opt2': 'value2'}
            }
        self.assertEqual(job_dict, expected_dict)


if __name__ == '__main__':
    unittest.main()
