import unittest
from unittest.mock import MagicMock

from hyrun.job import ArrayJob, Job, update_arrayjob


class TestArrayJob(unittest.TestCase):
    """Test array job."""

    def setUp(self):
        """Set up."""
        self.logger = MagicMock()
        self.jobs = [
            Job(name='job0', scheduler='s0', database='db0'),
            Job(name='job1', scheduler='s0', database='db1'),
            Job(name='job2', scheduler='s1', database='db0'),
            Job(scheduler='s1', database='db0'),
            Job(scheduler='s1', database='db1')
        ]
        self.array_job = ArrayJob(jobs=self.jobs, logger=self.logger)

    def test_initialization(self):
        """Test that array job is initialized."""
        self.assertEqual(len(self.array_job), 5)
        self.assertEqual(self.array_job[0].scheduler, 's0')
        self.assertEqual(self.array_job[0].database, 'db0')

    def test_getitem(self):
        """Test that item is retrieved."""
        job = self.array_job[1]
        self.assertEqual(job.scheduler, 's0')
        self.assertEqual(job.database, 'db1')
        job = self.array_job[0, 1]
        self.assertEqual(job.scheduler, 's0')
        self.assertEqual(job.database, 'db1')

    def test_setitem(self):
        """Test that item is set."""
        new_job = Job(scheduler='s2', database='db2')
        self.array_job[1] = new_job
        # note, the jobs get sorted by scheduler and database
        self.assertEqual(self.array_job[4].scheduler, 's2')
        self.assertEqual(self.array_job[4].database, 'db2')
        # with self.assertRaises(NotImplementedError):
        #     self.array_job[1] = Job()

    def test_normalize_input(self):
        """Test that input is normalized."""
        normalized_jobs = self.array_job._normalize_input(self.jobs)
        self.assertEqual(len(normalized_jobs), 5)
        self.assertEqual(normalized_jobs[0].scheduler, 's0')
        self.assertEqual(normalized_jobs[0].database, 'db0')
        self.assertEqual(normalized_jobs[-1].scheduler, 's1')
        self.assertEqual(normalized_jobs[-1].database, 'db1')

    def test_check_common_attributes(self):
        """Test that common attributes are checked."""
        self.assertTrue(self.array_job._check_common_attributes(self.jobs[0:2],
                                                                ['scheduler']))

    def test_update_arrayjob_decorator(self):
        """Test the update_arrayjob decorator."""

        class JobProcessor:
            @update_arrayjob
            def process_job(self, job, *args, **kwargs):
                """Process job."""
                if job.name is not None:
                    job.name += '_processed'
                return job

        processor = JobProcessor()
        updated_array_job = processor.process_job(self.array_job)

        self.assertEqual(len(updated_array_job.jobs), 5)
        self.assertEqual(updated_array_job.jobs[0].name, 'job0_processed')
        self.assertEqual(updated_array_job.jobs[1].name, 'job1_processed')
        self.assertEqual(updated_array_job.jobs[2].name, 'job2_processed')

    def test_update_arrayjob_decorator_no_jobs(self):
        """Test the update_arrayjob decorator with no jobs."""
        empty_array_job = ArrayJob(jobs=[])

        class JobProcessor:
            @update_arrayjob
            def process_job(self, job, *args, **kwargs):
                """Process job."""
                return job

        processor = JobProcessor()
        with self.assertRaises(ValueError) as context:
            processor.process_job(empty_array_job)

        self.assertEqual(str(context.exception), 'No jobs provided')

    def test_convert_to_job_from_job(self):
        """Test converting a Job object to a Job."""
        job = Job(id=1, name='test_job')
        converted_job = self.array_job._convert_to_job(job)
        self.assertEqual(converted_job.id, 1)
        self.assertEqual(converted_job.name, 'test_job')

    def test_convert_to_job_from_dict(self):
        """Test converting a dictionary to a Job."""
        job_dict = {'id': 2, 'name': 'test_job_dict'}
        converted_job = self.array_job._convert_to_job(job_dict)
        self.assertEqual(converted_job.id, 2)
        self.assertEqual(converted_job.name, 'test_job_dict')

    def test_convert_to_job_from_list(self):
        """Test converting a list of tasks to a Job."""
        job_list = [Job(id=3, name='task1'), Job(id=3, name='task2')]
        converted_job = self.array_job._convert_to_job(job_list)
        self.assertEqual(converted_job.id, 3)
        self.assertEqual(converted_job.name, 'task1')
        self.assertEqual(len(converted_job.tasks), 2)

    def test_convert_to_job_common_attributes_check(self):
        """Test that common attributes are checked."""
        job_list = [Job(id=4, name='task1', database='db1'),
                    Job(id=4, name='task2', database='db2')]
        with self.assertRaises(ValueError):
            self.array_job._convert_to_job(job_list)

    def test_group_jobs(self):
        """Test grouping jobs by scheduler."""
        groups = self.array_job._group_jobs(self.jobs)
        self.assertEqual(len(groups), 2)
        self.assertEqual(len(groups[0]), 2)  # Two jobs with scheduler 's0'
        self.assertEqual(len(groups[1]), 3)  # Three jobs with scheduler 's1'

        self.assertEqual(groups[0][0].scheduler, 's0')
        self.assertEqual(groups[1][0].scheduler, 's1')


if __name__ == '__main__':
    unittest.main()
