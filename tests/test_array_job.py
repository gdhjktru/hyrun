import unittest

from hytools.logger import LoggerDummy

from hyrun.job import ArrayJob, Job, update_arrayjob


def get_logger():
    """Get logger."""
    return LoggerDummy()


class TestArrayJob(unittest.TestCase):
    """Test array job."""

    def setUp(self):
        """Set up."""
        self.logger = get_logger()
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

    def test_setitem(self):
        """Test that item is set."""
        new_job = Job(scheduler='s2', database='db2')
        self.array_job[1] = new_job
        # note, the jobs get sorted by scheduler and database
        self.assertEqual(self.array_job[4].scheduler, 's2')
        self.assertEqual(self.array_job[4].database, 'db2')

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


if __name__ == '__main__':
    unittest.main()
