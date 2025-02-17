


# def wait(jobs, timeout=None) -> dict:
#     """Wait for job to finish."""
#     timeout = (timeout or Wait._get_timeout(jobs))
#     if timeout <= 0:
#         return self.get_status_run(jobs, connection=connection)

#     incrementer = self._increment_and_sleep(1)
#     self.logger.info(f'Waiting for jobs to finish. Timeout: {timeout} ' +
#                         'seconds.')
#     for t in incrementer:
#         jobs = self.get_status_run(jobs, connection=connection)
#         if t >= timeout or self.is_finished(jobs):
#             break
#     return jobs



class Wait:


    def _increment_t(self, t, tmin=1, tmax=60) -> int:
        """Increment t."""
        return min(max(2 * t, tmin), tmax)

    def _increment_and_sleep(self, t) -> Generator[int, None, None]:
        """Increment and sleep."""
        while True:
            yield t
            sleep(t)
            t = self._increment_t(t)

    def is_finished(self, jobs) -> bool:
        """Check if job is finished."""
        return all(job['scheduler'].is_finished(job['job'])
                   for job in jobs.values())

    @classmethod
    def _get_timeout(self, jobs):
        """Get timeout."""
        return max(sum([t.job_time.total_seconds()
                        if isinstance(t.job_time, timedelta)
                        else t.job_time for t in j['job'].tasks])
                   for j in jobs.values())
