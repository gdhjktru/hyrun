from collections import defaultdict

from hydb import get_database

from hyrun.job import update_arrayjob


class JobDatabaseManager:
    """Job database manager."""

    def gen_db_info(self, jobs: dict) -> dict:
        """Generate database info."""
        result = defaultdict(list)
        for job in jobs.values():
            db_id = int(job['job'].db_id)
            database = str(job['database'].name)
            result[database].append(db_id)
        return dict(result)

    @update_arrayjob
    def get_jobs_from_db(self,
                         *args,
                         job=None,
                         database=None,
                         key=None,
                         val=None,
                         **kwargs):
        """Resolve db_id."""
        db = get_database(database)
        db_id = job.db_id
        key = str(key) or 'db_id'
        val = str(val) or int(db_id)
        entry = db.get(key=key, value=val)
        if isinstance(entry, list):
            if len(entry) > 1:
                self.logger.error('Multiple entries found in database')
                return None
            entry = entry[0]
        db_id = entry.doc_id
        if not entry:
            self.logger.debug(f'No job found in database with {key} {val}')
            return None
        self.logger.debug(f'Found job in database with id {db_id}')

        job = db.dict_to_obj(entry)
        job.db_id = db_id
        return job

    @update_arrayjob
    def add_to_db(self, *args, job=None, database=None, **kwargs):
        """Add job to database."""
        if job.db_id is not None:
            self.logger.debug('Job already in database, updating...')
            return self.update_db(job=job, database=database)
        db_id = database.add(job)
        if db_id < 0:
            self.logger.error('Error adding job to database')
            return job
        else:
            job.db_id = db_id
            job = self.update_db(job=job, database=database)
            self.logger.info(f'Added job to database with id {db_id}')
            self.logger.debug('db entry: ' +
                              f'{database.get(key="db_id", value=db_id)}')
        return job

    @update_arrayjob
    def update_db(self, *args, job=None, database=None, **kwargs):
        """Update job in database."""
        db_id = getattr(job, 'db_id', None)
        if db_id is None:
            raise ValueError('db_id must be set to update job in database')
        database.update(db_id, job)
        self.logger.info(f'Updated job in database with id {db_id}')
        self.logger.debug('db entry: ' +
                          f'{database.get(key="db_id", value=db_id)}')
        return job
