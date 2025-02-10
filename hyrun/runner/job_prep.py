import hashlib
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from socket import gethostname

from hytools.logger import Logger

from hyrun.job import Job, update_arrayjob

from .filemanager import FileManager

try:
    from hytools.file import File
except ImportError:
    from hyset import File



class JobPrep:



class JobPrepOld(FileManager):
    """Prepare jobs for execution."""

    def gen_job_script(self, job=None, scheduler=None):
        """Generate job script."""
        job_script_str = scheduler.gen_job_script(job)  # type: ignore
        job_hash = hashlib.sha256(job_script_str.encode()).hexdigest()

        job_script_name = (getattr(job.tasks[0],  # type: ignore
                                   'job_script_filename', None)
                           or f'job_script_{job_hash}.sh')
        job_script = File(name=job_script_name,
                          content=job_script_str)  # type: ignore
        return replace(job,
                       job_script=job_script,
                       job_hash=job_hash)

    def check_types(self, obj):
        """Check types."""
        # move to hydb?
        self.logger.debug(f'Checking types in {obj} ({type(obj)}) ' +
                          'for storage in database')
        ignore_types = (Logger, timedelta, Path)
        std_types = (str, int, float, bool)
        seqs = (list, tuple, set)
        if isinstance(obj, ignore_types + std_types) or not obj:
            return
        if isinstance(obj, seqs):
            for e in obj:
                self.check_types(e)
        else:
            d = obj if isinstance(obj, dict) else obj.__dict__
            for k, v in d.items():
                if v is None or isinstance(v, ignore_types + std_types):
                    continue
                elif isinstance(v, dict) or isinstance(v, seqs):
                    self.check_types(v)
                else:
                    self.logger.error(f'Found non-standard type {type(v)} ' +
                                      f'in task: {k}')

    @update_arrayjob
    def prepare_jobs(self,
                     *args,
                     job=None,
                     scheduler=None,
                     **kwargs) -> Job:
        """Prepare jobs."""
        parent = getattr(job.tasks[0], 'submit_dir_local', None)
        host = gethostname()
        job = self.gen_job_script(job=job, scheduler=scheduler)
        self.write_file_local(job.job_script, parent=parent, host=host)

        job.job_script = self.resolve_file_name(
            job.job_script, parent=parent, host=host)
        for t in job.tasks:
            self._prepare_task_files(t, host=host)

        for o in job.outputs:
            self._prepare_output_files(o)
        return job

    def _prepare_task_files(self, task, host):
        """Prepare task files."""
        parent = getattr(task, 'work_dir_local', None)
        if not all([parent, host]):
            self.logger.error('work_dir_local and host must be set')
            return

        self.write_file_local(task.files_to_write, parent=parent, host=host)
        task.files_to_write = self.resolve_file_name(
            task.files_to_write, parent=parent, host=host)
        task.files_for_restarting = self.resolve_file_name(
            task.files_for_restarting, parent=parent, host=host)
        task.files_to_parse = self.resolve_file_name(
            task.files_to_parse, parent=parent, host=host)

        host = getattr(
            task,
            'host',
            None) or getattr(
            task,
            'connection',
            {}).get(
            'host',
            gethostname())
        parent = getattr(task, 'work_dir_remote', None)
        if host == gethostname():
            parent = getattr(task, 'work_dir_local', None)

        for f in ['output_file', 'stdout_file', 'stderr_file']:
            if hasattr(task, f):
                setattr(task, f, self.resolve_file_name(getattr(task, f),
                                                        parent=parent,
                                                        host=host))
        for f in ['file_handler', 'cluster_settings', 'logger']:
            if hasattr(task, f):
                setattr(task, f, None)
        self.check_types(task)

    def _prepare_output_files(self, output):
        """Prepare output files."""
        parent = getattr(output, 'work_dir_local', None)
        host = gethostname()
        for f in ['output_file', 'stdout_file', 'stderr_file']:
            if hasattr(output, f):
                setattr(output, f, self.resolve_file_name(getattr(output, f),
                                                          parent=parent,
                                                          host=host))

        output.files_to_parse = self.resolve_file_names(
            output.files_to_parse, parent, host)
