
from datetime import timedelta
from pathlib import Path

from .timedelta import timedelta_to_slurmtime


def gen_job_script(*args, **kwargs):
    """Generate job script."""
    return SlurmJobScript().job_script(*args, **kwargs)


class SlurmJobScript:
    """Generate job script SLURM."""

    # def _check_key_equal(self, rs: List[RunSettings], key: str):
    #     """Check that a key is the same for all run_settings."""
    #     val = [getattr(run_settings, key, None) for run_settings in rs]
    #     try:
    #         val = set(val)  # type: ignore
    #     except TypeError:
    #         val = set([tuple(v) for v in val])  # type: ignore
    #     if len(val) > 1:
    #         raise ValueError(f'{key} must be the same for all run_settings',
    #                          val if len(val) < 10 else val[:10])

    # def _check_key_not_equal(self, rs: List[RunSettings], key: str):
    #     """Check that a key is not the same for all run_settings."""
    #     val = [getattr(run_settings, key, None) for run_settings in rs]
    #     try:
    #         val = set(val)  # type: ignore
    #     except TypeError:
    #         val = set([tuple(v) for v in val])  # type: ignore
    #     if len(val) == 1:
    #         raise ValueError(f'{key} must be different for all run_settings')

    # def _check_common_settings(self, rs: List[RunSettings]):
    #     """Check that the run_settings have the same common settings."""
    #     for key in [
    #             'submit_dir_remote', 'submit_dir_local', 'debug',
    #             'use_relative_paths', 'force_recompute', 'overwrite_files',
    #             'monitor', 'job_script_file', 'job_id', 'create_symlinks',
    #             'progress_bar'
    #     ]:
    #         self._check_key_equal(rs, key)

    #     # for key in ['work_dir_remote']:
    #     #     self._check_key_not_equal(rs, key)

    def job_script(self, job_name: str, tasks: list) -> str:
        """Generate SLURM job script for running the `program`."""
        rs = tasks[0]  # reference run_settings
        job_time = sum([t.job_time.total_seconds()
                        for t in tasks])
        slurm_job_time = timedelta_to_slurmtime(timedelta(seconds=job_time))

        # job_name = self._gen_job_name_bundle(job)
        s = rs.submit_dir_remote

        job_script = '#!/bin/bash\n'
        job_script += f'#SBATCH --job-name={job_name}\n'
        job_script += f'#SBATCH --time={slurm_job_time}\n'
        if rs.memory_per_cpu is not None:
            job_script += '#SBATCH '
            job_script += f'--mem-per-cpu={rs.memory_per_cpu}\n'
        job_script += f'#SBATCH --cpus-per-task={rs.cpus_per_task}\n'
        job_script += f'#SBATCH --ntasks={rs.ntasks}\n'
        job_script += f'#SBATCH --account={rs.slurm_account}\n'
        job_script += f'#SBATCH --output={s}/{job_name}.out\n'
        job_script += f'#SBATCH --error={s}/{job_name}.err\n'
        print('CHANGEME')
        job_script += f'#SBATCH --acctg-freq=1\n'

        if rs.qos_devel:
            job_script += '#SBATCH --qos=devel\n'
        for slurm_option in rs.slurm_extra:
            job_script += f'#SBATCH {slurm_option}\n'
        job_script += '\nset -o errexit\n\n'
        job_script += 'set -x\n\n'

        modules = [t.modules for t in tasks]
        if self._check_nested_list_equal(modules, modules, 2):
            modules = [[] for _ in range(len(tasks))]
            modules[0] = rs.modules

        envs = [t.env for t in tasks]
        if self._check_nested_list_equal(envs, envs, 2):
            envs = [[] for _ in range(len(tasks))]
            envs[0] = rs.env
        multiple_remote_dirs = (
            len(list({r.work_dir_remote for r in tasks})) > 1
        )

        for i in range(len(tasks)):

            if modules[i]:
                job_script += '# Load modules\n'
                job_script += 'module purge\n'
                for module in modules[i]:
                    job_script += f'module load {module}\n'
                job_script += '\n'

            if envs[i]:
                job_script += '# Setup environment\n'
                for env_line in envs[i]:
                    job_script += f'{env_line}\n'
                job_script += '\n'

            run_settings = tasks[i]
            job_script += f'# Run task no. {i}\n'
            wdir = (run_settings.work_dir_remote.parent
                    if 'job_id' in run_settings.work_dir_remote.name
                    else run_settings.work_dir_remote)
            job_script += f'export SCRATCH="{wdir}/'
            job_script += f'${{SLURM_JOB_ID}}"\n'  # noqa: F541
            job_script += f'mkdir -p ${{SCRATCH}}\n'  # noqa: F541
            for f in run_settings.files_to_send + run_settings.files_to_write:
                file = str(Path(wdir)/f.name)
                job_script += f'cp -f {file} ${{SCRATCH}}\n'

            job_script += f'cd ${{SCRATCH}}\n'  # noqa: F541

            if run_settings.pre_cmd:
                job_script += '\n# Pre processing\n'
                pp = run_settings.pre_cmd  # type: ignore
                pp = [pp] if isinstance(pp, str) else pp
                for cmd in pp:
                    job_script += f'{cmd}\n'

            symlinks_created = []
            if run_settings.create_symlinks and len(
                    run_settings.data_files) > 0:
                job_script += '\n# Create symlinks\n'
                data_dir_remote = run_settings.data_dir_remote
                for f in run_settings.data_files:
                    file = str(Path(data_dir_remote)/f.name)
                    job_script += f'ln -s {file} ${{SCRATCH}}\n'
                    name = file
                    if '/' in name:
                        name = name.split('/')[-1]
                    symlinks_created.append(name)

            for i, arg in enumerate(run_settings.args):
                try:
                    path = Path(arg)
                except (TypeError, OSError, ValueError):
                    continue
                if path.is_absolute() or (path.is_relative_to('~')
                   and '/' in str(path)):
                    run_settings.args[i] = f'${{SCRATCH}}/{path.name}'

            job_script += f'{run_settings.launcher} {run_settings.program} '
            job_script += f'{" ".join(run_settings.args)} '
            if run_settings.stdin_file:
                job_script += f'< {run_settings.stdin_file.path} '
            job_script += f'>> {run_settings.stdout_file.path} '
            job_script += f'2>> {run_settings.stderr_file.path}\n'

            for symlink in symlinks_created:
                job_script += (f'rm {symlink}\n')
            if multiple_remote_dirs:
                job_script += f'cp -rf ${{SCRATCH}}/* '  # noqa: F541
                job_script += f'{run_settings.work_dir_remote}\n'

            if run_settings.post_cmd:
                job_script += '\n# Post processing\n'
                pp = run_settings.post_cmd  # type: ignore
                pp = [pp] if isinstance(pp, str) else pp
                for cmd in pp:
                    job_script += f'{cmd}\n'

            job_script += '\n'

        return job_script

    def _check_nested_list_equal(self, list1, list2, max_depth=1):
        """Check that two nested lists are equal."""
        if max_depth < 1:
            return list1 == list2
        if len(list1) != len(list2):
            return False
        for i, ll in enumerate(list1):
            if isinstance(ll, list):
                if not self._check_nested_list_equal(ll, list2[i],
                                                     max_depth - 1):
                    return False
            elif ll != list2[i]:
                return False
        return True

    def _gen_job_name_bundle(self, job) -> str:
        """Generate SLURM job name.

        Parameters
        ----------
        job : Job
            Information about the job.

        Returns
        -------
        str
            Job name generated from the current time and date.

        """
        job_names = [t.job_name for t in job.tasks]
        if len(set(job_names)) == 1 and job_names[0] is not None:
            return job_names[0]

        return 'hyrun_job'

        # program = 'slurm_job' if len(job_names) == 1 else 'slurm_bundle'
        # return (
        #     f"{program}_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
