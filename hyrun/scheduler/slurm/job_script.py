
from datetime import timedelta, datetime
from typing import List, Union
from pathlib import Path

from hytools.time import timedelta_to_slurmtime


def get_job_script(job, **kwargs) -> str:
    """Generate SLURM job script for running the `program`."""
    njobs = range(len(job.tasks))
    rs = job.tasks[0]  # reference run_settings
    job_time = max([t.job_time.total_seconds()
                    for t in job.tasks])
    slurm_job_time = timedelta_to_slurmtime(timedelta(seconds=job_time))
    job_name = kwargs.get('job_name') or gen_job_name(job)
    sdir = rs.get_full_dir_path(dirname='submit_dir_remote')

    job_script = '#!/bin/bash\n'
    job_script += f'#SBATCH --job-name={job_name}\n'
    job_script += f'#SBATCH --time={slurm_job_time}\n'
    if rs.memory_per_cpu is not None:
        job_script += '#SBATCH '
        job_script += f'--mem-per-cpu={rs.memory_per_cpu}\n'
    job_script += f'#SBATCH --cpus-per-task={rs.cpus_per_task}\n'
    job_script += f'#SBATCH --ntasks={rs.ntasks}\n'
    job_script += f'#SBATCH --account={rs.scheduler.slurm_account}\n'
    job_script += f'#SBATCH --output={sdir}/{job_name}.out\n'
    job_script += f'#SBATCH --error={sdir}/{job_name}.err\n'

    if rs.scheduler.qos_devel:
        job_script += '#SBATCH --qos=devel\n'
    for slurm_option in rs.scheduler.slurm_extra:
        job_script += f'#SBATCH {slurm_option}\n'

    shell_setup = [t.shell_setup for t in job.tasks]
    if _check_nested_list_equal(shell_setup, shell_setup, 2):
        shell_setup = [[] for _ in njobs]
        shell_setup[0] = job.tasks[0].shell_setup
    if not shell_setup[0]:
        shell_setup[0] = ['set -o errexit']

    modules = [t.scheduler.modules for t in job.tasks]
    if _check_nested_list_equal(modules, modules, 2):
        modules = [[] for _ in njobs]
        modules[0] = rs.scheduler.modules

    exports = [
        [f'export {k}={v}' for k, v in t.env_vars.items()]
        for t in job.tasks]
    if _check_nested_list_equal(exports, exports, 2):
        exports = [[] for _ in njobs]
        exports[0] = [f'export {k}={v}'
                    for k, v in job.tasks[0].env_vars.items()]

    wdirs = [t.get_full_dir_path(
        dirname='work_dir_remote') for t in job.tasks]
    multiple_remote_dirs = len(list(set(wdirs))) > 1

    for i in njobs:

        if shell_setup[i]:
            job_script += '\n# Shell setup\n'
            for ss in shell_setup[i]:
                job_script += f'{ss}\n'
            job_script += '\n'

        if modules[i]:
            job_script += '# Load modules\n'
            job_script += 'module purge\n'
            for module in modules[i]:
                job_script += f'module load {module}\n'
            job_script += '\n'

        if exports[i]:
            job_script += '# Setup environment\n'
            for env_line in exports[i]:
                job_script += f'{env_line}\n'
            job_script += '\n'

        run_settings = job.tasks[i]
        job_script += f'# Runing job no. {i}\n'
        job_script += f'export SCRATCH="{wdirs[i]}/'
        job_script += f'${{SLURM_JOB_ID}}"\n'  # noqa: F541
        job_script += f'mkdir -p ${{SCRATCH}}\n'  # noqa: F541
        for f in run_settings.files_to_send + run_settings.files_to_write:
            path = run_settings.get_full_file_path(
                file=f, dirname='work_dir_remote')
            job_script += f'cp -f {path} ${{SCRATCH}}\n'

        job_script += f'cd ${{SCRATCH}}\n'  # noqa: F541

        if run_settings.pre_cmd:
            job_script += '\n# Pre processing\n'
            pp = run_settings.pre_cmd  # type: ignore
            pp = [pp] if isinstance(pp, str) else pp
            for cmd in pp:
                job_script += f'{cmd}\n'

        symlinks_created = []
        if run_settings.scheduler.create_symlinks and len(
                run_settings.data_files) > 0:
            job_script += '\n# Create symlinks\n'
            for f in run_settings.data_files:
                path = run_settings.get_full_file_path(
                    file=Path(f.name).name, dirname='data_dir_remote')
                job_script += f'ln -s {path} ${{SCRATCH}}\n'
                name = str(Path(f).name)
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
        job_script += f'{" ".join(run_settings.get_running_list())} '

        if run_settings.stdin_file:
            stdin_file_path = (run_settings.get_full_file_path(
                run_settings.stdin_file,
                dirname='work_dir_remote')
                or run_settings.stdin_file.path)
            job_script += f'< {stdin_file_path} '
        stdout_file_path = (run_settings.get_full_file_path(
            run_settings.stdout_file,
            dirname='work_dir_remote').name
            or run_settings.stdout_file.path)
        stderr_file_path = (run_settings.get_full_file_path(
            run_settings.stderr_file,
            dirname='work_dir_remote').name
            or run_settings.stderr_file.path)
        job_script += f'>> {stdout_file_path} '
        job_script += f'2>> {stderr_file_path}\n'

        for symlink in symlinks_created:
            job_script += (f'rm {symlink}\n')
        if multiple_remote_dirs:
            job_script += f'cp -rf ${{SCRATCH}}/* '  # noqa: F541
            wdir = run_settings.get_full_dir_path(
                dirname='work_dir_remote')
            job_script += f'{wdir}\n'

        if run_settings.post_cmd:
            job_script += '\n# Post processing\n'
            pp = run_settings.post_cmd  # type: ignore
            pp = [pp] if isinstance(pp, str) else pp
            for cmd in pp:
                job_script += f'{cmd}\n'

    job_script += '\n# End of job script\n'
    return job_script


def _check_nested_list_equal(list1, list2, max_depth=1):
    """Check that two nested lists are equal."""
    if max_depth < 1:
        return list1 == list2
    if len(list1) != len(list2):
        return False
    for i, ll in enumerate(list1):
        if isinstance(ll, list):
            if not _check_nested_list_equal(ll, list2[i],
                                                max_depth - 1):
                return False
        elif ll != list2[i]:
            return False
    return True

def gen_job_name(job) -> str:
    "Generate a job name."""
    program = (job.tasks[0].program if len(job.tasks) == 1
                else 'hsp_job').replace('$', '').replace(
                    '{', '').replace('}', '')
    default = f"{program}_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
    return job.metadata.get('name') or default
