
from pathlib import Path
from typing import List, Optional, Union

PathLike = Union[str, Path, None]


def get_docker_launcher(cpus_per_task: Optional[int] = None,
                   env_vars: Optional[dict] = None,
                   container_image: Optional[str] = None,
                   container_mounts: Optional[dict] = None,
                   container_executable: Optional[str] = None,
                   work_dir_container: Optional[PathLike] = None,
                   **kwargs) -> List[str]:
    """Return a command to run a Docker container.

    Parameters
    ----------
    cpus_per_task : Optional[int], optional
        CPUs per task, by default None
    env_vars : Optional[dict], optional
        Environment variables, by default None
    container_image : Optional[str], optional
        Container image, by default None
    container_mounts : Optional[dict], optional
        Container mounts, by default None
    container_executable : Optional[str], optional
        Container executable, by default None
    work_dir_container : Optional[PathLike], optional
        Work directory container, by default None

    Returns
    -------
    list
        Command to run a Docker container.

    """
    cpus_per_task = cpus_per_task or 1
    env_vars = env_vars or {}
    docker_mounts = container_mounts or {}
    container_executable = container_executable or 'docker'
    if not container_image:
        return []

    work_dir_container = work_dir_container or Path('/work')

    env_vars_cmd = [f'--env {k}={v}' for k, v in env_vars.items()]
    if cpus_per_task > 1:
        env_vars_cmd.append(f'--env OMP_NUM_THREADS={cpus_per_task}')

    # remove double items from docker_mounts

    docker_mounts = {str(k): str(v)
                     for k, v in docker_mounts.items()}
    docker_mounts_cmd = [f'-v{k}:{v}' for k, v in docker_mounts.items()]

    cmd = [
        str(container_executable),
        'run', '-a', 'stdout', '-a', 'stderr', '--rm',
        '--cpus', str(cpus_per_task),
        *env_vars_cmd,
        *docker_mounts_cmd,
        f'-w{str(work_dir_container)}',
        container_image
    ]
    return cmd
