from typing import Optional, List
from pathlib import Path
from contextlib import suppress
import shutil


def get_conda_launcher(conda_env: Optional[str] = None,
                       launcher: Optional[List[str]] = None,
                       **kwargs
                       ) -> List[str]:
    """Set conda environment."""
    launcher = launcher or []

    if not conda_env:
        return launcher

    if 'conda' in launcher:
        pos_env = launcher.index(conda_env)
        launcher = launcher[pos_env + 1:]

    conda_launcher = ['conda', 'run', '-n', conda_env]
    launcher.extend(conda_launcher)
    return launcher
   

def get_conda_path(conda_env: Optional[str]= None,
                   path: Optional[List[str]] = None,
                   **kwargs
                   ) -> List[str]:
    """Get conda path."""
    conda_bin_dir = None
    path = path or []
    if not conda_env:
        return path
    with suppress(AttributeError):
        t_dir = str(conda_env) + '/bin'
        conda_bin_dir = str(
            Path(shutil.which('python')).parents[2] / t_dir  # type: ignore
        )
    return [conda_bin_dir] + path if conda_bin_dir else path
   