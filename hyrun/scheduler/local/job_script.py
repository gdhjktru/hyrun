from typing import List, Optional, Union
import re
import shlex
from hyset import RunSettings
from pathlib import Path
from sys import executable as python_ex
from .conda import get_conda_launcher
from .docker import get_docker_launcher

class JobScript:
    """Job script."""

    @classmethod
    def get_launcher(cls, run_settings):
        """Get launcher."""
        # allows conda in docker but not docker in conda
        return get_conda_launcher(run_settings.conda_env,
                                  [*get_docker_launcher(
                                      **run_settings.__dict__),
                                      *run_settings.launcher])

    @classmethod
    def _gen_running_list(cls, run_settings: RunSettings) -> List[str]:
        """Generate running list."""
        launcher = cls.get_launcher(run_settings)
        running_list: List[str] = [
            *launcher,
            run_settings.program,
            *run_settings.args,  # type: ignore
        ]  # type: ignore #14891
        return  [str(x).strip() for x in running_list]
     

    @classmethod
    def sanitize_cmd(cls, cmd: Union[str, list],
                     delimiters: Optional[List[str]] = None) -> List[str]:
        """Sanitize command."""
        delimiters = delimiters or [';', '|', '&&']
        if not cmd:
            return []
        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        pattern = '|'.join(map(re.escape, delimiters))  # type: ignore
        parts = re.split(f'({pattern})', cmd)

        result = []
        for part in parts:
            if part in delimiters:
                result.append(part)
            else:
                subparts = re.split(r'([ \t+])', part)
                for subpart in subparts:
                    if '\n' in subpart:
                        if '"' in subpart:
                            result.extend(shlex.split(subpart))
                        else:
                            # split
                            subpart = subpart.split('\n')
                            subpart.append(';')
                            result.extend(subpart)
                    else:
                        result.append(subpart)
        # remove empty strings
        cmd = [subpart for subpart in result if subpart.strip()]
        # remove leading and trailing delimiters
        if any([c in cmd[0] for c in delimiters]):
            cmd = cmd[1:]
        # add trailing delimiter
        if not any([c in cmd[-1] for c in delimiters]):
            cmd.append(';')

        return cmd

    @classmethod    
    def gen_job_script(cls, run_settings: RunSettings) -> str:
        """Generate command."""
        
        running_list = cls.sanitize_cmd(
            cls._gen_running_list(run_settings))
        pre_cmd = cls.sanitize_cmd(getattr(run_settings, 'pre_cmd', []))
        post_cmd = cls.sanitize_cmd(getattr(run_settings, 'post_cmd', []))
        running_list[-1] = (
            '&&' if running_list[-1] == ';' else running_list[-1]
        )

        cmd = pre_cmd + running_list + post_cmd

        for c in cmd:
            if 'python' in c:
                c = c.replace('python', python_ex)

        return ' '.join(cmd)
