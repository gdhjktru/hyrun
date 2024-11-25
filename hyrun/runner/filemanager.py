from contextlib import suppress
from functools import wraps
from pathlib import Path
from socket import gethostname
from string import Template
from typing import Optional


def list_exec(func):
    """Decorate to execute a function on a list of arguments."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """Execute function on list."""
        if isinstance(args[0], list):
            return [func(self, arg, *args[1:], **kwargs) for arg in args[0]]
        else:
            return func(self, *args, **kwargs)
    return wrapper


class FileManager:
    """File manager."""

    @list_exec
    def write_file_local(self, file, overwrite=True, **kwargs):
        """Write file locally."""
        p = Path(self.resolve_file_name(file, **kwargs).get('path'))
        if p.exists() and not overwrite:
            return file
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.replace_var_in_file_content(file).content)
        return str(p)

    @list_exec
    def replace_var_in_file_content(self, file):
        """Replace variables in file content."""
        with suppress(AttributeError):
            file.content = Template(file.content
                                    ).safe_substitute(**file.variables)
        return file

    def resolve_file_name(self,
                          file,
                          parent: Optional[str] = None,
                          host: Optional[str] = None) -> dict:
        """Resolve file name."""
        if not file:
            return {}
        parent = parent or str(Path('.'))
        file = (Path(file.folder) / file.name
                if file.folder is not None
                else Path(parent) / file.name)
        return {'path': str(file), 'host': host or gethostname()}

    def resolve_file_names(self, files, parent, host):
        """Resolve file names."""
        return [self.resolve_file_name(f, parent=parent, host=host)
                for f in files]

    def get_files_to_transfer(self,
                              jobs,
                              files_to_transfer=None,
                              job_keys=None,
                              task_keys=None):
        """Get files to transfer."""
        files_to_transfer = files_to_transfer or []
        job_keys = job_keys or []
        task_keys = task_keys or []
        for j in jobs.values():
            for k in job_keys:
                _list = getattr(j['job'], k, [])
                _list = [_list] if not isinstance(_list, list) else _list
                for f in _list:
                    files_to_transfer.append(f)
            for t in j['job'].tasks:
                for k in task_keys:
                    ll = getattr(t, k, [])
                    ll = [ll] if not isinstance(ll, list) else ll
                    for f in ll:
                        files_to_transfer.append(f)
        return [f for f in files_to_transfer if f is not None]
