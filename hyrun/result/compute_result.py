from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

PathLike = Union[Path, str]
PathLikeList = Union[PathLike, List[PathLike]]


class ComputeResult:
    """Base class for ComputeResult."""

    files_to_parse: Optional[PathLikeList] = None
    output_file: Optional[PathLike] = None
    output_folder: Optional[PathLike] = None
    stdout: Optional[PathLike] = None
    stderr: Optional[PathLike] = None
    returncode: Optional[int] = None
    error: Optional[Exception] = None


@dataclass
class LocalResult(ComputeResult):
    """Container for local results."""


@dataclass
class RemoteResult(LocalResult):
    """Container for remote results."""

    # scheduler_id: Optional[int] = None
    # database_id: Optional[int] = None
    # host: Optional[str] = None
    # job_hash: Optional[str] = None
