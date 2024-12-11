from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

PathLike = Union[Path, str]
PathLikeList = Union[PathLike, List[PathLike]]


@dataclass
class ResultContainer:
    """Container for local results."""
    files_to_parse: Optional[PathLikeList] = None
    output_file: Optional[PathLike] = None
    output_folder: Optional[PathLike] = None
    stdout: Optional[PathLike] = None
    stderr: Optional[PathLike] = None
    returncode: Optional[int] = None
    error: Optional[Exception] = None
    job_id: Optional[int] = None
    job_host: Optional[str] = None
    job_hash: Optional[str] = None
