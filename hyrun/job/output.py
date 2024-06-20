from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union
import inspect

PathLike = Union[Path, str]
PathLikeList = Union[PathLike, List[PathLike]]


@dataclass
class Output:
    """Container for hyrun results."""

    files_to_parse: Optional[PathLikeList] = None
    output_file: Optional[PathLike] = None
    output_folder: Optional[PathLike] = None
    stdout: Optional[PathLike] = None
    stderr: Optional[PathLike] = None
    stdout_file: Optional[PathLike] = None
    stderr_file: Optional[PathLike] = None
    returncode: Optional[int] = None
    error: Optional[Exception] = None

    def from_dict(self, data: dict):
        """Load data from dictionary."""
        for key, value in data.items():
            # if hasattr(self, key):
            #     setattr(self, key, value)
            if key in self.__annotations__:
                setattr(self, key, value)