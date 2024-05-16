from contextlib import suppress
from typing import Optional

import numpy as np


class ArrayJob:
    """Array job."""

    def __init__(self,
                 run_settings,
                 shape: Optional[tuple] = None,
                 **kwargs):
        """Initialize."""
        self.run_settings = self.get_settings(
            run_settings=run_settings, shape=shape)
        self.common_keys = self.check_common_keys(**kwargs)

    def flatten_to_2d_list(self, l):
        """Flatten a deeply nested list to a 2D list."""
        result = []
        for sublist in l:
            if isinstance(sublist, list):
                flat_sublist = [item for subsublist in sublist
                                for item in (self.flatten_to_2d_list(subsublist)
                                             if isinstance(subsublist, list)
                                             else [subsublist])]
                result.append(flat_sublist)
            else:
                result.append([sublist])
        return result

    def get_settings(self, run_settings=None, shape=None):
        """Get settings."""
        if not isinstance(run_settings, list):
            run_settings = [[run_settings]]
        else:
            if not all(isinstance(rs, list) for rs in run_settings):
                run_settings = [run_settings]
        run_settings = self.flatten_to_2d_list(run_settings)

        with suppress(ValueError):
            shape = shape or (len(run_settings), 1)
            run_settings = np.array(run_settings).reshape(shape).tolist()
        return run_settings


    def check_common_keys(self, keys: Optional[list] = None) -> bool:
        """Prepare array job."""
        keys = keys or []
        return all(
            all(getattr(rrs, k, None) == getattr(rs[0], k, None) for rrs in rs)
            for k in keys for rs in self.run_settings
        )
