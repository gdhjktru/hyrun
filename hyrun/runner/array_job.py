import numpy as np
from typing import Optional
from contextlib import suppress

class ArrayJob:
    """Array job."""

    def __init__(self,
                 run_settings,
                 shape: Optional[tuple] = None,
                 **kwargs):
        """Initialize."""
        self.shape, self.run_settings = self.get_settings(
            run_settings=run_settings, shape=shape)
        self.common_keys = self.check_common_keys(**kwargs)

    def get_settings(self, run_settings=None, shape=None): 
        """Get settings."""
        if not isinstance(run_settings, list):
            run_settings = [[run_settings]]
        shape = self.get_shape(shape, run_settings)
        with suppress(ValueError):
            run_settings = np.array(run_settings).reshape(shape).tolist()
        return shape, run_settings
       
    def get_shape(self, shape=None, run_settings=[]):
        """Get shape."""
        if shape is not None and len(shape) > 2:
            raise ValueError("array_job must be 1D or 2D.")
        return shape if shape else (len(run_settings), 1)
        
    def check_common_keys(self, keys: Optional[list] = None) -> bool:
        """Prepare array job."""
        keys = keys or []
        return all(
            all(getattr(rrs, k, None) == getattr(rs[0], k, None) for rrs in rs)
            for k in keys for rs in self.run_settings
        )
        
        