from typing import Optional

from hytools.logger import LoggerDummy

import itertools



class ArrayJob:
    """Array job."""

    def __init__(self,
                 run_settings,
                 **kwargs):
        """Initialize."""
        self.logger = kwargs.pop('logger', LoggerDummy())
        self.run_settings = self.get_settings(run_settings,
                                              kwargs.pop('scheduler', None))
        self.common_keys = self.check_common_keys(**kwargs)
 

    def flatten_any_list(self, ll):
        """Flatten any list."""
        if not isinstance(ll, list):
            return [ll]
        while any(isinstance(item, list) for item in ll):
            ll = [item if isinstance(item, list) else [item] for item in ll]  
            return list(itertools.chain.from_iterable(ll)) 
        return ll if isinstance(ll, list) else [ll]
     
    def _check_nested_levels(self, run_settings):
        """Check nested levels."""
        for rs in run_settings:
            if isinstance(rs, list):
                for item in rs:
                    if isinstance(item, list):
                        raise ValueError('Run settings must be at most 2D list') 
    def get_settings(self, run_settings, scheduler) -> list:
        """Get settings."""
        if not isinstance(run_settings, list):
            return [[run_settings]]
        self._check_nested_levels(run_settings)
                        
        if  'local' in scheduler.__class__.__name__.lower():
            return [[item] for item in self.flatten_any_list(run_settings)]
        else:
            if not any(isinstance(item, list) for item in run_settings):
                return [run_settings]
            return [self.flatten_any_list(item) for item in run_settings]
            
    def check_common_keys(self, keys: Optional[list] = None, **kwargs) -> bool:
        """Prepare array job."""
        keys = keys or []
        return all(
            all(getattr(rrs, k, None) == getattr(rs[0], k, None) for rrs in rs)
            for k in keys for rs in self.run_settings
        )
