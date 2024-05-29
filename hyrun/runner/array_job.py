from typing import Optional

from hytools.logger import LoggerDummy


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

    def flatten_to_2d_list(self, ll):
        """Flatten a deeply nested list to a 2D list."""
        return [[item for subsublist in sublist
                 for item in (self.flatten_to_2d_list(subsublist)
                              if isinstance(subsublist, list)
                              else [subsublist])]
                for sublist in ll]

    def fix_1d_list(self, ll, scheduler) -> list:
        """Fix 1d list."""
        return (ll if any(isinstance(item, list) for item in ll)
                else [[item] for item in ll]
                if 'local' in scheduler.__class__.__name__.lower()
                else [ll])

    def get_settings(self, run_settings, scheduler) -> list:
        """Get settings."""
        if not isinstance(run_settings, list):
            run_settings = [[run_settings]]

        run_settings = self.flatten_to_2d_list(
            self.fix_1d_list(run_settings, scheduler))

        self.logger.info(
            f'{len(run_settings)} job(s) with ' +
            f'{scheduler.__class__.__name__} scheduler detected.')

        for i, rs in enumerate(run_settings):
            self.logger.debug(
                f'   job {i} task(s): {len(rs)}')
        return run_settings

    def check_common_keys(self, keys: Optional[list] = None, **kwargs) -> bool:
        """Prepare array job."""
        keys = keys or []
        return all(
            all(getattr(rrs, k, None) == getattr(rs[0], k, None) for rrs in rs)
            for k in keys for rs in self.run_settings
        )
