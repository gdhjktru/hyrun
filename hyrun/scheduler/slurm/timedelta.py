import re
from datetime import timedelta


def timedelta_to_slurmtime(t: timedelta) -> str:
    """Convert a timedelta object to SLURM string format {D}-{H}:{M}:{S}.

    Parameters
    ----------
    t : datetime.timedelta
        Input timedelta object.

    Returns
    -------
    str
        String containing a {days}-{hours}:{minutes}:{seconds} repesentation
        of the input timedelta object.

    """
    days, seconds = divmod(int(t.total_seconds()), 86400)
    hours, seconds = divmod(int(seconds), 3600)
    minutes, seconds = divmod(int(seconds), 60)
    return f'{days}-{hours:02d}:{minutes:02d}:{seconds:02d}'


def slurmtime_to_timedelta(time_str: str) -> timedelta:
    """Convert time on slurm format to timedelta."""
    regex_patterns = {
        'days-hours:minutes:seconds': r'^(\d+)-(\d+):(\d+):(\d+)$',
        'days-hours:minutes': r'^(\d+)-(\d+):(\d+)$',
        'days-hours': r'^(\d+)-(\d+)$',
        'hours:minutes:seconds': r'^(\d+):(\d+):(\d+)$',
        'minutes:seconds': r'^(\d+):(\d+)$',
        'minutes': r'^(\d+)$'
    }

    for fmt, pattern in regex_patterns.items():
        match = re.match(pattern, time_str)
        if match:
            if fmt == 'days-hours:minutes:seconds':
                days, hours, minutes, seconds = map(int, match.groups())
            elif fmt == 'days-hours:minutes':
                days, hours, minutes = map(int, match.groups())
                seconds = 0
            elif fmt == 'days-hours':
                days, hours = map(int, match.groups())
                minutes, seconds = 0, 0
            elif fmt == 'hours:minutes:seconds':
                hours, minutes, seconds = map(int, match.groups())
                days = 0
            elif fmt == 'minutes:seconds':
                minutes, seconds = map(int, match.groups())
                days, hours = 0, 0
            elif fmt == 'minutes':
                minutes = int(match.group())
                days, hours, seconds = 0, 0, 0

            return timedelta(days=days,
                             hours=hours,
                             minutes=minutes,
                             seconds=seconds)

    raise ValueError('Invalid time format')
