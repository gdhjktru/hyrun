from datetime import timedelta
from typing import Optional, Union

import tqdm
from hytools.time import get_timedelta

steps = ['Initialization', 'Transfer', 'Submission', 'Running', 'Finalization']


class ProgressBar:
    """Progress bar for hyrun jobs.

    Parameters
    ----------
    job_time : str
        Job time.
    msg : Optional[str], optional
        Message to display, by default 'Initialization'

    """

    def __init__(self,
                 job_time: Union[str, int, float, timedelta],
                 msg: Optional[str] = 'Job') -> None:
        """Initialize progress bar."""
        job_time = get_timedelta(job_time)
        self.total_job_time: int = job_time.total_seconds()
        self.progress_bar = tqdm.tqdm(
            total=self.total_job_time,
            ncols=120,
            ascii=True,
            bar_format=(f'{{l_bar:30}}{{bar}}|'),
            colour='#2b38ff',
            # bar_format=(f'{{l_bar:30}}{{bar}}| {{elapsed:9}} < {job_time:9}')
            )
        self.progress_bar.n = 0  # type: ignore
        self.progress_bar.n_last_print = 0  # type: ignore
        self.progress_bar.desc = f'{"":<10}{msg:>20}'  # type: ignore # noqa: E501
        self.progress_bar.refresh()  # type: ignore

    def close(self) -> None:
        """Close the progress bar."""
        self.progress_bar.close()

    def update(self, msg: str, percentage=None) -> None:
        """Update the description of a tqdm progress bar.

        Parameters
        ----------
        msg : str
            Message to display.

        """
        if percentage:
                self.progress_bar.n = self.total_job_time * percentage
                self.progress_bar.last_print_n = self.progress_bar.n
        if msg in steps:
            if not percentage:
                self.progress_bar.n = self.total_job_time * steps.index(msg) // len(steps)
                self.progress_bar.last_print_n = self.progress_bar.n
            self.progress_bar.desc = f'{"":<10}{msg[:20]:>20}'
            self.progress_bar.refresh()
        else:
            self.progress_bar.desc = f'{"":<10}{msg[:20]:>20}'
            self.progress_bar.refresh()

    # def _tqdm_update_to_slurm_stat(self,
    #                                job: Optional[SLURMJob] = None,
    #                                finished=False) -> None:
    #     """Update a tqdm progress bar to a specific progress value.

    #     Parameters
    #     ----------
    #     job : SLURMJob, optional
    #         SLURM job info object.
    #     finished : bool, optional
    #         Whether the job has finished, by default `False`.

    #     """
    #     if finished:
    #         self.progress_bar.n = self.progress_bar.total
    #         self.progress_bar.last_print_n = self.progress_bar.total
    #         self.progress_bar.refresh()
    #         return

    #     time_left_delta: timedelta = _parse_slurm_time_string(job.time_left)
    #     elapsed_time_delta: timedelta = _parse_slurm_time_string(
    #         job.elapsed_time)
    #     elapsed: float = float(elapsed_time_delta.total_seconds())
    #     total: float = float(elapsed + time_left_delta.total_seconds())
    #     progress: int = round(self.progress_bar.total * elapsed / total)

    #     self.progress_bar.n = progress
    #     self.progress_bar.last_print_n = progress
    #     if self.progress_bar.desc != f'{job.id:<10}{job.state:>20}':
    #         self.progress_bar.desc = f'{job.id:<10}{job.state:>20}'
    #     self.progress_bar.refresh()
